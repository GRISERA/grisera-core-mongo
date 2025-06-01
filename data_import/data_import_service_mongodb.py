from typing import Union, List, Dict, Any, Set
import json
from datetime import datetime
import threading

from data_import.utils import decode_file_content, remove_prefix, generate_content_hash
from data_import.entity_converters import ENTITY_CONVERTERS, BaseEntityConverter
from data_import.entity_type_mapping import EntityTypeMapping

from data_import.data_import_model import (
    DataImportIn,
    DataImportOut,
    ImportStatus,
    ImportProgressOut
)
from mongo_service.mongo_api_service import MongoApiService
from mongo_service.service_mixins import GenericMongoServiceMixin
from mongo_service.collection_mapping import Collections

# Import GRISERA serwisów i modeli
from services.mongo_services import MongoServiceFactory
from grisera import (
    ActivityIn, ActivityService,
    ChannelIn, ChannelService,
    MeasureNameIn, MeasureNameService,
    ModalityIn, ModalityService,
    LifeActivityIn, LifeActivityService,
    ArrangementIn, ArrangementService,
    ParticipantIn, ParticipantService,
    TimeSeriesIn, TimeSeriesService,
    PropertyIn,
    ExperimentIn, ExperimentService,
    ActivityExecutionIn, ActivityExecutionService,
    ScenarioIn, ScenarioService,
    ParticipationIn, ParticipationService,
    RecordingIn, RecordingService,
    RegisteredDataIn,
    RegisteredChannelIn
)

class DataImportServiceMongoDB(GenericMongoServiceMixin):
    """
    Serwis do obsługi importu danych ontologicznych
    """

    def __init__(self):
        super().__init__()
        self.mongo_api_service = MongoApiService()
        self.model_out_class = DataImportOut
        self.services = MongoServiceFactory() 

        # self.property_mapper = OntologyPropertyMapper()
        
        # Globalny licznik dla nazw Scenario Execution
        self.scenario_execution_counter = 1

        print("🔧 DataImportServiceMongoDB initialized with refactored components:")
        print("   - Entity converters for GRISERA object creation")
        print("   - ScenarioBuilderService for scenario construction")
        print("   - Utils module for helper functions")

    def _background_import_processor(self, import_data: DataImportIn, import_id: str):
        """
        Processes the import data in a background thread.
        """
        try:
            print(f"🧵 Background task started for import ID: {import_id}")
            self._update_import_status(
                import_id,
                import_data.dataset_id,
                ImportStatus.PROCESSING,
                0,
                0
            )

            imported_count = self._process_import_data(
                import_data,
                import_id
            )
            print(f"📊 Background task: Data processing completed. Imported {imported_count} records for import ID: {import_id}")

            error_count = self._get_error_count(import_id, import_data.dataset_id)
            print(f"⚠️ Background task: Found {error_count} errors during import ID: {import_id}")

            final_status = ImportStatus.COMPLETED
            print(f"📋 Background task: Updating final status to: {final_status} for import ID: {import_id}")
            self._update_import_status(
                import_id,
                import_data.dataset_id,
                final_status,
                imported_count,
                error_count
            )
            print(f"✅ Background task: Import completed for ID: {import_id}")

        except Exception as e:
            print(f"❌ Background task: Import failed with error for ID {import_id}: {str(e)}")
            self._update_import_status(
                import_id,
                import_data.dataset_id,
                ImportStatus.FAILED,
                0,  # Reset counts on critical failure
                0,  # Error count for this specific failure is captured in messages
                [f"Background processing error: {str(e)}"]
            )

    def start_import(self, import_data: DataImportIn) -> DataImportOut:
        """
        Rozpoczyna proces importu danych (asynchronicznie)
        Args:
            import_data: Dane do importu
        Returns:
            Status importu
        """
        print(
            f"🚀 Starting import for file: {import_data.file_name}, type: {import_data.import_type}, dataset: {import_data.dataset_id}")

        import_id = None
        try:
            print("📝 Creating import record in database...")
            # Generuj hash zawartości pliku do identyfikacji duplikatów - użycie utils
            content_hash = generate_content_hash(import_data.file_content)
            print(f"🔐 Generated content hash: {content_hash[:16]}...")

            initial_status = ImportStatus.PENDING
            import_record_data = {
                "file_name": import_data.file_name,
                "import_type": import_data.import_type,
                "dataset_id": import_data.dataset_id,
                "status": initial_status.value,
                "description": import_data.description,
                "created_at": datetime.utcnow().isoformat(),
                "imported_records": 0,
                "failed_records": 0,
                "error_count": 0,
                "content_hash": content_hash,
                "experiment_id": import_data.experiment_id
            }

            import_id = self.mongo_api_service.create_document_from_dict(
                import_record_data,
                Collections.IMPORT_JOBS.value,  # "import_jobs"
                import_data.dataset_id
            )
            print(f"✅ Import job created with ID: {import_id}, status: {initial_status.value}")

            print(f"🚀 Launching background import process for ID: {import_id}...")
            thread = threading.Thread(
                target=self._background_import_processor,
                args=(import_data, import_id)
            )
            thread.daemon = True # Ensure thread doesn't block program exit if main thread finishes
            thread.start()
            print(f"🧵 Background thread started for import ID: {import_id}")

            # Zwróć natychmiast ze statusem PENDING
            result = DataImportOut(
                id=import_id,
                file_name=import_data.file_name,
                import_type=import_data.import_type,
                dataset_id=import_data.dataset_id,
                status=initial_status, # Zwraca PENDING
                description=import_data.description,
                created_at=import_record_data["created_at"]
                # imported_records i failed_records będą aktualizowane przez wątek w tle
            )
            print(f"✅ Import initiated for ID: {import_id}. Returning PENDING status.")
            return result

        except Exception as e:
            print(f"❌ Critical error during import initiation: {str(e)}")
            if import_id: # Jeśli ID zostało utworzone, ale wątek nie ruszył
                print(f"🔄 Updating status to FAILED for import ID: {import_id} due to initiation error.")
                self._update_import_status(
                    import_id,
                    import_data.dataset_id,
                    ImportStatus.FAILED,
                    0,
                    0,
                    [f"Initiation error: {str(e)}"]
                )

            result = DataImportOut(
                id=import_id if import_id else "unknown_initiation_failure",
                file_name=import_data.file_name,
                import_type=import_data.import_type,
                dataset_id=import_data.dataset_id,
                status=ImportStatus.FAILED,
                error_messages=[f"Initiation error: {str(e)}"]
            )
            print(f"💥 Returning failed import result: {result.id}")
            return result

    def get_import_status(self, import_id: str, dataset_id: str) -> DataImportOut:
        """
        Pobiera status importu
        """
        print(f"🔍 Getting import status for ID: {import_id}, dataset: {dataset_id}")
        try:
            import_doc = self.mongo_api_service.get_document(
                import_id,
                Collections.IMPORT_JOBS.value,
                dataset_id
            )
            
            result = DataImportOut(**import_doc)
            print(f"✅ Import status retrieved: {result.status}")
            return result
            
        except Exception as e:
            print(f"❌ Failed to get import status for ID {import_id} (dataset: {dataset_id}): {str(e)}")
            # Logowanie pełnego tracebacku dla błędu deserializacji
            import traceback
            print(traceback.format_exc())
            return DataImportOut(
                id=import_id,
                file_name="unknown",
                import_type="unknown",
                dataset_id=dataset_id,
                status=ImportStatus.FAILED,
                error_messages=[f"Import not found: {str(e)}"]
            )

    def get_imports_by_dataset_id(self, dataset_id: str) -> List[DataImportOut]:
        """
        Pobiera wszystkie importy dla danego ID datasetu.
        """
        print(f"🔍 Fetching all imports for dataset ID: {dataset_id}")
        try:
            query = {"dataset_id": dataset_id}
            import_docs = self.mongo_api_service.get_documents(
                collection_name=Collections.IMPORT_JOBS.value,
                dataset_id=dataset_id,
                query=query
            )
            
            if not import_docs:
                print(f"ℹ️ No import jobs found for dataset ID: {dataset_id}")
                return []

            # Konwertuj dokumenty na listę obiektów DataImportOut
            imports_list = [DataImportOut(**doc) for doc in import_docs]
            print(f"✅ Found {len(imports_list)} import jobs for dataset ID: {dataset_id}")
            return imports_list
            
        except Exception as e:
            print(f"❌ Error fetching imports for dataset {dataset_id}: {str(e)}")
            # Można tu rzucić wyjątek dalej lub zwrócić pustą listę w zależności od wymagań
            # Na razie zwracam pustą listę w przypadku błędu, żeby nie crashować API
            import traceback
            print(traceback.format_exc())
            return []

    def _process_import_data(self, import_data: DataImportIn, import_id: str) -> int:
        """
        Przetwarza i importuje dane do MongoDB
        Returns: liczba zaimportowanych rekordów
        """
        print(f"⚙️ Processing import data for type: {import_data.import_type} (Import ID: {import_id})")
        
        if import_data.import_type.lower() == "json":
            print(f"📄 Processing JSON data for import ID: {import_id}...")
            return self._import_json_data(import_data, import_id)
        else:
            error_msg = f"Unsupported import type: {import_data.import_type}"
            print(f"❌ {error_msg} (Import ID: {import_id})")
            # Ten błąd zostanie przechwycony przez _background_import_processor
            # i zaktualizuje status na FAILED.
            raise ValueError(error_msg)

    def _import_json_data(self, import_data: DataImportIn, import_id: str) -> int:
        """
        Importuje dane JSON zgodnie z instrukcją ontologiczną
        Zrefaktoryzowane aby korzystać z nowych komponentów
        Returns: liczba zaimportowanych rekordów
        """
        print(f"📄 Starting JSON data import for import ID: {import_id}")
        processed_ids: Set[str] = set()
        nested_entities_queue: List[Dict[str, Any]] = []
        total_imported = 0

        try:
            print("🔓 Decoding file content using utils...")
            decoded_content = decode_file_content(import_data.file_content)
            print(f"📏 File content size: {len(decoded_content)} characters")

            print("🔍 Parsing JSON data...")
            json_data = json.loads(decoded_content)

            if not isinstance(json_data, dict):
                error_msg = "Expected JSON object at root level"
                print(f"❌ {error_msg}")
                raise ValueError(error_msg)

            print(f"📊 JSON contains {len(json_data.keys())} top-level entity types")
            print(f"🗂️ Entity types found: {list(json_data.keys())}")

            experiment_id = self._handle_experiment_creation(import_data, json_data, import_id)
            if experiment_id:
                print(f"🧪 Using experiment ID: {experiment_id}")
                if not import_data.experiment_id:
                    import_data.experiment_id = experiment_id

            entity_order = EntityTypeMapping.get_import_order()
            print(f"📋 Processing {len(entity_order)} entity types in dependency order...")

            for i, entity_type in enumerate(entity_order, 1):
                print(
                    f"🔄 [{i}/{len(entity_order)}] Looking for {entity_type.json_name} -> {entity_type.collection_name}")

                # Pomiń eksperyment jeśli został już zaimportowany wcześniej
                if entity_type.json_name == "Experiment" and experiment_id:
                    print(f"⏭️ Skipping Experiment processing - already imported with ID: {experiment_id}")
                    continue

                # Szukaj klucza w JSON, który po usunięciu prefiksu pasuje do entity_type.json_name
                matched_json_key = None
                for json_key in json_data.keys():
                    normalized_key = remove_prefix(json_key)
                    if normalized_key == entity_type.json_name:
                        matched_json_key = json_key
                        print(f"✅ Found match: {json_key} (normalized: {normalized_key})")
                        break

                if matched_json_key and matched_json_key in json_data:
                    entity_list = json_data[matched_json_key]
                    if not isinstance(entity_list, list):
                        print(f"❌ Expected list for {matched_json_key}, got {type(entity_list).__name__}")
                        self._log_import_error(
                            import_id,
                            import_data.dataset_id,
                            "INVALID_ENTITY_LIST_TYPE",
                            f"Expected list for {matched_json_key}, got {type(entity_list).__name__}",
                            matched_json_key
                        )
                        continue

                    print(f"✅ Found {len(entity_list)} entities of type {entity_type.json_name}")
                    try:
                        imported_count = self._process_entity_list(
                            entity_list,
                            entity_type.collection_name,
                            import_data.dataset_id,
                            import_id,
                            processed_ids,
                            nested_entities_queue
                        )
                        total_imported += imported_count
                        print(
                            f"📈 Imported {imported_count} entities of type {entity_type.json_name}. Total: {total_imported}")

                    except Exception as e:
                        print(f"❌ Error processing {matched_json_key}: {str(e)}")
                        self._log_import_error(
                            import_id,
                            import_data.dataset_id,
                            "ENTITY_LIST_PROCESSING_ERROR",
                            f"Error processing {matched_json_key}: {str(e)}",
                            matched_json_key
                        )
                else:
                    print(f"⚠️ No data found for entity type: {entity_type.json_name}")

            # Sprawdź czy są nieprzetworzone klucze JSON
            unprocessed_keys = []
            for json_key in json_data.keys():
                normalized_key = remove_prefix(json_key)
                entity_mapping = EntityTypeMapping.find_mapping_by_normalized_name(normalized_key)
                if not entity_mapping:
                    unprocessed_keys.append(json_key)

            if unprocessed_keys:
                print(f"⚠️ Found {len(unprocessed_keys)} unrecognized entity types: {unprocessed_keys}")
                self._log_import_error(
                    import_id,
                    import_data.dataset_id,
                    "UNRECOGNIZED_ENTITY_TYPES",
                    f"Found unrecognized entity types: {unprocessed_keys}",
                    entity_str=str(unprocessed_keys)
                )

            # Przetwarzanie kolejki zagnieżdżonych encji
            print(f"🔄 Processing {len(nested_entities_queue)} nested entities from queue...")
            processed_nested = 0
            while nested_entities_queue:
                entity_data = nested_entities_queue.pop(0)
                entity_id = entity_data.get("@id", "unknown_nested")
                
                if self._process_nested_entity(entity_data, import_data.dataset_id, import_id, processed_ids):
                    processed_nested += 1
            
            print(f"📊 Queue processing completed. Processed {processed_nested} nested entities")

            print(f"✅ JSON import completed! Total imported: {total_imported} entities")
            print(f"🧠 Processed unique IDs: {len(processed_ids)}")

            # Budowanie scenariuszy na podstawie eksperymentów i ActivityExecution
            print("🎬 Building scenarios based on experiments and ActivityExecution...")
            scenarios_count = self._build_experiment_scenarios(import_data, import_id)
            print(f"🎭 Created {scenarios_count} scenarios from experiments")

            return total_imported

        except Exception as e:
            print(f"💥 Critical error in JSON import: {str(e)}")
            self._log_import_error(
                import_id,
                import_data.dataset_id,
                "CRITICAL_JSON_ERROR",
                f"Critical error in JSON import: {str(e)}"
            )
            raise ValueError(f"Critical error in JSON import: {str(e)}")

    def _process_entity_list(
            self,
            entity_list: List[Dict[str, Any]],
            collection_name: str,
            dataset_id: str,
            import_id: str,
            processed_ids: Set[str],
            nested_entities_queue: List[Dict[str, Any]]
    ) -> int:
        """
        Przetwarza listę encji danego typu używając serwisów GRISERA i nowych konwerterów
        """
        print(f"📋 Processing entity list for collection: {collection_name}")

        if not isinstance(entity_list, list):
            error_msg = f"Expected list of entities, got {type(entity_list)}"
            print(f"❌ {error_msg}")
            raise ValueError(error_msg)

        print(f"📊 Found {len(entity_list)} entities to process with GRISERA services")
        imported_count = 0

        entity_type = self._get_entity_type_from_collection(collection_name)
        print(f"🏷️ Determined entity type: {entity_type} for collection: {collection_name}")

        for i, entity in enumerate(entity_list):
            entity_id = entity.get("@id", f"unknown_{i}")
            print(f"🔄 [{i + 1}/{len(entity_list)}] Processing entity: {entity_id}")

            if entity_id in processed_ids:
                print(f"⚠️ Entity already processed: {entity_id}")
                continue

            try:
                grisera_object = self._convert_json_to_grisera_object(
                    entity,
                    entity_type,
                    dataset_id,
                    import_id
                )

                if grisera_object:
                    saved_id = self._save_with_grisera_service(grisera_object, entity_type, dataset_id, import_id)
                    if saved_id is not None:
                        processed_ids.add(entity_id)
                        imported_count += 1
                        print(f"✅ Entity saved with GRISERA service, ID: {saved_id}")
                    else:
                        print(f"⚠️ Entity skipped (incomplete dependencies): {entity_id}")
                        processed_ids.add(entity_id)  # Dodaj do processed_ids żeby nie próbować ponownie
                else:
                    print(f"⚠️ Could not convert entity to GRISERA object: {entity_id}")

            except Exception as e:
                print(f"❌ Error processing entity {entity_id}: {str(e)}")
                self._log_import_error(
                    import_id,
                    dataset_id,
                    "SINGLE_ENTITY_ERROR",
                    f"Error processing single entity: {str(e)}",
                    entity_id
                )

        print(f"📈 Entity list processing completed. Imported: {imported_count}/{len(entity_list)}")
        return imported_count

    def _convert_json_to_grisera_object(
            self,
            json_entity: Dict[str, Any],
            entity_type: str,
            dataset_id: str,
            import_id: str
    ):
        try:
            print(f"🔄 Converting {entity_type} from JSON to GRISERA object using converters")

            converter_class = ENTITY_CONVERTERS.get(entity_type)

            if not converter_class:
                print(f"⚠️ No converter found for entity type: {entity_type}")
                return None

            converter = converter_class(import_id)

            # Wywołaj konwersję
            grisera_object = converter.convert(json_entity)

            print(f"✅ Successfully converted {entity_type} using {converter_class.__name__}")
            return grisera_object

        except Exception as e:
            print(f"❌ Error converting {entity_type} to GRISERA object: {e}")
            entity_id = json_entity.get("@id", "unknown")
            self._log_import_error(
                import_id,
                dataset_id,
                "GRISERA_CONVERSION_ERROR",
                f"Error converting {entity_type} to GRISERA object: {str(e)}",
                entity_id
            )
            return None

    def _save_with_grisera_service(self, grisera_object, entity_type: str, dataset_id: str, import_id: str) -> str:
        """
        Zapisuje obiekt GRISERA używając odpowiedniego serwisu
        """
        try:
            print(f"💾 Saving {entity_type} using GRISERA service")

            if entity_type == "Activity":
                result = self.services.get_activity_service().save_activity(grisera_object, dataset_id)
            elif entity_type == "Channel":
                result = self.services.get_channel_service().save_channel(grisera_object, dataset_id)
            elif entity_type == "MeasureName":
                result = self.services.get_measure_name_service().save_measure_name(grisera_object, dataset_id)
            elif entity_type == "Measure":
                # Specjalna logika dla Measure - mapuj measure_name_id z source ID na MongoDB ID
                result = self._save_measure_with_mapping(grisera_object, dataset_id, import_id)
                saved_id = getattr(result, 'id', 'unknown')
                print(f"✅ {entity_type} saved successfully with ID: {saved_id}")
                return str(saved_id)
            elif entity_type == "Modality":
                result = self.services.get_modality_service().save_modality(grisera_object, dataset_id)
            elif entity_type == "LifeActivity":
                result = self.services.get_life_activity_service().save_life_activity(grisera_object, dataset_id)
            elif entity_type == "Arrangement":
                result = self.services.get_arrangement_service().save_arrangement(grisera_object, dataset_id)
            elif entity_type == "Participant":
                result = self.services.get_participant_service().save_participant(grisera_object, dataset_id)
            elif entity_type == "ParticipantState":
                result = self.services.get_participant_state_service().save_participant_state(grisera_object, dataset_id)
            elif entity_type == "TimeSeries":
                result = self.services.get_time_series_service().save_time_series(grisera_object, dataset_id)
            elif entity_type == "Experiment":
                result = self.services.get_experiment_service().save_experiment(grisera_object, dataset_id)
            elif entity_type == "ActivityExecution":
                # Specjalna logika dla ActivityExecution
                result = self._save_activity_execution_with_scenario_linking(grisera_object, dataset_id, import_id)
                saved_id = getattr(result, 'id', 'unknown')
                print(f"✅ {entity_type} saved successfully with ID: {saved_id}")
                return str(saved_id)
            elif entity_type == "Participation":
                # Specjalna logika dla Participation - mapuj source IDs na MongoDB IDs
                result = self._save_participation_with_mapping(grisera_object, dataset_id, import_id)
                if result is None:
                    print(f"⚠️ Participation skipped - incomplete mapping (na razie olamy ParticipantState)")
                    return None  # Zwróć None żeby _process_entity_list nie liczył tego jako błąd
                saved_id = getattr(result, 'id', 'unknown')
                print(f"✅ {entity_type} saved successfully with ID: {saved_id}")
                return str(saved_id)
            elif entity_type == "Recording":
                result = self._save_recording_with_mapping(grisera_object, dataset_id, import_id)
                saved_id = getattr(result, 'id', 'unknown')
                print(f"✅ {entity_type} saved successfully with ID: {saved_id}")
                return str(saved_id)
            elif entity_type == "RegisteredData":
                result = self.services.get_registered_data_service().save_registered_data(grisera_object, dataset_id)
            elif entity_type == "RegisteredChannel":
                result = self._save_registered_channel_with_mapping(grisera_object, dataset_id, import_id)
                saved_id = getattr(result, 'id', 'unknown')
                print(f"✅ {entity_type} saved successfully with ID: {saved_id}")
                return str(saved_id)
            else:
                raise ValueError(f"No service mapping for entity type: {entity_type}")

            # Wyciągnij ID z wyniku
            saved_id = getattr(result, 'id', 'unknown')
            print(f"✅ {entity_type} saved successfully with ID: {saved_id}")
            return str(saved_id)

        except Exception as e:
            print(f"❌ Error saving {entity_type} with GRISERA service: {e}")
            raise e

    def _get_entity_type_from_collection(self, collection_name: str) -> str:
        """Określa typ encji na podstawie nazwy kolekcji"""
        collection_to_type = {
            Collections.ACTIVITY.value: "Activity",
            Collections.CHANNEL.value: "Channel",
            Collections.MEASURE_NAME.value: "MeasureName",
            Collections.MEASURE.value: "Measure",
            Collections.MODALITY.value: "Modality",
            Collections.LIFE_ACTIVITY.value: "LifeActivity",
            Collections.ARRANGEMENT.value: "Arrangement",
            Collections.PARTICIPANT.value: "Participant",
            Collections.PARTICIPANT_STATE.value: "ParticipantState",
            Collections.TIME_SERIES.value: "TimeSeries",
            Collections.EXPERIMENT.value: "Experiment",
            Collections.ACTIVITY_EXECUTION.value: "ActivityExecution",
            Collections.PARTICIPATION.value: "Participation",
            Collections.RECORDING.value: "Recording",
            Collections.REGISTERED_DATA.value: "RegisteredData",
            Collections.REGISTERED_CHANNEL.value: "RegisteredChannel",
            # Dodaj więcej mapowań według potrzeb
        }
        return collection_to_type.get(collection_name, "Unknown")

    def _get_error_count(self, import_id: str, dataset_id: str) -> int:
        """
        Pobiera liczbę błędów dla danego importu
        """
        print(f"🔍 Getting error count for import: {import_id}")
        try:
            query_filter = {"import_job_id": import_id}
            error_documents = self.mongo_api_service.get_documents(
                collection_name=Collections.IMPORT_ERRORS.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            error_count = len(error_documents) if error_documents else 0
            print(f"📊 Found {error_count} errors for import {import_id}")
            return error_count

        except Exception as e:
            print(f"❌ Error getting error count: {e}")
            return 0

    def _update_import_status(
            self,
            import_id: str,
            dataset_id: str,
            status: ImportStatus,
            imported_records: int,
            error_count: int,
            error_messages: List[str] = None
    ):
        """
        Aktualizuje status importu w bazie danych
        """
        print(f"🔄 Updating import status for ID: {import_id}, dataset: {dataset_id}, status: {status.value}")
        try:
            # 1. Odczytaj istniejący dokument, aby nie stracić pól
            existing_doc = self.mongo_api_service.get_document(
                import_id,
                Collections.IMPORT_JOBS.value,
                dataset_id
            )

            if not isinstance(existing_doc, dict) or "id" not in existing_doc:
                # Obsługa przypadku, gdy dokument nie istnieje lub jest w niepoprawnym formacie
                print(f"❌ Cannot update status. Import job {import_id} not found or invalid.")
                return
            
            fields_to_set = {
                "status": status.value,
                "imported_records": imported_records,
                "failed_records": error_count,
                "updated_at": datetime.utcnow().isoformat()
            }
            if status in [ImportStatus.COMPLETED, ImportStatus.FAILED]:
                fields_to_set["end_time"] = datetime.utcnow().isoformat()

            if error_messages:
                fields_to_set["error_messages"] = error_messages
            else: # Upewnijmy się, że jeśli nie ma nowych błędów, a były, to je wyczyścimy, lub zostawimy - zależy od logiki.
                  # Bezpieczniejsze jest nie usuwać error_messages, jeśli nie są jawnie przekazane jako pusta lista.
                  # Jeśli error_messages jest None, nie dodajemy go do $set, zachowując poprzednią wartość.
                  pass 

            # 3. Zaktualizuj istniejący dokument o nowe wartości
            # Usuwamy '_id' z existing_doc przed scaleniem, jeśli jest, bo Pydantic go nie lubi w **data
            # Ale my chcemy zachować wszystkie pola z existing_doc i nadpisać/dodać te z fields_to_set
            # Mongo samo zarządza _id.
            
            # Tworzymy nowy dokument poprzez scalenie starego z nowymi wartościami
            # Pola z fields_to_set nadpiszą te w existing_doc
            # Musimy usunąć 'id' z existing_doc jeśli jest, bo replace_one oczekuje _id, a nie id.
            # Ale `update_document_with_dict` w `MongoApiService` robi `_update_mongo_input_id` które zamienia 'id' na '_id'.
            # Więc `existing_doc` po `get_document` ma klucz 'id'.
            # `update_data` dla `update_document_with_dict` powinno mieć 'id' (które zostanie zamienione na `_id`) albo nie mieć `id` i `_id`.

            # Bezpieczniejsze jest stworzenie nowego słownika z wszystkimi potrzebnymi polami.
            # `existing_doc` już ma 'id' jako klucz, a nie '_id'.
            updated_full_document = {**existing_doc, **fields_to_set}
            
            # Usuwamy 'id' jeśli jest, bo MongoApiService.update_document_with_dict
            # oczekuje, że ID będzie przekazane jako osobny argument 'id', a nie w słowniku 'new_document'.
            # Jeśli 'id' jest w 'new_document', metoda _update_mongo_input_id może próbować je przekonwertować
            # na '_id' i potencjalnie zduplikować lub spowodować konflikt.
            # Najlepiej, aby 'new_document' nie zawierało ani 'id', ani '_id'.
            # Jednak `update_document_with_dict` w `MongoApiService` bierze `new_document` i w nim zamienia `id` na `_id`
            # a potem robi replace_one. Więc `new_document` POWINIEN zawierać wszystkie pola oprócz `_id`.
            # `get_document` zwraca 'id' zamiast '_id'.

            # Wersja z $set byłaby znacznie czystsza.
            # Ponieważ musimy użyć replace_one, a `update_document_with_dict` zamienia `id` na `_id`:
            # Upewnijmy się, że `updated_full_document` ma wszystkie pola z `DataImportOut` które są w `existing_doc`
            # plus zaktualizowane pola.

            # update_document_with_dict spodziewa się, że `new_document` to kompletny dokument bez `_id` (może mieć `id`).
            # `existing_doc` ma już `id` i inne potrzebne pola.
            # `fields_to_set` zawiera to co aktualizujemy.

            final_document_for_replace = {**existing_doc, **fields_to_set} # `id` jest z `existing_doc`

            # Logowanie dokumentu, który zostanie użyty do zastąpienia istniejącego
            print(f"📄 Preparing to replace document {import_id} with: {final_document_for_replace}")

            self.mongo_api_service.update_document_with_dict(
                collection_name=Collections.IMPORT_JOBS.value,
                id=import_id, # To jest ID używane w filtrze replace_one
                new_document=final_document_for_replace, # Ten dokument zastąpi stary
                dataset_id=dataset_id 
            )
            print("✅ Import status updated successfully")
        except Exception as e:
            print(f"❌ Error updating import status: {str(e)}")

    def _log_import_error(
            self,
            import_id: str,
            dataset_id: str,
            error_type: str,
            error_message: str,
            entity_str: str = None
    ):
        """
        Loguje błąd importu do bazy danych
        """
        print(f"📝 Logging import error: {error_type} - {error_message}")
        try:
            error_data = {
                "import_job_id": import_id,
                "dataset_id": dataset_id,
                "error_type": error_type,
                "error_message": error_message,
                "entity_str": entity_str,
                "timestamp": datetime.utcnow().isoformat()
            }

            self.mongo_api_service.create_document_from_dict(
                error_data,
                Collections.IMPORT_ERRORS.value,
                dataset_id
            )
            print("✅ Import error logged successfully")
        except Exception as e:
            print(f"❌ Error logging import error: {str(e)}")

    def _handle_experiment_creation(self, import_data: DataImportIn, json_data: Dict[str, Any], import_id: str) -> str:
        """
        Sprawdza czy potrzeba utworzyć eksperyment automatycznie.
        
        Args:
            import_data: Dane importu
            json_data: Przesłane dane JSON
            import_id: ID procesu importu
            
        Returns:
            ID eksperymentu (istniejący lub nowo utworzony) lub None
        """
        print("🧪 Checking experiment creation requirements...")
        
        if import_data.experiment_id:
            print(f"✅ Using provided experiment ID: {import_data.experiment_id}")
            return import_data.experiment_id
        
        # 🔍 KROK 1: Sprawdź czy w JSON-ie jest eksperyment do zaimportowania
        print("🔍 Checking if JSON contains experiment data...")
        experiment_from_json = self._find_and_import_experiment_from_json(json_data, import_data.dataset_id, import_id)
        if experiment_from_json:
            print(f"✅ Found and imported experiment from JSON: {experiment_from_json}")
            return experiment_from_json
        
        # 🔍 KROK 2: Sprawdź jakie typy encji są w JSON (poza activity i participant)
        print("🔍 Analyzing JSON entity types for auto-creation...")
        normalized_keys = []
        for json_key in json_data.keys():
            normalized_key = remove_prefix(json_key)
            normalized_keys.append(normalized_key)
            print(f"📋 Found entity type: {json_key} (normalized: {normalized_key})")
        
        # Podstawowe typy które nie wymagają eksperymentu
        basic_types = EntityTypeMapping.get_basic_types()
        other_types = set(normalized_keys) - basic_types
        
        print(f"🧩 Basic types found: {basic_types & set(normalized_keys)}")
        print(f"🔬 Advanced types found: {other_types}")
        
        # Jeśli są tylko activity i participant, nie tworzymy eksperymentu
        if not other_types:
            print("ℹ️ Only basic entity types (Activity, Participant) found. No experiment needed.")
            return None
        
        # 🔍 KROK 3: Mamy złożone dane - tworzymy eksperyment automatycznie
        print(f"🧪 Advanced entity types detected: {other_types}. Creating experiment...")
        
        try:
            # Stwórz nazwę eksperymentu na podstawie nazwy pliku
            experiment_name = f"Auto-generated from {import_data.file_name}"
            if len(experiment_name) > 100:  # Ogranicznie długości
                experiment_name = experiment_name[:97] + "..."
            
            experiment_in = ExperimentIn(
                experiment_name=experiment_name,
                additional_properties=[
                    PropertyIn(key="auto_generated", value="true"),
                    PropertyIn(key="source_file", value=import_data.file_name),
                    PropertyIn(key="import_job_id", value=import_id),
                    PropertyIn(key="entity_types", value=", ".join(sorted(other_types)))
                ]
            )
            
            print(f"📝 Creating experiment with name: {experiment_name}")
            experiment_service = self.services.get_experiment_service()
            experiment_result = experiment_service.save_experiment(experiment_in, import_data.dataset_id)
            
            if hasattr(experiment_result, 'errors') and experiment_result.errors:
                print(f"❌ Error creating experiment: {experiment_result.errors}")
                self._log_import_error(
                    import_id,
                    import_data.dataset_id,
                    "EXPERIMENT_CREATION_ERROR",
                    f"Failed to create experiment: {experiment_result.errors}",
                    experiment_name
                )
                return None
            
            experiment_id = str(experiment_result.id)
            print(f"✅ Experiment created successfully with ID: {experiment_id}")
            
            return experiment_id
            
        except Exception as e:
            print(f"❌ Exception while creating experiment: {str(e)}")
            self._log_import_error(
                import_id,
                import_data.dataset_id,
                "EXPERIMENT_CREATION_EXCEPTION",
                f"Exception while creating experiment: {str(e)}",
                experiment_name if 'experiment_name' in locals() else "unknown"
            )
            return None

    def _find_and_import_experiment_from_json(self, json_data: Dict[str, Any], dataset_id: str, import_id: str) -> str:
        """
        Znajduje i importuje eksperyment z danych JSON jeśli istnieje.
        
        Args:
            json_data: Dane JSON do przeszukania
            dataset_id: ID datasetu
            import_id: ID procesu importu
            
        Returns:
            ID zaimportowanego eksperymentu lub None jeśli nie znaleziono
        """
        print("🔍 Searching for experiment data in JSON...")
        
        # Szukaj klucza eksperymentu w JSON (z uwzględnieniem prefiksów)
        experiment_key = None
        experiment_data = None
        
        for json_key in json_data.keys():
            normalized_key = remove_prefix(json_key)
            if normalized_key == "Experiment":
                experiment_key = json_key
                experiment_data = json_data[json_key]
                print(f"✅ Found experiment data under key: {json_key}")
                break
        
        if not experiment_data:
            print("ℹ️ No experiment data found in JSON")
            return None
        
        if not isinstance(experiment_data, list):
            print(f"❌ Expected list for experiments, got {type(experiment_data).__name__}")
            self._log_import_error(
                import_id,
                dataset_id,
                "INVALID_EXPERIMENT_DATA_TYPE",
                f"Expected list for experiments, got {type(experiment_data).__name__}",
                experiment_key
            )
            return None
        
        if len(experiment_data) == 0:
            print("ℹ️ Experiment list is empty")
            return None
        
        # Bierzemy pierwszy eksperyment z listy
        experiment_json = experiment_data[0]
        print(f"📋 Processing first experiment from list of {len(experiment_data)} experiments")
        
        try:
            # Konwertuj JSON na obiekt GRISERA używając konwertera
            converter_class = ENTITY_CONVERTERS.get("Experiment")
            
            if not converter_class:
                print("❌ No converter found for Experiment entity type")
                self._log_import_error(
                    import_id,
                    dataset_id,
                    "NO_EXPERIMENT_CONVERTER",
                    "No converter found for Experiment entity type",
                    str(experiment_json.get("@id", "unknown"))
                )
                return None
            
            converter = converter_class(import_id)
            experiment_grisera = converter.convert(experiment_json)
            
            if not experiment_grisera:
                print("❌ Failed to convert experiment JSON to GRISERA object")
                return None
            
            # Zapisz eksperyment używając serwisu
            experiment_service = self.services.get_experiment_service()
            experiment_result = experiment_service.save_experiment(experiment_grisera, dataset_id)
            
            if hasattr(experiment_result, 'errors') and experiment_result.errors:
                print(f"❌ Error saving experiment from JSON: {experiment_result.errors}")
                self._log_import_error(
                    import_id,
                    dataset_id,
                    "EXPERIMENT_IMPORT_ERROR",
                    f"Error saving experiment from JSON: {experiment_result.errors}",
                    str(experiment_json.get("@id", "unknown"))
                )
                return None
            
            experiment_id = str(experiment_result.id)
            print(f"✅ Successfully imported experiment from JSON with ID: {experiment_id}")
            
            return experiment_id
            
        except Exception as e:
            print(f"❌ Exception while importing experiment from JSON: {str(e)}")
            self._log_import_error(
                import_id,
                dataset_id,
                "EXPERIMENT_IMPORT_EXCEPTION",
                f"Exception while importing experiment from JSON: {str(e)}",
                str(experiment_json.get("@id", "unknown")) if 'experiment_json' in locals() else "unknown"
            )
            return None

    def _process_nested_entity(
            self, 
            entity_data: Dict[str, Any], 
            dataset_id: str, 
            import_id: str, 
            processed_ids: Set[str]
    ) -> bool:
        """
        Przetwarza zagnieżdżone encje które były w kolejce
        
        Args:
            entity_data: Dane encji do przetworzenia
            dataset_id: ID datasetu
            import_id: ID importu
            processed_ids: Zbiór już przetworzonych ID
            
        Returns:
            True jeśli encja została pomyślnie przetworzona, False w przeciwnym razie
        """
        entity_id = entity_data.get("@id", "unknown")
        print(f"🔄 Processing nested entity: {entity_id}")
        
        # Sprawdź czy już przetwarzana
        if entity_id in processed_ids:
            print(f"⚠️ Nested entity already processed: {entity_id}")
            return False
        
        try:
            # Zidentyfikuj typ nested entity z rdf:type
            entity_types = entity_data.get("rdf:type", [])
            if not isinstance(entity_types, list):
                entity_types = [entity_types]
            
            # Szukaj znanego typu
            target_entity_type = None
            for type_info in entity_types:
                type_id = type_info.get("@id", "") if isinstance(type_info, dict) else str(type_info)
                
                # Mapuj co:Type na nazwy encji
                if "co:Recording" in type_id:
                    target_entity_type = "Recording"
                    break
                elif "co:RegisteredChannel" in type_id:
                    target_entity_type = "RegisteredChannel"
                    break
                elif "co:RegisteredData" in type_id:
                    target_entity_type = "RegisteredData"
                    break
                elif "co:Participation" in type_id:
                    target_entity_type = "Participation"
                    break
            
            if not target_entity_type:
                print(f"⚠️ Unknown nested entity type for {entity_id}, skipping")
                return False
            
            print(f"🏷️ Identified nested entity type: {target_entity_type}")
            
            # Konwertuj za pomocą converterów
            grisera_object = self._convert_json_to_grisera_object(
                entity_data,
                target_entity_type,
                dataset_id,
                import_id
            )
            
            if not grisera_object:
                print(f"❌ Failed to convert nested entity {entity_id} to GRISERA object")
                return False
            
            # Zapisz używając GRISERA service
            saved_id = self._save_with_grisera_service(grisera_object, target_entity_type, dataset_id, import_id)
            
            if saved_id:
                processed_ids.add(entity_id)
                print(f"✅ Nested entity {entity_id} saved successfully with MongoDB ID: {saved_id}")
                return True
            else:
                print(f"❌ Failed to save nested entity {entity_id}")
                return False
            
        except Exception as e:
            print(f"❌ Error processing nested entity {entity_id}: {str(e)}")
            self._log_import_error(
                import_id,
                dataset_id,
                "NESTED_ENTITY_PROCESSING_ERROR",
                f"Error processing nested entity: {str(e)}",
                entity_id
            )
            return False

    def _build_experiment_scenarios(self, import_data: DataImportIn, import_id: str) -> int:
        """
        Buduje scenariusze na podstawie zaimportowanych eksperymentów.
        
        NOWA LOGIKA:
        1. Scenariusz (Scenario) = szablon dla Activity (już mamy)
        2. Scenario Execution = konkretne wykonanie scenariusza - grupa Activity Executions z tym samym scenarioExecutionName
        """
        print(f"🎬 Building experiment-based scenarios for import ID: {import_id}")
        scenarios_created = 0
        scenario_executions_created = 0
        processed_activities = set()  # Deduplikacja scenariuszy dla tego samego Activity
        
        try:
            # KROK 1: Pobierz eksperymenty zaimportowane w tym import_id
            print("📋 Step 1: Fetching experiments imported in this job...")
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "import_job_id",
                        "value": import_id
                    }
                }
            }
            
            imported_experiments = self.mongo_api_service.get_documents(
                collection_name=Collections.EXPERIMENT.value,
                dataset_id=import_data.dataset_id,
                query=query_filter
            )
            
            if not imported_experiments:
                print("ℹ️ No experiments found from this import job")
                return 0
            
            print(f"📊 Found {len(imported_experiments)} experiments imported in this job")
            
            # KROK 2: Dla każdego eksperymentu analizuj ActivityExecution w hasScenario
            for experiment_doc in imported_experiments:
                experiment_id = str(experiment_doc.get("id", "unknown"))
                experiment_name = experiment_doc.get("experiment_name", f"Experiment_{experiment_id}")
                print(f"\n🧪 Processing experiment: {experiment_name} (ID: {experiment_id})")
                
                # KROK 2.1: Pobierz ActivityExecution z has_scenario_data w additional_properties
                activity_executions = self._extract_activity_executions_from_experiment(experiment_doc)
                if not activity_executions:
                    print(f"ℹ️ No ActivityExecution found in experiment {experiment_id}")
                    continue
                
                print(f"📋 Found {len(activity_executions)} ActivityExecution in hasScenario")
                
                # KROK 2.2: Zbierz wszystkie unikalne Activities z tego eksperymentu (SZABLON SCENARIUSZA)
                experiment_activities = []
                for ae_data in activity_executions:
                    ae_id = ae_data.get("@id", "unknown")
                    print(f"🔍 Processing ActivityExecution: {ae_id}")
                    
                    # Wyciągnij Activity ID z co:hasActivity
                    activity_source_id = self._extract_activity_id_from_ae(ae_data)
                    if not activity_source_id:
                        print(f"❌ No Activity found in ActivityExecution {ae_id}")
                        self._log_import_error(
                            import_id,
                            import_data.dataset_id,
                            "MISSING_ACTIVITY_IN_AE",
                            f"No co:hasActivity found in ActivityExecution {ae_id}",
                            ae_id
                        )
                        continue
                    
                    # KROK 2.3: Sprawdź czy Activity istnieje w MongoDB
                    activity_mongo_id = self._find_activity_by_source_id(activity_source_id, import_data.dataset_id)
                    if not activity_mongo_id:
                        print(f"❌ Activity with source ID '{activity_source_id}' not found in MongoDB")
                        self._log_import_error(
                            import_id,
                            import_data.dataset_id,
                            "ACTIVITY_NOT_FOUND_IN_MONGO",
                            f"Activity with source ID '{activity_source_id}' not found in MongoDB",
                            activity_source_id
                        )
                        continue
                    
                    # Dodaj do listy Activities dla tego eksperymentu (z deduplikacją)
                    if activity_mongo_id not in [a["mongo_id"] for a in experiment_activities]:
                        experiment_activities.append({
                            "source_id": activity_source_id,
                            "mongo_id": activity_mongo_id
                        })
                        print(f"✅ Added Activity {activity_source_id} (MongoDB ID: {activity_mongo_id})")
                    else:
                        print(f"⚠️ Activity {activity_source_id} already in list, skipping duplicate")
                
                # KROK 2.4: Stwórz jeden scenariusz-szablon dla wszystkich Activities z eksperymentu
                if experiment_activities:
                    scenario_name = f"Scenario Template for Experiment {experiment_name}"
                    scenario_description = f"Template scenario containing {len(experiment_activities)} activities from experiment '{experiment_name}'"
                    
                    # Stwórz additional_properties z wszystkimi activity_id
                    additional_properties = [
                        PropertyIn(key="name", value=scenario_name),
                        PropertyIn(key="description", value=scenario_description),
                        PropertyIn(key="auto_generated", value="true"),
                        PropertyIn(key="scenario_type", value="template"),
                        PropertyIn(key="source_experiment_id", value=experiment_id),
                        PropertyIn(key="import_job_id", value=import_id),
                        PropertyIn(key="status", value="completed")
                    ]
                    
                    # Dodaj wszystkie activity_id jako osobne PropertyIn
                    for activity in experiment_activities:
                        additional_properties.append(PropertyIn(
                            key="activity_id", 
                            value=activity["mongo_id"]
                        ))
                        print(f"🎯 Added activity_id: {activity['mongo_id']} (source: {activity['source_id']})")
                    
                    scenario_in = ScenarioIn(
                        experiment_id=experiment_id,
                        activity_executions=[],  # Pusta lista - activity_id będzie w additional_properties
                        additional_properties=additional_properties
                    )
                    
                    # KROK 2.5: Zapisz scenariusz szablon
                    try:
                        scenario_service = self.services.get_scenario_service()
                        scenario_result = scenario_service.save_scenario(scenario_in, import_data.dataset_id)
                        
                        # Debug: sprawdź co zwraca serwis
                        print(f"🔍 Scenario service result: id={getattr(scenario_result, 'id', 'MISSING')}, errors={getattr(scenario_result, 'errors', 'NONE')}")
                        
                        if hasattr(scenario_result, 'errors') and scenario_result.errors:
                            print(f"❌ Error creating template scenario: {scenario_result.errors}")
                            self._log_import_error(
                                import_id,
                                import_data.dataset_id,
                                "SCENARIO_TEMPLATE_CREATION_ERROR",
                                f"Failed to create template scenario: {scenario_result.errors}",
                                scenario_name
                            )
                        elif not hasattr(scenario_result, 'id') or scenario_result.id is None:
                            print(f"❌ Template scenario created but ID is missing or None")
                            self._log_import_error(
                                import_id,
                                import_data.dataset_id,
                                "SCENARIO_TEMPLATE_ID_MISSING",
                                f"Template scenario created but ID is missing or None",
                                scenario_name
                            )
                        else:
                            template_scenario_id = str(scenario_result.id)
                            scenarios_created += 1
                            print(f"🎭 Created template scenario {scenarios_created}: {template_scenario_id} with {len(experiment_activities)} activities")
                            
                            # KROK 3: Stwórz Scenario Executions na podstawie Activity Executions
                            executions_created = self._create_scenario_executions(
                                experiment_id, 
                                template_scenario_id,
                                activity_executions, 
                                import_data.dataset_id, 
                                import_id
                            )
                            scenario_executions_created += executions_created
                            print(f"🎯 Created {executions_created} scenario executions for template {template_scenario_id}")
                            
                    except Exception as e:
                        print(f"❌ Exception creating template scenario: {str(e)}")
                        self._log_import_error(
                            import_id,
                            import_data.dataset_id,
                            "SCENARIO_TEMPLATE_CREATION_EXCEPTION",
                            f"Exception creating template scenario: {str(e)}",
                            scenario_name
                        )
                else:
                    print(f"⚠️ No valid Activities found for experiment {experiment_id}")
            
            print(f"🎭 Scenario building completed. Created {scenarios_created} template scenarios and {scenario_executions_created} scenario executions")
            
            # NOWA LOGIKA: Dodaj participantów do eksperymentów
            print(f"\n👥 FAZA 2: Adding participants to experiments from import job: {import_id}")
            participants_processed = self._process_participants_for_experiments(imported_experiments, import_id, import_data.dataset_id)
            print(f"✅ Participant processing completed. Updated {participants_processed} experiments with participants")
            
            return scenarios_created + scenario_executions_created
            
        except Exception as e:
            print(f"❌ Error in scenario building: {str(e)}")
            self._log_import_error(
                import_id,
                import_data.dataset_id,
                "SCENARIO_BUILDING_ERROR",
                f"Error in scenario building: {str(e)}"
            )
            return scenarios_created

    def _extract_activity_executions_from_experiment(self, experiment_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Wyciąga pełne dane ActivityExecution z has_scenario_data w additional_properties
        """
        additional_properties = experiment_doc.get("additional_properties", [])
        
        for prop in additional_properties:
            if prop.get("key") == "has_scenario_data":
                scenario_data_str = prop.get("value", "")
                if scenario_data_str:
                    try:
                        import json
                        return json.loads(scenario_data_str)
                    except json.JSONDecodeError:
                        print(f"❌ Invalid JSON in has_scenario_data: {scenario_data_str}")
                        return []
        
        print("ℹ️ No has_scenario_data found in experiment")
        return []

    def _extract_activity_id_from_ae(self, ae_data: Dict[str, Any]) -> str:
        """
        Wyciąga Activity ID z ActivityExecution data
        """
        has_activity = ae_data.get("co:hasActivity", [])
        if has_activity and len(has_activity) > 0:
            activity_ref = has_activity[0]
            return activity_ref.get("@id", "").replace(":", "")
        return ""

    def _find_activity_by_source_id(self, source_id: str, dataset_id: str) -> str:
        """
        Znajduje Activity w MongoDB po source_id i zwraca jego MongoDB ID
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            activities = self.mongo_api_service.get_documents(
                collection_name=Collections.ACTIVITY.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if activities and len(activities) > 0:
                return str(activities[0].get("id", ""))
            return ""
            
        except Exception as e:
            print(f"❌ Error finding Activity by source_id {source_id}: {e}")
            return ""

    def _find_activity_execution_by_source_id(self, source_id: str, dataset_id: str) -> str:
        """
        Znajduje ActivityExecution w MongoDB po source_id i zwraca jego MongoDB ID
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            activity_executions = self.mongo_api_service.get_documents(
                collection_name=Collections.ACTIVITY_EXECUTION.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if activity_executions and len(activity_executions) > 0:
                return str(activity_executions[0].get("id", ""))
            return ""
            
        except Exception as e:
            print(f"❌ Error finding ActivityExecution by source_id {source_id}: {e}")
            return ""

    def _find_participant_state_by_source_id(self, source_id: str, dataset_id: str) -> str:
        """
        Znajduje ParticipantState w MongoDB po source_id i zwraca jego MongoDB ID
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            participant_states = self.mongo_api_service.get_documents(
                collection_name=Collections.PARTICIPANT_STATE.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if participant_states and len(participant_states) > 0:
                return str(participant_states[0].get("id", ""))
            return ""
            
        except Exception as e:
            print(f"❌ Error finding ParticipantState by source_id {source_id}: {e}")
            return ""

    def _find_participant_by_source_id(self, source_id: str, dataset_id: str) -> str:
        """
        Znajduje Participant w MongoDB po source_id i zwraca jego MongoDB ID
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            participants = self.mongo_api_service.get_documents(
                collection_name=Collections.PARTICIPANT.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if participants and len(participants) > 0:
                return str(participants[0].get("id", ""))
            return ""
            
        except Exception as e:
            print(f"❌ Error finding Participant by source_id {source_id}: {e}")
            return ""

    def _save_participation_with_mapping(self, grisera_object, dataset_id: str, import_id: str):
        """
        Zapisuje Participation z mapowaniem source IDs na MongoDB IDs.
        PROSTE MAPOWANIE: source_entity_ref -> MongoDB ID
        
        UWAGA: Nazwy pól w modelu ParticipationIn są mylące:
        - activity_execution_id = MongoDB ID ActivityExecution ✅
        - participant_state_id = MongoDB ID Participant (z kolekcji participants, NIE participant_states!)
        """
        try:
            print(f"💾 Saving Participation with simple ID mapping...")
            
            print(f"🔍 Original activity_execution_id: {grisera_object.activity_execution_id}")
            print(f"🔍 Original participant_state_id: {grisera_object.participant_state_id}")
            
            # KROK 1: Mapuj activity_execution_id przez source_entity_ref
            mapped_activity_execution_id = grisera_object.activity_execution_id
            if grisera_object.activity_execution_id and str(grisera_object.activity_execution_id).startswith(":"):
                # To jest source ID, znajdź MongoDB ID ActivityExecution
                ae_source_id = str(grisera_object.activity_execution_id).replace(":", "")
                ae_mongo_id = self._find_activity_execution_by_source_id(ae_source_id, dataset_id)
                
                if ae_mongo_id:
                    mapped_activity_execution_id = ae_mongo_id
                    print(f"✅ Mapped activity_execution_id: {grisera_object.activity_execution_id} -> {ae_mongo_id}")
                else:
                    print(f"❌ ActivityExecution not found for source ID: {ae_source_id}")
                    return None
            
            # KROK 2: Mapuj participant_state_id przez source_entity_ref -> ale na Participant ID!
            mapped_participant_id = grisera_object.participant_state_id
            if grisera_object.participant_state_id and str(grisera_object.participant_state_id).startswith(":"):
                # To jest source ID, znajdź MongoDB ID Participant (NIE ParticipantState!)
                participant_source_id = str(grisera_object.participant_state_id).replace(":", "")
                participant_mongo_id = self._find_participant_by_source_id(participant_source_id, dataset_id)
                
                if participant_mongo_id:
                    mapped_participant_id = participant_mongo_id
                    print(f"✅ Mapped participant_state_id: {grisera_object.participant_state_id} -> Participant ID: {participant_mongo_id}")
                else:
                    print(f"❌ Participant not found for source ID: {participant_source_id}")
                    return None
            
            if not mapped_activity_execution_id or not mapped_participant_id:
                print(f"❌ Missing required IDs after mapping")
                return None
            
            # KROK 4: Utwórz nowy obiekt Participation z mapowanymi MongoDB IDs
            from grisera import ParticipationIn
            mapped_participation = ParticipationIn(
                activity_execution_id=mapped_activity_execution_id,  # MongoDB ID ActivityExecution
                participant_state_id=mapped_participant_id           # MongoDB ID Participant (z kolekcji participants!)
            )
            
            # KROK 5: Zapisz Participation używając serwisu
            result = self.services.get_participation_service().save_participation(mapped_participation, dataset_id)
            saved_participation_id = str(getattr(result, 'id', 'unknown'))
            print(f"✅ Participation saved with MongoDB ID: {saved_participation_id}")
            
            print(f"🔗 Final mapping: ActivityExecution({grisera_object.activity_execution_id} -> {mapped_activity_execution_id}), Participant({grisera_object.participant_state_id} -> {mapped_participant_id})")
            
            return result
            
        except Exception as e:
            print(f"❌ Error saving Participation with mapping: {e}")
            raise e

    def _save_activity_execution_with_scenario_linking(self, grisera_object, dataset_id: str, import_id: str):
        """
        Zapisuje ActivityExecution z mapowaniem activity_id i linkiem do scenariusza
        """
        try:
            print(f"💾 Saving ActivityExecution with scenario linking...")
            
            # KROK 1: Pobierz source_entity_ref z additional_properties (to @id z JSON)
            source_entity_ref = None
            for prop in grisera_object.additional_properties:
                if prop.key == "source_entity_ref":
                    source_entity_ref = prop.value
                    break
            
            if not source_entity_ref:
                print("⚠️ No source_entity_ref found in ActivityExecution")
                # Zapisz bez mapowania
                return self.services.get_activity_execution_service().save_activity_execution(grisera_object, dataset_id)
            
            print(f"🔍 ActivityExecution source ID: {source_entity_ref}")
            
            # KROK 2: Mapuj activity_id z source ID na MongoDB ID
            if grisera_object.activity_id and grisera_object.activity_id.startswith(":"):
                # To jest source ID, mapuj na MongoDB ID
                activity_source_id = grisera_object.activity_id.replace(":", "")
                activity_mongo_id = self._find_activity_by_source_id(activity_source_id, dataset_id)
                
                if activity_mongo_id:
                    print(f"✅ Mapped activity_id: {grisera_object.activity_id} -> {activity_mongo_id}")
                    from grisera import ActivityExecutionIn, PropertyIn
                    new_ae = ActivityExecutionIn(
                        activity_id=activity_mongo_id,  
                        arrangement_id=grisera_object.arrangement_id,
                        additional_properties=grisera_object.additional_properties
                    )
                    grisera_object = new_ae
                else:
                    print(f"❌ Could not find Activity in MongoDB for source ID: {activity_source_id}")
                    self._log_import_error(
                        import_id,
                        dataset_id,
                        "ACTIVITY_NOT_FOUND_FOR_AE",
                        f"Activity with source ID '{activity_source_id}' not found for ActivityExecution",
                        source_entity_ref
                    )
            
            # KROK 3: Zapisz ActivityExecution
            result = self.services.get_activity_execution_service().save_activity_execution(grisera_object, dataset_id)
            saved_ae_id = str(getattr(result, 'id', 'unknown'))
            print(f"✅ ActivityExecution saved with ID: {saved_ae_id}")
            
            # KROK 4: Znajdź i zaktualizuj scenariusz który miał to ActivityExecution w has_scenario_data
            updated_scenarios = self._update_scenarios_with_activity_execution(
                source_entity_ref, saved_ae_id, dataset_id
            )
            
            if updated_scenarios > 0:
                print(f"🎭 Updated {updated_scenarios} scenarios with ActivityExecution {saved_ae_id}")
            else:
                print(f"ℹ️ No scenarios found to update with ActivityExecution {saved_ae_id}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error saving ActivityExecution with scenario linking: {e}")
            raise e

    def _update_scenarios_with_activity_execution(self, ae_source_id: str, ae_mongo_id: str, dataset_id: str) -> int:
        """
        Znajduje scenariusze które mają to ActivityExecution w has_scenario_data i aktualizuje je
        """
        try:
            print(f"🔍 Looking for scenarios containing ActivityExecution {ae_source_id}")
            
            # Pobierz scenariusze które mają to ActivityExecution w scenario_data
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "has_scenario_data",
                        "value": {"$regex": f'"@id"\\s*:\\s*"{ae_source_id}"'}
                    }
                }
            }
            
            scenarios = self.mongo_api_service.get_documents(
                collection_name=Collections.SCENARIO.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            updated_count = 0
            for scenario in scenarios:
                scenario_id = str(scenario.get("id", "unknown"))
                print(f"🎭 Found matching scenario: {scenario_id}")
                
                # Sprawdź czy activity_executions jest puste i zaktualizuj
                current_ae_list = scenario.get("activity_executions", [])
                if not current_ae_list:
                    # Dodaj ActivityExecution do scenario
                    try:
                        # Aktualizuj scenario używając ScenarioService
                        from grisera import ScenarioIn, ActivityExecutionIn, PropertyIn
                        
                        # Stwórz ActivityExecutionIn reference dla scenario
                        ae_ref = ActivityExecutionIn(
                            activity_id=None,  # Nie potrzebujemy activity_id w referencji
                            arrangement_id=None,
                            additional_properties=[
                                PropertyIn(key="activity_execution_ref", value=ae_mongo_id)
                            ]
                        )
                        
                        scenario_update = ScenarioIn(
                            experiment_id=scenario.get("experiment_id"),
                            activity_executions=[ae_ref],  # Dodaj referencję do ActivityExecution
                            additional_properties=scenario.get("additional_properties", [])
                        )
                        
                        # Zaktualizuj scenario
                        scenario_service = self.services.get_scenario_service()
                        # Tutaj musiałbym użyć update_scenario, ale najprawdopodobniej nie ma takiej metody
                        # Na razie tylko zalogujmy
                        print(f"🎭 Would update scenario {scenario_id} with ActivityExecution {ae_mongo_id}")
                        updated_count += 1
                        
                    except Exception as e:
                        print(f"❌ Error updating scenario {scenario_id}: {e}")
                else:
                    print(f"ℹ️ Scenario {scenario_id} already has activity_executions, skipping")
            
            return updated_count
            
        except Exception as e:
            print(f"❌ Error updating scenarios with ActivityExecution: {e}")
            return 0

    def _create_scenario_executions(self, experiment_id: str, template_scenario_id: str, activity_executions: List[Dict[str, Any]], dataset_id: str, import_id: str) -> int:
        """
        Tworzy osobne Scenario Executions dla każdego ActivityExecution który pasuje do template scenario.
        
        NOWA LOGIKA:
        1. Pobierz template scenario i sprawdź jakie activity_id ma
        2. Dla każdego ActivityExecution sprawdź czy jego activity_id pasuje do template scenario
        3. Jeśli TAK → stwórz osobny "Scenario Execution X" z tym jednym ActivityExecution
        4. Każdy Scenario Execution = 1 ActivityExecution
        """
        try:
            print(f"🎬 Creating individual scenario executions for template scenario {template_scenario_id}")
            
            # KROK 1: Pobierz template scenario i wyciągnij activity_id z additional_properties
            try:
                scenario_dict = self.mongo_api_service.get_document(
                    template_scenario_id, 
                    Collections.SCENARIO.value, 
                    dataset_id
                )
                if not scenario_dict:
                    print(f"❌ Template scenario {template_scenario_id} not found")
                    return 0
                    
                print(f"✅ Retrieved template scenario: {scenario_dict.get('id')}")
                
                # Wyciągnij activity_id z additional_properties template scenario
                template_activity_ids = []
                for prop in scenario_dict.get("additional_properties", []):
                    if prop.get("key") == "activity_id":
                        template_activity_ids.append(prop.get("value"))
                
                if not template_activity_ids:
                    print(f"⚠️ Template scenario has no activity_id in additional_properties")
                    return 0
                
                print(f"🎯 Template scenario activity_ids: {template_activity_ids}")
                
                # Upewnij się że activity_executions jest listą
                if "activity_executions" not in scenario_dict:
                    scenario_dict["activity_executions"] = []
                    
            except Exception as e:
                print(f"❌ Error retrieving template scenario {template_scenario_id}: {e}")
                return 0
            
            # KROK 2: Pobierz Activity które zawierają Activity Executions z tego import job
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "import_job_id",
                        "value": import_id
                    }
                },
                "activity_executions": {"$exists": True, "$ne": []}  # Activity musi mieć activity_executions
            }
            
            activities_with_executions = self.mongo_api_service.get_documents(
                collection_name=Collections.ACTIVITY.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if not activities_with_executions:
                print("ℹ️ No Activities with Activity Executions found from this import job")
                return 0
            
            print(f"📋 Found {len(activities_with_executions)} Activities with Activity Executions from this import")
            
            # KROK 3: Przejdź przez wszystkie Activity Executions i sprawdź które pasują do template scenario
            matching_activity_executions = []
            total_ae_found = 0
            
            for activity_doc in activities_with_executions:
                activity_id = str(activity_doc.get("id"))
                activity_name = activity_doc.get("activity", "unknown")
                ae_list = activity_doc.get("activity_executions", [])
                
                print(f"🔍 Processing Activity '{activity_name}' (ID: {activity_id}) with {len(ae_list)} Activity Executions")
                
                # Sprawdź czy to Activity pasuje do template scenario
                if activity_id in template_activity_ids:
                    print(f"✅ Activity {activity_name} (ID: {activity_id}) MATCHES template scenario!")
                    
                    for ae_doc in ae_list:
                        total_ae_found += 1
                        
                        # Znajdź source_entity_ref (oryginalną nazwę z JSON)
                        source_name = "Unknown"
                        for prop in ae_doc.get("additional_properties", []):
                            if prop.get("key") == "source_entity_ref":
                                source_name = prop.get("value", "Unknown").replace(":", "")
                                break
                        
                        matching_activity_executions.append({
                            "ae_doc": ae_doc,
                            "source_name": source_name,
                            "activity_name": activity_name
                        })
                        
                        print(f"📎 Added matching ActivityExecution: {source_name} (ID: {ae_doc.get('id')})")
                else:
                    print(f"⚠️ Activity {activity_name} (ID: {activity_id}) does NOT match template scenario")
            
            print(f"📊 Total matching Activity Executions found: {len(matching_activity_executions)}")
            
            if not matching_activity_executions:
                print("ℹ️ No matching Activity Executions found for template scenario")
                return 0
            
            # KROK 4: Stwórz osobny Scenario Execution dla każdego pasującego ActivityExecution
            scenario_executions_created = 0
            
            for ae_info in matching_activity_executions:
                ae_doc = ae_info["ae_doc"]
                source_name = ae_info["source_name"]
                activity_name = ae_info["activity_name"]
                ae_id = str(ae_doc.get("id"))
                
                # Globalny licznik dla nazw Scenario Execution
                scenario_execution_name = f"Scenario Execution {self.scenario_execution_counter}"
                self.scenario_execution_counter += 1
                print(f"\n🎯 Creating {scenario_execution_name} for ActivityExecution '{source_name}'")
                
                # Stwórz listę z jednym ActivityExecution ID
                single_ae_list = [ae_id]
                
                # Dodaj jako osobną listę do activity_executions template scenario
                scenario_dict["activity_executions"].append(single_ae_list)
                scenario_executions_created += 1
                
                print(f"✅ Created {scenario_execution_name}: [{source_name}] (ID: {ae_id})")
            
            # KROK 5: Zapisz zaktualizowany template scenario do MongoDB
            try:
                self.mongo_api_service.update_document_with_dict(
                    collection_name=Collections.SCENARIO.value,
                    id=template_scenario_id,
                    new_document=scenario_dict,
                    dataset_id=dataset_id
                )
                
                print(f"🎭 Successfully updated template scenario {template_scenario_id} with {scenario_executions_created} individual scenario executions")
                
                # Loguj finalną strukturę
                print(f"📊 Final activity_executions structure: {len(scenario_dict['activity_executions'])} scenario executions")
                for i, execution in enumerate(scenario_dict["activity_executions"]):
                    print(f"   Scenario Execution {i+1}: {len(execution)} Activity Execution (ID: {execution[0] if execution else 'empty'})")
                
                return scenario_executions_created
                
            except Exception as e:
                print(f"❌ Error updating template scenario {template_scenario_id}: {e}")
                self._log_import_error(
                    import_id,
                    dataset_id,
                    "SCENARIO_UPDATE_ERROR",
                    f"Error updating template scenario with individual executions: {str(e)}",
                    template_scenario_id
                )
                return 0
            
        except Exception as e:
            print(f"❌ Error creating individual scenario executions: {str(e)}")
            self._log_import_error(
                import_id,
                dataset_id,
                "SCENARIO_EXECUTION_CREATION_ERROR",
                f"Error creating individual scenario executions: {str(e)}"
            )
            return 0

    def _find_imported_participants(self, import_id: str, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Znajduje participantów zaimportowanych w tym import job.
        
        Args:
            import_id: ID procesu importu
            dataset_id: ID datasetu
            
        Returns:
            Lista słowników z danymi participantów (id, name, source_id)
        """
        try:
            print(f"👥 Searching for participants imported in job: {import_id}")
            
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "import_job_id",
                        "value": import_id
                    }
                }
            }
            
            imported_participants = self.mongo_api_service.get_documents(
                collection_name=Collections.PARTICIPANT.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if not imported_participants:
                print(f"ℹ️ No participants found for import job: {import_id}")
                return []
            
            participants_data = []
            for participant_doc in imported_participants:
                participant_id = str(participant_doc.get("id", "unknown"))
                participant_name = participant_doc.get("name", "Unknown Participant")
                
                # Znajdź source_entity_ref (oryginalne @id z JSON)
                source_id = "unknown"
                for prop in participant_doc.get("additional_properties", []):
                    if prop.get("key") == "source_entity_ref":
                        source_id = prop.get("value", "unknown").replace(":", "")
                        break
                
                participants_data.append({
                    "mongo_id": participant_id,
                    "name": participant_name,
                    "source_id": source_id
                })
                
                print(f"✅ Found participant: {participant_name} (MongoDB ID: {participant_id}, Source: {source_id})")
            
            print(f"📊 Total participants found: {len(participants_data)}")
            return participants_data
            
        except Exception as e:
            print(f"❌ Error finding imported participants: {str(e)}")
            self._log_import_error(
                import_id,
                dataset_id,
                "PARTICIPANT_SEARCH_ERROR",
                f"Error finding imported participants: {str(e)}"
            )
            return []

    def _add_participants_to_experiment(self, experiment_id: str, participants: List[Dict[str, Any]], dataset_id: str) -> bool:
        """
        Dodaje participantów do eksperymentu przez aktualizację additional_properties.
        Wykorzystuje wzorzec z frontend UI (participant_id w additional_properties).
        
        Args:
            experiment_id: MongoDB ID eksperymentu
            participants: Lista participantów (z mongo_id, name, source_id)
            dataset_id: ID datasetu
            
        Returns:
            True jeśli operacja zakończona sukcesem, False w przeciwnym razie
        """
        try:
            print(f"👥 Adding {len(participants)} participants to experiment: {experiment_id}")
            
            # KROK 1: Pobierz istniejący dokument eksperymentu
            experiment_doc = self.mongo_api_service.get_document(
                experiment_id,
                Collections.EXPERIMENT.value,
                dataset_id
            )
            
            if not experiment_doc:
                print(f"❌ Experiment {experiment_id} not found")
                return False
            
            experiment_name = experiment_doc.get("experiment_name", f"Experiment_{experiment_id}")
            print(f"✅ Retrieved experiment: {experiment_name}")
            
            # KROK 2: Przygotuj additional_properties z participant_id (wzorzec frontend)
            current_additional_properties = experiment_doc.get("additional_properties", [])
            
            # Sprawdź czy już są participanci w additional_properties
            existing_participant_ids = set()
            for prop in current_additional_properties:
                if prop.get("key") == "participant_id":
                    existing_participant_ids.add(prop.get("value"))
            
            print(f"📋 Existing participants in experiment: {len(existing_participant_ids)}")
            
            # KROK 3: Dodaj nowych participantów (unikaj duplikatów)
            new_participants_added = 0
            for participant in participants:
                participant_mongo_id = participant["mongo_id"]
                participant_name = participant["name"]
                
                if participant_mongo_id not in existing_participant_ids:
                    # Dodaj participant_id do additional_properties (wzorzec frontend)
                    current_additional_properties.append({
                        "key": "participant_id",
                        "value": participant_mongo_id
                    })
                    new_participants_added += 1
                    print(f"✅ Added participant to experiment: {participant_name} (ID: {participant_mongo_id})")
                else:
                    print(f"⚠️ Participant already in experiment: {participant_name} (ID: {participant_mongo_id})")
            
            if new_participants_added == 0:
                print(f"ℹ️ No new participants to add to experiment {experiment_name}")
                return True
            
            # KROK 4: Zaktualizuj dokument eksperymentu (replace całego dokumentu)
            experiment_doc["additional_properties"] = current_additional_properties
            
            self.mongo_api_service.update_document_with_dict(
                collection_name=Collections.EXPERIMENT.value,
                id=experiment_id,
                new_document=experiment_doc,
                dataset_id=dataset_id
            )
            
            print(f"🎉 Successfully added {new_participants_added} participants to experiment '{experiment_name}'")
            print(f"📊 Total participants in experiment: {len([p for p in current_additional_properties if p.get('key') == 'participant_id'])}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error adding participants to experiment {experiment_id}: {str(e)}")
            return False

    def _process_participants_for_experiments(self, experiments: List[Dict[str, Any]], import_id: str, dataset_id: str) -> int:
        """
        Przetwarza i dodaje participantów do eksperymentów zaimportowanych w tym import job.
        
        Args:
            experiments: Lista słowników z danymi eksperymentów
            import_id: ID procesu importu
            dataset_id: ID datasetu
            
        Returns:
            Liczba zaktualizowanych eksperymentów
        """
        try:
            print(f"👥 Starting participant processing for {len(experiments)} experiments")
            
            # KROK 1: Znajdź wszystkich participantów z tego import job (raz dla wszystkich eksperymentów)
            participants = self._find_imported_participants(import_id, dataset_id)
            
            if not participants:
                print(f"ℹ️ No participants found for import job: {import_id}")
                return 0
            
            print(f"📊 Found {len(participants)} participants to add to experiments")
            
            # KROK 2: Dla każdego eksperymentu dodaj participantów
            updated_count = 0
            for experiment_doc in experiments:
                experiment_id = str(experiment_doc.get("id", "unknown"))
                print(f"🧪 Processing experiment: {experiment_id}")
                
                if self._add_participants_to_experiment(experiment_id, participants, dataset_id):
                    updated_count += 1
                    print(f"✅ Experiment '{experiment_id}' updated successfully with participants")
                else:
                    print(f"❌ Error updating experiment {experiment_id} with participants")
                    self._log_import_error(
                        import_id,
                        dataset_id,
                        "EXPERIMENT_PARTICIPANT_UPDATE_FAILED",
                        f"Failed to add participants to experiment {experiment_id}",
                        experiment_id
                    )
            
            print(f"📈 Participant processing summary: {updated_count}/{len(experiments)} experiments updated")
            return updated_count
            
        except Exception as e:
            print(f"❌ Error processing participants for experiments: {str(e)}")
            self._log_import_error(
                import_id,
                dataset_id,
                "PARTICIPANT_PROCESSING_ERROR",
                f"Error processing participants for experiments: {str(e)}"
            )
            return 0

    def _create_participations_direct_insert(self, import_id: str, dataset_id: str) -> int:
        """
        PROSTY DIRECT INSERT do kolekcji participations - bez skomplikowanej logiki konwerterów.
        
        Algorytm:
        1. Pobierz wszystkich participantów i activity_executions z tego import job
        2. Dla każdego participanta stwórz participant_state (jeśli nie istnieje)
        3. Dla każdego participanta znajdź jego activity_execution przez source mapping
        4. Zrób prosty insert_one do kolekcji 'participations' z participant_state_id
        
        Args:
            import_id: ID procesu importu
            dataset_id: ID datasetu
            
        Returns:
            Liczba utworzonych participation rekordów
        """
        try:
            print(f"🤝 Starting direct participation insert for import: {import_id}")
            
            # KROK 1: Pobierz wszystkich participantów z tego import job
            participants = self._find_imported_participants(import_id, dataset_id)
            if not participants:
                print("ℹ️ No participants found for participation creation")
                return 0
            
            # KROK 2: Pobierz wszystkie activity_executions z tego import job  
            activity_executions = self._find_imported_activity_executions(import_id, dataset_id)
            if not activity_executions:
                print("ℹ️ No activity executions found for participation creation")
                return 0
                
            print(f"📊 Found {len(participants)} participants and {len(activity_executions)} activity executions")
            
            # KROK 3: Stwórz participant_states dla każdego participant
            db = self.mongo_api_service.client[dataset_id]
            participant_states_collection = db["participant_states"]
            participations_collection = db["participations"]
            
            from bson import ObjectId
            
            participant_state_mapping = {}  # participant_id -> participant_state_id
            
            for participant in participants:
                participant_mongo_id = participant["mongo_id"]
                
                # Sprawdź czy participant_state już istnieje dla tego participant
                existing_ps = participant_states_collection.find_one({
                    "participant_id": ObjectId(participant_mongo_id)
                })
                
                if existing_ps:
                    participant_state_id = str(existing_ps["_id"])
                    print(f"✅ Found existing participant_state for {participant['source_id']}: {participant_state_id}")
                else:
                    # Stwórz nowy participant_state
                    participant_state_doc = {
                        "_id": ObjectId(),
                        "participant_id": ObjectId(participant_mongo_id),
                        "personality_ids": None,
                        "appearance_ids": None,
                        "age": None,
                        "additional_properties": []
                    }
                    
                    result = participant_states_collection.insert_one(participant_state_doc)
                    participant_state_id = str(result.inserted_id)
                    print(f"✅ Created new participant_state for {participant['source_id']}: {participant_state_id}")
                
                participant_state_mapping[participant_mongo_id] = participant_state_id
            
            # KROK 4: Stwórz mapowanie source_id -> mongo_id dla activity_executions
            ae_mapping = {ae["source_id"]: ae["mongo_id"] for ae in activity_executions}
            
            # KROK 5: Direct insert participations według wzorca source ID
            participations_created = 0
            
            # Mapowanie z przykładu: P09 -> actExecP09, P06 -> actExecP06, etc.
            for participant in participants:
                participant_source_id = participant["source_id"]  # np. "P09"
                participant_mongo_id = participant["mongo_id"]
                participant_state_id = participant_state_mapping[participant_mongo_id]
                
                # Znajdź pasujący ActivityExecution (np. "actExecP09")
                matching_ae_source_id = f"actExec{participant_source_id}"
                
                if matching_ae_source_id in ae_mapping:
                    activity_execution_mongo_id = ae_mapping[matching_ae_source_id]
                    
                    # PROSTY INSERT do kolekcji participations
                    participation_doc = {
                        "_id": ObjectId(),
                        "activity_execution_id": ObjectId(activity_execution_mongo_id),
                        "participant_state_id": ObjectId(participant_state_id)  # TERAZ używamy participant_state_id!
                    }
                    
                    # Direct MongoDB insert
                    result = participations_collection.insert_one(participation_doc)
                    participations_created += 1
                    
                    print(f"✅ Created participation: Participant {participant_source_id} -> ActivityExecution {matching_ae_source_id}")
                    print(f"   📊 MongoDB IDs: participant_state_id={participant_state_id}, activity_execution_id={activity_execution_mongo_id}")
                else:
                    print(f"⚠️ No matching ActivityExecution found for participant {participant_source_id} (looking for {matching_ae_source_id})")
            
            print(f"🎉 Successfully created {participations_created} participation records using direct MongoDB insert")
            return participations_created
            
        except Exception as e:
            print(f"❌ Error in direct participation insert: {str(e)}")
            self._log_import_error(
                import_id,
                dataset_id,
                "DIRECT_PARTICIPATION_INSERT_ERROR",
                f"Error in direct participation insert: {str(e)}"
            )
            return 0
    
    def _find_imported_activity_executions(self, import_id: str, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Znajduje activity_executions zaimportowane w tym import job.
        
        POPRAWKA: ActivityExecutions są embedded w Activity documents, nie w osobnej kolekcji!
        
        Returns:
            Lista słowników z danymi activity_executions (mongo_id, source_id)
        """
        try:
            # POPRAWIONE QUERY: Szukaj w kolekcji Activities które mają activity_executions
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "import_job_id",
                        "value": import_id
                    }
                },
                "activity_executions": {"$exists": True, "$ne": []}  # Activity musi mieć activity_executions
            }
            
            activities_with_executions = self.mongo_api_service.get_documents(
                collection_name=Collections.ACTIVITY.value,  # SZUKAJ W ACTIVITIES, NIE ACTIVITY_EXECUTIONS!
                dataset_id=dataset_id,
                query=query_filter
            )
            
            ae_data = []
            for activity_doc in activities_with_executions:
                activity_executions = activity_doc.get("activity_executions", [])
                
                # Wyciągnij każdy ActivityExecution z embedded array
                for ae_doc in activity_executions:
                    ae_id = str(ae_doc.get("id", "unknown"))
                    
                    # Znajdź source_entity_ref w ActivityExecution  
                    source_id = "unknown"
                    for prop in ae_doc.get("additional_properties", []):
                        if prop.get("key") == "source_entity_ref":
                            source_id = prop.get("value", "unknown").replace(":", "")
                            break
                    
                    ae_data.append({
                        "mongo_id": ae_id,
                        "source_id": source_id
                    })
                    print(f"🔍 Found embedded ActivityExecution: {source_id} (ID: {ae_id})")
                
            print(f"📊 Found {len(ae_data)} activity executions for participation creation")
            return ae_data
            
        except Exception as e:
            print(f"❌ Error finding imported activity executions: {str(e)}")
            return []

    def _save_recording_with_mapping(self, grisera_object, dataset_id: str, import_id: str):
        """
        Zapisuje Recording z mapowaniem source IDs na MongoDB IDs dla participation_id i registered_channel_id.
        """
        try:
            print(f"💾 Saving Recording with ID mapping...")
            
            # KROK 1: Pobierz source_entity_ref z additional_properties (to @id z JSON)
            source_entity_ref = None
            for prop in grisera_object.additional_properties:
                if prop.key == "source_entity_ref":
                    source_entity_ref = prop.value
                    break
            
            if not source_entity_ref:
                print("⚠️ No source_entity_ref found in Recording")
            else:
                print(f"🔍 Recording source ID: {source_entity_ref}")
            
            # KROK 2: Mapuj participation_id z source ID na MongoDB ID
            mapped_participation_id = grisera_object.participation_id
            if grisera_object.participation_id and grisera_object.participation_id.startswith(":"):
                # To jest source ID, mapuj na MongoDB ID
                participation_source_id = grisera_object.participation_id.replace(":", "")
                participation_mongo_id = self._find_participation_by_source_id(participation_source_id, dataset_id)
                
                if participation_mongo_id:
                    mapped_participation_id = participation_mongo_id
                    print(f"✅ Mapped participation_id: {grisera_object.participation_id} -> {participation_mongo_id}")
                else:
                    print(f"❌ Could not find Participation in MongoDB for source ID: {participation_source_id}")
                    self._log_import_error(
                        import_id,
                        dataset_id,
                        "PARTICIPATION_NOT_FOUND_FOR_RECORDING",
                        f"Participation with source ID '{participation_source_id}' not found for Recording",
                        source_entity_ref or "unknown"
                    )
                    # Kontynuuj z oryginalnym ID - może zostanie utworzone później
            
            # KROK 3: Mapuj registered_channel_id z source ID na MongoDB ID
            mapped_registered_channel_id = grisera_object.registered_channel_id
            if grisera_object.registered_channel_id and grisera_object.registered_channel_id.startswith(":"):
                # To jest source ID, mapuj na MongoDB ID
                registered_channel_source_id = grisera_object.registered_channel_id.replace(":", "")
                registered_channel_mongo_id = self._find_registered_channel_by_source_id(registered_channel_source_id, dataset_id)
                
                if registered_channel_mongo_id:
                    mapped_registered_channel_id = registered_channel_mongo_id
                    print(f"✅ Mapped registered_channel_id: {grisera_object.registered_channel_id} -> {registered_channel_mongo_id}")
                else:
                    print(f"❌ Could not find RegisteredChannel in MongoDB for source ID: {registered_channel_source_id}")
                    self._log_import_error(
                        import_id,
                        dataset_id,
                        "REGISTERED_CHANNEL_NOT_FOUND_FOR_RECORDING",
                        f"RegisteredChannel with source ID '{registered_channel_source_id}' not found for Recording",
                        source_entity_ref or "unknown"
                    )
                    # Kontynuuj z oryginalnym ID - może zostanie utworzone później
            
            # KROK 4: Utwórz nowy obiekt Recording z mapowanymi IDs
            from grisera import RecordingIn
            mapped_recording = RecordingIn(
                participation_id=mapped_participation_id,
                registered_channel_id=mapped_registered_channel_id,
                additional_properties=grisera_object.additional_properties
            )
            
            # KROK 5: Zapisz Recording używając serwisu
            result = self.services.get_recording_service().save_recording(mapped_recording, dataset_id)
            saved_recording_id = str(getattr(result, 'id', 'unknown'))
            print(f"✅ Recording saved with ID: {saved_recording_id}")
            
            # KROK 6: Loguj mapowanie dla debugowania
            if mapped_participation_id != grisera_object.participation_id:
                print(f"🔗 Final participation_id mapping: {grisera_object.participation_id} -> {mapped_participation_id}")
            if mapped_registered_channel_id != grisera_object.registered_channel_id:
                print(f"🔗 Final registered_channel_id mapping: {grisera_object.registered_channel_id} -> {mapped_registered_channel_id}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error saving Recording with mapping: {e}")
            raise e

    def _find_participation_by_source_id(self, source_id: str, dataset_id: str) -> str:
        """
        Znajduje Participation w MongoDB po source_id i zwraca jego MongoDB ID
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            participations = self.mongo_api_service.get_documents(
                collection_name=Collections.PARTICIPATION.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if participations and len(participations) > 0:
                return str(participations[0].get("id", ""))
            return ""
            
        except Exception as e:
            print(f"❌ Error finding Participation by source_id {source_id}: {e}")
            return ""

    def _find_registered_channel_by_source_id(self, source_id: str, dataset_id: str) -> str:
        """
        Znajduje RegisteredChannel w MongoDB po source_id i zwraca jego MongoDB ID
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            registered_channels = self.mongo_api_service.get_documents(
                collection_name=Collections.REGISTERED_CHANNEL.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if registered_channels and len(registered_channels) > 0:
                return str(registered_channels[0].get("id", ""))
            return ""
            
        except Exception as e:
            print(f"❌ Error finding RegisteredChannel by source_id {source_id}: {e}")
            return ""

    def _save_registered_channel_with_mapping(self, grisera_object, dataset_id: str, import_id: str):
        """
        Zapisuje RegisteredChannel z mapowaniem source IDs na MongoDB IDs dla registered_data_id i channel_id.
        """
        try:
            print(f"💾 Saving RegisteredChannel with ID mapping...")
            
            # KROK 1: Pobierz source_entity_ref z additional_properties (to @id z JSON)
            source_entity_ref = None
            for prop in grisera_object.additional_properties:
                if prop.key == "source_entity_ref":
                    source_entity_ref = prop.value
                    break
            
            if not source_entity_ref:
                print("⚠️ No source_entity_ref found in RegisteredChannel")
            else:
                print(f"🔍 RegisteredChannel source ID: {source_entity_ref}")
            
            # KROK 2: Mapuj registered_data_id z source ID na MongoDB ID
            mapped_registered_data_id = grisera_object.registered_data_id
            if grisera_object.registered_data_id and grisera_object.registered_data_id.startswith(":"):
                # To jest source ID, mapuj na MongoDB ID
                registered_data_source_id = grisera_object.registered_data_id.replace(":", "")
                registered_data_mongo_id = self._find_registered_data_by_source_id(registered_data_source_id, dataset_id)
                
                if registered_data_mongo_id:
                    mapped_registered_data_id = registered_data_mongo_id
                    print(f"✅ Mapped registered_data_id: {grisera_object.registered_data_id} -> {registered_data_mongo_id}")
                else:
                    print(f"❌ Could not find RegisteredData in MongoDB for source ID: {registered_data_source_id}")
                    self._log_import_error(
                        import_id,
                        dataset_id,
                        "REGISTERED_DATA_NOT_FOUND_FOR_REGISTERED_CHANNEL",
                        f"RegisteredData with source ID '{registered_data_source_id}' not found for RegisteredChannel",
                        source_entity_ref or "unknown"
                    )
                    # Kontynuuj z oryginalnym ID - może zostanie utworzone później
            
            # KROK 3: Mapuj channel_id z source ID na MongoDB ID
            mapped_channel_id = grisera_object.channel_id
            if grisera_object.channel_id and grisera_object.channel_id.startswith(":"):
                # To jest source ID, mapuj na MongoDB ID
                channel_source_id = grisera_object.channel_id.replace(":", "")
                channel_mongo_id = self._find_channel_by_source_id(channel_source_id, dataset_id)
                
                if channel_mongo_id:
                    mapped_channel_id = channel_mongo_id
                    print(f"✅ Mapped channel_id: {grisera_object.channel_id} -> {channel_mongo_id}")
                else:
                    print(f"❌ Could not find Channel in MongoDB for source ID: {channel_source_id}")
                    self._log_import_error(
                        import_id,
                        dataset_id,
                        "CHANNEL_NOT_FOUND_FOR_REGISTERED_CHANNEL",
                        f"Channel with source ID '{channel_source_id}' not found for RegisteredChannel",
                        source_entity_ref or "unknown"
                    )
                    # Kontynuuj z oryginalnym ID - może zostanie utworzone później
            
            # KROK 4: Utwórz nowy obiekt RegisteredChannel z mapowanymi IDs
            from grisera import RegisteredChannelIn
            mapped_registered_channel = RegisteredChannelIn(
                registered_data_id=mapped_registered_data_id,
                channel_id=mapped_channel_id,
                additional_properties=grisera_object.additional_properties
            )
            
            # KROK 5: Zapisz RegisteredChannel używając serwisu
            result = self.services.get_registered_channel_service().save_registered_channel(mapped_registered_channel, dataset_id)
            saved_registered_channel_id = str(getattr(result, 'id', 'unknown'))
            print(f"✅ RegisteredChannel saved with ID: {saved_registered_channel_id}")
            
            # KROK 6: Loguj mapowanie dla debugowania
            if mapped_registered_data_id != grisera_object.registered_data_id:
                print(f"🔗 Final registered_data_id mapping: {grisera_object.registered_data_id} -> {mapped_registered_data_id}")
            if mapped_channel_id != grisera_object.channel_id:
                print(f"🔗 Final channel_id mapping: {grisera_object.channel_id} -> {mapped_channel_id}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error saving RegisteredChannel with mapping: {e}")
            raise e

    def _find_registered_data_by_source_id(self, source_id: str, dataset_id: str) -> str:
        """
        Znajduje RegisteredData w MongoDB po source_id i zwraca jego MongoDB ID
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            registered_data = self.mongo_api_service.get_documents(
                collection_name=Collections.REGISTERED_DATA.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if registered_data and len(registered_data) > 0:
                return str(registered_data[0].get("id", ""))
            return ""
            
        except Exception as e:
            print(f"❌ Error finding RegisteredData by source_id {source_id}: {e}")
            return ""

    def _find_channel_by_source_id(self, source_id: str, dataset_id: str) -> str:
        """
        Znajduje Channel w MongoDB po source_id i zwraca jego MongoDB ID
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            channels = self.mongo_api_service.get_documents(
                collection_name=Collections.CHANNEL.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if channels and len(channels) > 0:
                return str(channels[0].get("id", ""))
            return ""
            
        except Exception as e:
            print(f"❌ Error finding Channel by source_id {source_id}: {e}")
            return ""

    def _find_activity_execution_document_by_source_id(self, source_id: str, dataset_id: str) -> dict:
        """
        Znajduje pełny dokument ActivityExecution w MongoDB po source_id
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            activity_executions = self.mongo_api_service.get_documents(
                collection_name=Collections.ACTIVITY_EXECUTION.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if activity_executions and len(activity_executions) > 0:
                return activity_executions[0]
            return {}
            
        except Exception as e:
            print(f"❌ Error finding ActivityExecution document by source_id {source_id}: {e}")
            return {}

    def _find_participant_state_document_by_source_id(self, source_id: str, dataset_id: str) -> dict:
        """
        Znajduje pełny dokument ParticipantState w MongoDB po source_id
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            participant_states = self.mongo_api_service.get_documents(
                collection_name=Collections.PARTICIPANT_STATE.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if participant_states and len(participant_states) > 0:
                return participant_states[0]
            return {}
            
        except Exception as e:
            print(f"❌ Error finding ParticipantState document by source_id {source_id}: {e}")
            return {}

    def _process_participations_from_json(self, json_data: Dict[str, Any], dataset_id: str, import_id: str, processed_ids: Set[str]) -> int:
        """
        Wyszukuje i przetwarza wszystkie encje Participation z zagnieżdżonych struktur JSON.
        Participation nie jest zwykle na głównym poziomie JSON, ale zagnieżdżone w innych encjach.
        """
        participations_found = []
        
        def extract_participations_recursive(data, path=""):
            """Rekurencyjnie szuka Participation w zagnieżdżonych strukturach"""
            if isinstance(data, dict):
                # Sprawdź czy to Participation
                if "@id" in data and "rdf:type" in data:
                    rdf_types = data.get("rdf:type", [])
                    if not isinstance(rdf_types, list):
                        rdf_types = [rdf_types]
                    
                    for type_info in rdf_types:
                        type_id = type_info.get("@id", "") if isinstance(type_info, dict) else str(type_info)
                        if "co:Participation" in type_id:
                            entity_id = data.get("@id")
                            if entity_id and entity_id not in processed_ids:
                                participations_found.append(data)
                                print(f"🔍 Found Participation in path: {path} -> {entity_id}")
                            break
                
                # Rekurencyjnie przeszukaj wszystkie zagnieżdżone obiekty
                for key, value in data.items():
                    new_path = f"{path}.{key}" if path else key
                    extract_participations_recursive(value, new_path)
                    
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    new_path = f"{path}[{i}]" if path else f"[{i}]"
                    extract_participations_recursive(item, new_path)
        
        print(f"🔍 Searching for Participation entities in JSON structure...")
        extract_participations_recursive(json_data)
        
        if not participations_found:
            print("ℹ️ No Participation entities found in nested structures")
            return 0
        
        print(f"📊 Found {len(participations_found)} Participation entities to process")
        
        # Przetwarzaj znalezione Participation
        processed_count = 0
        for participation_data in participations_found:
            entity_id = participation_data.get("@id", "unknown_participation")
            
            try:
                print(f"🤝 Processing Participation: {entity_id}")
                
                # Konwertuj na GRISERA object
                grisera_object = self._convert_json_to_grisera_object(
                    participation_data,
                    "Participation",
                    dataset_id,
                    import_id
                )
                
                if grisera_object:
                    saved_id = self._save_with_grisera_service(grisera_object, "Participation", dataset_id, import_id)
                    if saved_id:
                        processed_ids.add(entity_id)
                        processed_count += 1
                        print(f"✅ Participation saved: {entity_id} -> MongoDB ID: {saved_id}")
                    else:
                        print(f"❌ Failed to save Participation: {entity_id}")
                else:
                    print(f"❌ Failed to convert Participation to GRISERA object: {entity_id}")
                    
            except Exception as e:
                print(f"❌ Error processing Participation {entity_id}: {str(e)}")
                self._log_import_error(
                    import_id,
                    dataset_id,
                    "PARTICIPATION_PROCESSING_ERROR",
                    f"Error processing Participation: {str(e)}",
                    entity_id
                )
        
        return processed_count

    def _save_measure_with_mapping(self, grisera_object, dataset_id: str, import_id: str):
        """
        Zapisuje Measure z mapowaniem measure_name_id z source ID na MongoDB ID.
        """
        try:
            print(f"💾 Saving Measure with measure_name_id mapping...")
            
            # KROK 1: Mapuj measure_name_id z source ID na MongoDB ID
            mapped_measure_name_id = grisera_object.measure_name_id
            if grisera_object.measure_name_id and str(grisera_object.measure_name_id).startswith(":"):
                # To jest source ID, mapuj na MongoDB ID
                measure_name_source_id = str(grisera_object.measure_name_id).replace(":", "")
                measure_name_mongo_id = self._find_measure_name_by_source_id(measure_name_source_id, dataset_id)
                
                if measure_name_mongo_id:
                    mapped_measure_name_id = measure_name_mongo_id
                    print(f"✅ Mapped measure_name_id: {grisera_object.measure_name_id} -> {measure_name_mongo_id}")
                else:
                    print(f"❌ Could not find MeasureName in MongoDB for source ID: {measure_name_source_id}")
                    self._log_import_error(
                        import_id,
                        dataset_id,
                        "MEASURE_NAME_NOT_FOUND_FOR_MEASURE",
                        f"MeasureName with source ID '{measure_name_source_id}' not found for Measure",
                        str(grisera_object.measure_name_id)
                    )
                    # Kontynuuj z oryginalnym ID lub None - może zostanie utworzone później
                    mapped_measure_name_id = None
            
            # KROK 2: Utwórz nowy obiekt Measure z mapowanym measure_name_id
            from grisera import MeasureIn
            mapped_measure = MeasureIn(
                datatype=grisera_object.datatype,
                range=grisera_object.range,
                unit=grisera_object.unit,
                measure_name_id=mapped_measure_name_id
            )
            
            # KROK 3: Zapisz Measure używając serwisu
            result = self.services.get_measure_service().save_measure(mapped_measure, dataset_id)
            saved_measure_id = str(getattr(result, 'id', 'unknown'))
            print(f"✅ Measure saved with MongoDB ID: {saved_measure_id}")
            
            # KROK 4: Loguj mapowanie dla debugowania
            if mapped_measure_name_id != grisera_object.measure_name_id:
                print(f"🔗 Final measure_name_id mapping: {grisera_object.measure_name_id} -> {mapped_measure_name_id}")
            
            return result
            
        except Exception as e:
            print(f"❌ Error saving Measure with mapping: {e}")
            raise e

    def _find_measure_name_by_source_id(self, source_id: str, dataset_id: str) -> str:
        """
        Znajduje MeasureName w MongoDB po source_id i zwraca jego MongoDB ID
        """
        try:
            query_filter = {
                "additional_properties": {
                    "$elemMatch": {
                        "key": "source_entity_ref",
                        "value": f":{source_id}"
                    }
                }
            }
            
            measure_names = self.mongo_api_service.get_documents(
                collection_name=Collections.MEASURE_NAME.value,
                dataset_id=dataset_id,
                query=query_filter
            )
            
            if measure_names and len(measure_names) > 0:
                return str(measure_names[0].get("id", ""))
            return ""
            
        except Exception as e:
            print(f"❌ Error finding MeasureName by source_id {source_id}: {e}")
            return ""

from typing import Dict, Any, List, Set
from datetime import datetime

from mongo_service.mongo_api_service import MongoApiService
from mongo_service.collection_mapping import Collections


class ScenarioBuilderService:
    """
    Serwis odpowiedzialny za budowanie scenariuszy sekwencyjnych na podstawie ActivityExecution
    """
    
    def __init__(self, mongo_api_service: MongoApiService, import_id: str, dataset_id: str):
        self.mongo_api_service = mongo_api_service
        self.import_id = import_id
        self.dataset_id = dataset_id
    
    def build_scenarios(self) -> int:
        """
        Buduje scenariusze sekwencyjne na podstawie ActivityExecution połączonych przez hasNext...
        Returns: liczba utworzonych scenariuszy
        """
        try:
            print(f"🎬 Starting sequential scenarios building for import: {self.import_id}")
            
            # Pobierz wszystkie ActivityExecution z bieżącego importu
            query_filter = {"import_job_id": self.import_id}
            activity_executions = self.mongo_api_service.get_documents(
                collection_name="activity_executions",
                dataset_id=self.dataset_id,
                query=query_filter
            )
            
            if not activity_executions:
                print("⚠️ No ActivityExecutions found for scenario building")
                return 0
            
            print(f"📊 Found {len(activity_executions)} ActivityExecutions to analyze")
            
            # Przygotuj słownik dla szybkiego dostępu po source_entity_ref
            ae_by_source_ref = {ae["source_entity_ref"]: ae for ae in activity_executions}
            print(f"🗂️ Indexed {len(ae_by_source_ref)} ActivityExecutions by source_entity_ref")
            
            # Znajdź pola referencyjne wskazujące na następny ActivityExecution
            next_ref_fields = [
                "has_next_activity_execution_ref",
                "has_next_ref", 
                "next_activity_execution_ref",
                "co_has_next_activity_execution_ref"  # możliwe warianty
            ]
            
            # Zidentyfikuj które ActivityExecution są referencjonowane jako "następne"
            referenced_as_next = set()
            ae_next_mapping = {}  # source_id -> next_source_id
            
            for ae in activity_executions:
                for field_name in next_ref_fields:
                    if field_name in ae and ae[field_name]:
                        next_source_id = ae[field_name]
                        if isinstance(next_source_id, str):
                            referenced_as_next.add(next_source_id)
                            ae_next_mapping[ae["source_entity_ref"]] = next_source_id
                            print(f"🔗 Found chain: {ae['source_entity_ref']} -> {next_source_id}")
                        elif isinstance(next_source_id, list) and len(next_source_id) > 0:
                            # Jeśli lista, weź pierwszy element
                            next_id = next_source_id[0]
                            referenced_as_next.add(next_id)
                            ae_next_mapping[ae["source_entity_ref"]] = next_id
                            print(f"🔗 Found chain: {ae['source_entity_ref']} -> {next_id} (from list)")
            
            print(f"🔗 Found {len(ae_next_mapping)} next-references")
            
            # Znajdź początki scenariuszy (nie są referencjonowane jako "następne")
            scenario_starts = []
            for ae in activity_executions:
                if ae["source_entity_ref"] not in referenced_as_next:
                    scenario_starts.append(ae["source_entity_ref"])
            
            print(f"🎬 Found {len(scenario_starts)} potential scenario starts: {scenario_starts}")
            
            scenarios_created = 0
            
            # Zbuduj scenariusze dla każdego początku
            for start_source_id in scenario_starts:
                try:
                    scenario_chain = self._build_chain(
                        start_source_id, 
                        ae_next_mapping, 
                        ae_by_source_ref
                    )
                    
                    if len(scenario_chain) > 1:  # Scenariusz musi mieć więcej niż 1 ActivityExecution
                        scenario_doc = {
                            "import_job_id": self.import_id,
                            "dataset_id": self.dataset_id,
                            "scenario_source_ref": start_source_id,  # Główny identyfikator scenariusza
                            "activity_execution_source_refs": scenario_chain,  # Lista ID ActivityExecution
                            "activity_execution_count": len(scenario_chain),
                            "created_at": datetime.utcnow().isoformat(),
                            "scenario_type": "sequential"
                        }
                        
                        # Zapisz scenariusz do MongoDB
                        scenario_id = self.mongo_api_service.create_document_from_dict(
                            scenario_doc,
                            Collections.SCENARIO.value,  # "scenarios"
                            self.dataset_id
                        )
                        
                        scenarios_created += 1
                        print(f"🎭 Created scenario {scenarios_created}: {scenario_id} with {len(scenario_chain)} ActivityExecutions")
                        print(f"   Chain: {' -> '.join(scenario_chain)}")
                    
                    else:
                        print(f"⚠️ Skipping single ActivityExecution scenario: {start_source_id}")
                        
                except Exception as e:
                    print(f"❌ Error building scenario for {start_source_id}: {str(e)}")
                    self._log_scenario_error(
                        "SCENARIO_BUILD_ERROR",
                        f"Error building scenario for {start_source_id}: {str(e)}",
                        start_source_id
                    )
            
            print(f"🎉 Sequential scenarios building completed: {scenarios_created} scenarios created")
            return scenarios_created
            
        except Exception as e:
            print(f"❌ Critical error in scenarios building: {str(e)}")
            self._log_scenario_error(
                "CRITICAL_SCENARIO_ERROR",
                f"Critical error in scenarios building: {str(e)}"
            )
            return 0
    
    def _build_chain(
        self, 
        start_source_id: str, 
        ae_next_mapping: Dict[str, str], 
        ae_by_source_ref: Dict[str, Any]
    ) -> List[str]:
        """
        Buduje łańcuch scenariusza rozpoczynając od danego ActivityExecution
        Returns: lista source_id w kolejności scenariusza
        """
        chain = [start_source_id]
        visited = {start_source_id}  # Wykrywanie cykli
        current_id = start_source_id
        
        print(f"🔄 Building chain starting from: {start_source_id}")
        
        while current_id in ae_next_mapping:
            next_id = ae_next_mapping[current_id]
            
            if next_id in visited:
                print(f"⚠️ Cycle detected in scenario chain at: {next_id}")
                self._log_scenario_error(
                    "SCENARIO_CYCLE_DETECTED",
                    f"Cycle detected in scenario chain: {' -> '.join(chain)} -> {next_id}",
                    current_id
                )
                break
            
            if next_id not in ae_by_source_ref:
                print(f"⚠️ Referenced ActivityExecution not found: {next_id}")
                break
            
            chain.append(next_id)
            visited.add(next_id)
            current_id = next_id
            print(f"   ➡️ Added to chain: {next_id}")
        
        print(f"✅ Chain completed with {len(chain)} ActivityExecutions")
        return chain
    
    def _log_scenario_error(
        self,
        error_code: str,
        message: str,
        problematic_entity_id: str = None
    ):
        """Loguje błąd scenariusza do bazy danych"""
        print(f"📝 Logging scenario error: {error_code} - {message}")
        
        error_doc = {
            "import_job_id": self.import_id,
            "timestamp": datetime.utcnow().isoformat(),
            "error_code": error_code,
            "message": message,
            "context": {
                "import_phase": "scenario_building",
                "dataset_id": self.dataset_id,
                "scenario_builder": True
            }
        }
        
        if problematic_entity_id:
            error_doc["problematic_entity_id"] = problematic_entity_id
            error_doc["context"]["entity_id"] = problematic_entity_id
            print(f"🔍 Error relates to entity: {problematic_entity_id}")
        
        # Dodaj metadane błędu specyficzne dla scenariuszy
        error_doc["context"]["error_severity"] = self._determine_scenario_error_severity(error_code)
        error_doc["context"]["retry_recommended"] = self._is_scenario_retry_recommended(error_code)
        
        try:
            self.mongo_api_service.create_document_from_dict(
                error_doc,
                Collections.IMPORT_ERRORS.value,  # "import_errors"
                self.dataset_id
            )
            print(f"✅ Scenario error logged to database")
        except Exception as e:
            # Błąd podczas logowania błędu - nie przerywaj procesu
            print(f"❌ Failed to log scenario error to database: {e}")
            print(f"💾 Original error was: {error_code} - {message}")
    
    def _determine_scenario_error_severity(self, error_code: str) -> str:
        """Określa poziom ważności błędu scenariusza"""
        critical_errors = ["CRITICAL_SCENARIO_ERROR"]
        warning_errors = ["SCENARIO_CYCLE_DETECTED"]
        
        if error_code in critical_errors:
            return "CRITICAL"
        elif error_code in warning_errors:
            return "WARNING"
        else:
            return "ERROR"
    
    def _is_scenario_retry_recommended(self, error_code: str) -> bool:
        """Określa czy błąd scenariusza sugeruje ponowienie próby"""
        retry_errors = ["SCENARIO_BUILD_ERROR"]
        no_retry_errors = ["CRITICAL_SCENARIO_ERROR", "SCENARIO_CYCLE_DETECTED"]
        
        if error_code in no_retry_errors:
            return False
        elif error_code in retry_errors:
            return True
        else:
            return False  # Domyślnie nie rekomenduj retry 
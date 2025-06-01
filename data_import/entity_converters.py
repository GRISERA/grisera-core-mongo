import json # Dla serializacji w _add_remaining_properties, jeśli będzie potrzebna bardziej zaawansowana
from abc import ABC, abstractmethod
from typing import Dict, Any, List, TypeVar, Generic, Optional, Set
from datetime import datetime

from grisera import (
    ActivityIn, ChannelIn, MeasureNameIn, ModalityIn, LifeActivityIn,
    ArrangementIn, ParticipantIn, TimeSeriesIn, PropertyIn,
    ExperimentIn, ActivityExecutionIn, ParticipationIn, ParticipantStateIn,
    RecordingIn, RegisteredDataIn, RegisteredChannelIn, MeasureIn
)
from data_import.utils import remove_prefix # Założenie: ta funkcja istnieje i działa poprawnie

# Type variable dla generycznego typu GRISERA In
GriseraInType = TypeVar('GriseraInType')


class BaseEntityConverter(ABC, Generic[GriseraInType]):
    """
    Abstrakcyjna klasa bazowa dla konwerterów encji JSON na obiekty GRISERA.
    Zakłada, że `json_entity` może mieć klucze z prefiksami, a `remove_prefix` je usuwa.
    Klucze przekazywane w `*_key_candidates` powinny być "czystymi" kluczami (bez prefiksów).
    """

    def __init__(self, import_id: str):
        self.import_id = import_id

    def _get_optional_field_value(
        self,
        json_entity: Dict[str, Any],
        key_candidates: List[str]  # Lista "czystych" kluczy
    ) -> Optional[str]:
        """
        Wyszukuje pierwszą pasującą wartość w json_entity na podstawie listy potencjalnych "czystych" kluczy.
        Porównuje klucze po usunięciu prefiksów z kluczy encji JSON.
        Zwraca wartość jako string lub None, jeśli nie znaleziono lub wartość jest "pusta".
        IGNORUJE złożone typy (listy, słowniki) - są one przeznaczone dla properties, nie głównych pól.
        """
        for clean_candidate_key in key_candidates:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # Upewnij się, że wartość nie jest pusta (None lub pusty string)
                    # NOWE: Ignoruj złożone typy (listy, słowniki) - nie powinny być używane jako główne pola
                    if entity_value is not None and entity_value != "" and isinstance(entity_value, (str, int, float, bool)):  
                        return str(entity_value)
        return None

    def _get_main_field_value(
        self,
        json_entity: Dict[str, Any],
        json_key_candidates: List[str],  # Lista "czystych" kluczy
        default_prefix_for_fallback: str,  # Np. "Activity", używane gdy @id też nie ma
        entity_id_str_for_fallback: Optional[str] = None  # Oryginalne @id jako string (może mieć prefix)
    ) -> str:
        """
        Wyciąga główną wartość pola encji. Jeśli nie znajdzie wśród kandydatów,
        używa wartości @id encji jako fallback. Jeśli @id nie ma, tworzy placeholder.
        Zawsze zwraca string.
        """
        value = self._get_optional_field_value(json_entity, json_key_candidates)
        if value is not None:
            return value

        # Nie znaleziono wartości wśród kandydatów, użyj fallbacka.
        final_fallback_value: str
        # Domyślna wartość dla logowania, jeśli nie ma @id
        clean_entity_id_for_print = f"{default_prefix_for_fallback.lower()}_unknown_id" 

        effective_clean_entity_id: Optional[str] = None
        # Spróbuj uzyskać czyste @id z przekazanego entity_id_str_for_fallback
        if entity_id_str_for_fallback:
            cleaned_id = remove_prefix(entity_id_str_for_fallback)
            if cleaned_id and cleaned_id != "": # Upewnij się, że po usunięciu prefixu coś zostało
                effective_clean_entity_id = cleaned_id
        
        # Jeśli nie z przekazanego, spróbuj z json_entity["@id"]
        if not effective_clean_entity_id and "@id" in json_entity:
            raw_id_from_entity = json_entity["@id"]
            if raw_id_from_entity is not None and raw_id_from_entity != "":
                cleaned_id = remove_prefix(str(raw_id_from_entity))
                if cleaned_id and cleaned_id != "":
                     effective_clean_entity_id = cleaned_id
        
        if effective_clean_entity_id:
            final_fallback_value = effective_clean_entity_id
            clean_entity_id_for_print = effective_clean_entity_id # Zaktualizuj dla logu
            print(f"⚠️ No main field found from candidates {json_key_candidates} for entity '{clean_entity_id_for_print}', using entity ID as fallback: {final_fallback_value}")
        else:
            final_fallback_value = f"{default_prefix_for_fallback}_id_placeholder" # Zmieniony placeholder
            print(f"⚠️ No main field found from candidates {json_key_candidates} AND no valid @id found for '{clean_entity_id_for_print}'. Using placeholder: {final_fallback_value}")
        
        return final_fallback_value

    def _create_common_properties(self, json_entity: Dict[str, Any]) -> List[PropertyIn]:
        """
        Tworzy listę standardowych metadanych importu jako PropertyIn.
        `source_entity_ref` przechowuje oryginalne @id (z potencjalnym prefiksem).
        """
        source_entity_id = "unknown_source_id"
        if "@id" in json_entity and json_entity["@id"] is not None and json_entity["@id"] != "":
            source_entity_id = str(json_entity["@id"])
        
        properties = [
            PropertyIn(key="source_entity_ref", value=source_entity_id),
            PropertyIn(key="import_job_id", value=str(self.import_id)),
            PropertyIn(key="import_timestamp", value=datetime.utcnow().isoformat()),
        ]
        return properties

    def _add_remaining_properties(
        self,
        json_entity: Dict[str, Any],
        properties_list: List[PropertyIn],
        processed_clean_keys: Optional[List[str]] = None # Lista "czystych" kluczy, które już zostały użyte
    ):
        """
        Dodaje wszystkie pozostałe właściwości z JSON jako PropertyIn.
        Klucze specjalne (@id, @type, rdf:type) są domyślnie wykluczane.
        Dodatkowo wyklucza klucze z `processed_clean_keys`.
        Obsługuje wartości string, numeryczne, boolean oraz listy prostych typów.
        """
        default_excluded_clean_keys: Set[str] = {
            remove_prefix("@id"),
            remove_prefix("@type"),
            remove_prefix("rdf:type")
        }
        
        # Upewnij się, że processed_clean_keys są czyste i unikalne
        user_excluded_clean_keys = set(remove_prefix(k) for k in (processed_clean_keys or []))
        final_excluded_clean_keys = default_excluded_clean_keys.union(user_excluded_clean_keys)

        for entity_key_with_prefix, value in json_entity.items():
            clean_key = remove_prefix(entity_key_with_prefix)
            if clean_key not in final_excluded_clean_keys:
                if isinstance(value, (str, int, float, bool)):
                    properties_list.append(PropertyIn(key=clean_key, value=str(value)))
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, (str, int, float, bool)):
                            properties_list.append(PropertyIn(key=clean_key, value=str(item)))
                        # Można dodać obsługę serializacji bardziej złożonych elementów listy
                        # np. elif isinstance(item, dict): properties_list.append(PropertyIn(key=clean_key, value=json.dumps(item)))
                # Można dodać obsługę serializacji zagnieżdżonych słowników
                # np. elif isinstance(value, dict): properties_list.append(PropertyIn(key=clean_key, value=json.dumps(value)))
    
    @abstractmethod
    def convert(self, json_entity: Dict[str, Any]) -> GriseraInType:
        pass


class ActivityConverter(BaseEntityConverter[ActivityIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["hasActivityType", "activityType", "hasType", "type"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Activity"
    
    def convert(self, json_entity: Dict[str, Any]) -> ActivityIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        activity_type = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=entity_id_str
        )
        
        additional_properties = self._create_common_properties(json_entity)
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        print(f"📝 Creating ActivityIn: activity='{activity_type}', properties={len(additional_properties)} (including common)")
        return ActivityIn(
            activity=activity_type,
            additional_properties=additional_properties
        )


class ChannelConverter(BaseEntityConverter[ChannelIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["channelName", "hasName", "name"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Channel"
    
    def convert(self, json_entity: Dict[str, Any]) -> ChannelIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        channel_name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=entity_id_str
        )
        
        additional_properties = self._create_common_properties(json_entity)
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        print(f"📝 Creating ChannelIn: channel_name='{channel_name}', properties={len(additional_properties)} (including common)")
        return ChannelIn(
            channel_name=channel_name,
            additional_properties=additional_properties
        )


class MeasureNameConverter(BaseEntityConverter[MeasureNameIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["measureName", "hasName", "name"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Measure"
    
    def convert(self, json_entity: Dict[str, Any]) -> MeasureNameIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None
        measure_name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=entity_id_str
        )
        
        additional_properties = self._create_common_properties(json_entity)
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        print(f"📝 Creating MeasureNameIn: measure_name='{measure_name}', properties={len(additional_properties)} (including common)")
        return MeasureNameIn(
            measure_name=measure_name,
            additional_properties=additional_properties
        )


class MeasureConverter(BaseEntityConverter[MeasureIn]):
    JSON_KEY_CANDIDATES_FOR_DATATYPE = ["measureDatatype", "hasDatatype", "datatype"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_RANGE = ["measureRange", "hasRange", "range"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_UNIT = ["measureUnit", "hasUnit", "unit"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_MEASURE_NAME_ID = ["hasMeasureName", "measure_name_id", "measureNameId"]  # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Measure"
    
    def convert(self, json_entity: Dict[str, Any]) -> MeasureIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        # Wyciągnij datatype - pole wymagane
        datatype = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_DATATYPE,
            "float",  # domyślna wartość
            entity_id_str_for_fallback=entity_id_str
        )
        
        # Wyciągnij range - pole wymagane
        range_value = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_RANGE,
            "unknown",  # domyślna wartość
            entity_id_str_for_fallback=entity_id_str
        )
        
        # Wyciągnij unit - pole wymagane, ale może nie być w JSON
        unit = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_UNIT)
        if not unit:
            unit = "unknown"  # domyślna wartość
        
        # Wyciągnij measure_name_id z zagnieżdżonej struktury co:hasMeasureName
        measure_name_id = self._extract_measure_name_id_from_json(json_entity)
        
        additional_properties = self._create_common_properties(json_entity)
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_DATATYPE)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_RANGE)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_UNIT)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MEASURE_NAME_ID)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(entity_id_str) if entity_id_str else "Unknown"
        print(f"📝 Creating MeasureIn: name='{clean_name_for_log}', datatype='{datatype}', range='{range_value}', unit='{unit}', measure_name_id='{measure_name_id}', properties={len(additional_properties)} (including common)")
        
        return MeasureIn(
            datatype=datatype,
            range=range_value,
            unit=unit,
            measure_name_id=measure_name_id
        )
    
    def _extract_measure_name_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga MeasureName ID z JSON z zagnieżdżonej struktury co:hasMeasureName.
        Zwraca source ID który będzie mapowany na MongoDB ID później.
        """
        # Najpierw sprawdź proste przypadki
        simple_mn_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_MEASURE_NAME_ID)
        if simple_mn_id:
            print(f"✅ Found simple measure_name_id: {simple_mn_id}")
            return simple_mn_id
        
        # Następnie sprawdź co:hasMeasureName (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_MEASURE_NAME_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # To jest hasMeasureName - wyciągnij @id MeasureName
                    measure_name_id = self._extract_nested_entity_id(entity_value)
                    if measure_name_id:
                        print(f"✅ Found MeasureName source ID from {entity_key_with_prefix}: {measure_name_id}")
                        # Zapisujemy source ID - mapowanie na MongoDB ID zostanie zrobione później
                        return measure_name_id
        
        print("⚠️ No MeasureName reference found in Measure")
        return None
    
    def _extract_nested_entity_id(self, entity_value: Any) -> Optional[str]:
        """
        Wyciąga @id z zagnieżdżonych obiektów JSON (lista obiektów lub pojedynczy obiekt).
        """
        if isinstance(entity_value, list) and len(entity_value) > 0:
            # Lista obiektów - bierz pierwszy
            first_entity = entity_value[0]
            if isinstance(first_entity, dict) and "@id" in first_entity:
                return str(first_entity["@id"])
        elif isinstance(entity_value, dict) and "@id" in entity_value:
            # Pojedynczy obiekt
            return str(entity_value["@id"])
        elif isinstance(entity_value, str):
            # Proste ID jako string
            return entity_value
        
        return None


class ModalityConverter(BaseEntityConverter[ModalityIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["modalityName", "hasName", "name"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Modality"
    
    def convert(self, json_entity: Dict[str, Any]) -> ModalityIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None
        modality_name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=entity_id_str
        )
        
        additional_properties = self._create_common_properties(json_entity)
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        print(f"📝 Creating ModalityIn: modality_name='{modality_name}', properties={len(additional_properties)} (including common)")
        return ModalityIn(modality_name=modality_name, additional_properties=additional_properties)


class LifeActivityConverter(BaseEntityConverter[LifeActivityIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["lifeActivityName", "hasName", "name"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "LifeActivity"
    
    def convert(self, json_entity: Dict[str, Any]) -> LifeActivityIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None
        life_activity_name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=entity_id_str
        )
        
        additional_properties = self._create_common_properties(json_entity)
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        print(f"📝 Creating LifeActivityIn: life_activity_name='{life_activity_name}', properties={len(additional_properties)} (including common)")
        return LifeActivityIn(life_activity_name=life_activity_name, additional_properties=additional_properties)


class ArrangementConverter(BaseEntityConverter[ArrangementIn]):
    JSON_KEY_CANDIDATES_FOR_TYPE_FIELD = ["arrangementType", "hasType", "type"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Arrangement"
    
    def convert(self, json_entity: Dict[str, Any]) -> ArrangementIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None
        arrangement_type = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_TYPE_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=entity_id_str
        )
        
        additional_properties = self._create_common_properties(json_entity)
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_TYPE_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        print(f"📝 Creating ArrangementIn: arrangement_type='{arrangement_type}', properties={len(additional_properties)} (including common)")
        return ArrangementIn(arrangement_type=arrangement_type, additional_properties=additional_properties)


class ParticipantConverter(BaseEntityConverter[ParticipantIn]):
    JSON_KEY_CANDIDATES_FOR_NAME = ["name", "hasName"] 
    JSON_KEY_CANDIDATES_FOR_SEX = ["sex", "hasSex"]
    JSON_KEY_CANDIDATES_FOR_DOB = ["dateOfBirth", "hasDateOfBirth"]
    JSON_KEY_CANDIDATES_FOR_DISORDER = ["disorder", "hasDisorder"] 
    DEFAULT_MAIN_FIELD_PREFIX = "Participant"
    
    def convert(self, json_entity: Dict[str, Any]) -> ParticipantIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        # Wyciągnij nazwę uczestnika
        name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_NAME,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str
        )
        
        # Wyciągnij pola specyficzne dla Participant
        sex = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_SEX)
        date_of_birth = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_DOB)
        disorder = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_DISORDER)
        
        # Utwórz standardowe właściwości
        additional_properties = self._create_common_properties(json_entity)
        
        # Wyklucz już przetworzone klucze z additional_properties
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_NAME)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_SEX)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_DOB)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_DISORDER)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(entity_id_str) if entity_id_str else "Unknown"
        
        disorder_log_msg = f", disorder='{disorder}'" if disorder else ""
        print(f"📝 Creating ParticipantIn: name='{name}', sex='{sex}', date_of_birth='{date_of_birth}'{disorder_log_msg}, properties={len(additional_properties)} (including common)")
        return ParticipantIn(
            name=name,
            sex=sex,
            date_of_birth=date_of_birth,
            additional_properties=additional_properties
        )


class ParticipantStateConverter(BaseEntityConverter["ParticipantStateIn"]):
    JSON_KEY_CANDIDATES_FOR_PARTICIPANT_ID = ["hasParticipant", "participant_id"]
    JSON_KEY_CANDIDATES_FOR_AGE = ["age", "hasAge"]
    JSON_KEY_CANDIDATES_FOR_PERSONALITY = ["hasPersonality", "personality_ids"]
    JSON_KEY_CANDIDATES_FOR_APPEARANCE = ["hasApperance", "hasAppearance", "appearance_ids"]  # Note: "hasApperance" jest w JSON (typo)
    DEFAULT_MAIN_FIELD_PREFIX = "ParticipantState"
    
    def convert(self, json_entity: Dict[str, Any]) -> "ParticipantStateIn":
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None
        
        # Wyciągnij participant_id z zagnieżdżonej struktury hasParticipant
        participant_id = self._extract_participant_id_from_json(json_entity)
        
        # Wyciągnij proste pola
        age = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_AGE)
        if age:
            try:
                age = int(age)
            except (ValueError, TypeError):
                age = None
        
        # Personality IDs - na razie nie implementujemy złożonego wyciągania
        personality_ids = self._extract_related_ids(json_entity, self.JSON_KEY_CANDIDATES_FOR_PERSONALITY)
        
        # Appearance IDs - wyciągnij z hasApperance
        appearance_ids = self._extract_related_ids(json_entity, self.JSON_KEY_CANDIDATES_FOR_APPEARANCE)
        
        # Utwórz standardowe właściwości
        additional_properties = self._create_common_properties(json_entity)
        
        # Wyklucz już przetworzone klucze
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_PARTICIPANT_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_AGE)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_PERSONALITY)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_APPEARANCE)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(entity_id_str) if entity_id_str else "Unknown"
        
        print(f"📝 Creating ParticipantStateIn: name='{clean_name_for_log}', participant_id='{participant_id}', age={age}, properties={len(additional_properties)} (including common)")
        
        return ParticipantStateIn(
            participant_id=participant_id,
            personality_ids=personality_ids,
            appearance_ids=appearance_ids,
            age=age,
            additional_properties=additional_properties
        )
    
    def _extract_participant_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga Participant ID z zagnieżdżonej struktury hasParticipant.
        """
        # Najpierw sprawdź proste przypadki
        simple_participant_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_PARTICIPANT_ID)
        if simple_participant_id:
            print(f"✅ Found simple participant_id: {simple_participant_id}")
            return simple_participant_id
        
        # Następnie sprawdź co:hasParticipant (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_PARTICIPANT_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    participant_id = self._extract_nested_entity_id(entity_value)
                    if participant_id:
                        print(f"✅ Found Participant ID from {entity_key_with_prefix}: {participant_id}")
                        # Zapisujemy Participant ID - mapowanie na MongoDB ID zostanie zrobione później
                        return participant_id
        
        print("⚠️ No Participant reference found in ParticipantState")
        return None
    
    def _extract_related_ids(self, json_entity: Dict[str, Any], key_candidates: List[str]) -> Optional[List[str]]:
        """
        Wyciąga listę powiązanych ID z zagnieżdżonych obiektów.
        """
        ids = []
        
        for clean_candidate_key in key_candidates:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    if isinstance(entity_value, list):
                        for item in entity_value:
                            if isinstance(item, dict) and "@id" in item:
                                ids.append(str(item["@id"]))
                    elif isinstance(entity_value, dict) and "@id" in entity_value:
                        ids.append(str(entity_value["@id"]))
        
        return ids if ids else None
    
    def _extract_nested_entity_id(self, entity_value: Any) -> Optional[str]:
        """
        Wyciąga @id z zagnieżdżonych obiektów JSON.
        """
        if isinstance(entity_value, list) and len(entity_value) > 0:
            first_entity = entity_value[0]
            if isinstance(first_entity, dict) and "@id" in first_entity:
                return str(first_entity["@id"])
        elif isinstance(entity_value, dict) and "@id" in entity_value:
            return str(entity_value["@id"])
        elif isinstance(entity_value, str):
            return entity_value
        
        return None


class TimeSeriesConverter(BaseEntityConverter[TimeSeriesIn]):
    JSON_KEY_CANDIDATES_MEASURE_ID = ["hasMeasure", "measure_id"]
    JSON_KEY_CANDIDATES_OBS_INFO_ID = ["hasObservableInformation", "observable_information_id"]
    JSON_KEY_CANDIDATES_SOURCE = ["timeSeriesSource", "hasSource", "source"]
    JSON_KEY_CANDIDATES_TYPE = ["hasType", "timeSeriesType", "type"]

    def convert(self, json_entity: Dict[str, Any]) -> TimeSeriesIn:
        # Użyj oryginalnego @id dla logowania (po usunięciu prefixu), jeśli istnieje
        raw_entity_id = json_entity.get("@id")
        clean_entity_id_for_log = remove_prefix(str(raw_entity_id)) if raw_entity_id else "unknown_timeseries_id"

        measure_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_MEASURE_ID)
        if measure_id is None:
            print(f"ℹ️ Optional field 'measure_id' not found for TimeSeries '{clean_entity_id_for_log}'.")

        observable_information_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_OBS_INFO_ID)
        if observable_information_id is None:
            print(f"ℹ️ Optional field 'observable_information_id' not found for TimeSeries '{clean_entity_id_for_log}'.")

        source = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_SOURCE)
        if source is None:
            print(f"ℹ️ Optional field 'source' not found for TimeSeries '{clean_entity_id_for_log}'.")

        time_series_type = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_TYPE)
        if time_series_type is None:
            print(f"⚠️ Required field 'type' not found for TimeSeries '{clean_entity_id_for_log}'. Using fallback: 'timeseries'")
            # Ustaw fallback zamiast None żeby Pydantic validation przeszedł
            time_series_type = "timeseries"

        additional_properties = self._create_common_properties(json_entity)
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_MEASURE_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_OBS_INFO_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_SOURCE)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_TYPE)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)

        print(f"📝 Creating TimeSeriesIn for '{clean_entity_id_for_log}': measure_id='{measure_id}', obs_info_id='{observable_information_id}', type='{time_series_type}', source='{source}', properties={len(additional_properties)} (including common)")
        
        return TimeSeriesIn(
            measure_id=measure_id,
            observable_information_id=observable_information_id,
            source=source,
            type=time_series_type, # Jeśli None, Pydantic zgłosi błąd walidacji
            additional_properties=additional_properties
        )


class ExperimentConverter(BaseEntityConverter[ExperimentIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["experimentName", "hasName", "name"] # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_CREATOR = ["creator", "hasCreator", "author", "hasAuthor"] # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_DESCRIPTION = ["description", "hasDescription", "comment", "hasComment"] # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_FOOTNOTE = ["footnote", "hasFootnote", "note", "hasNote"] # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_SCENARIO = ["hasScenario", "scenario", "activityExecutions"] # Czyste klucze dla powiązań z ActivityExecution
    DEFAULT_MAIN_FIELD_PREFIX = "Experiment"
    
    def convert(self, json_entity: Dict[str, Any]) -> ExperimentIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        experiment_name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=entity_id_str
        )
        
        # Szukaj creator w JSON, jeśli nie ma, użyj domyślnego
        creator = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_CREATOR)
        if not creator:
            creator = "system"
            print(f"ℹ️ No creator found for experiment '{experiment_name}', using default: '{creator}'")
        
        # Szukaj description w JSON, jeśli nie ma, użyj domyślnego
        description = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_DESCRIPTION)
        if not description:
            description = f"Added during import {self.import_id}"
            print(f"ℹ️ No description found for experiment '{experiment_name}', using default: '{description}'")
        
        # Szukaj footnote w JSON (opcjonalne, może być puste)
        footnote = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_FOOTNOTE)
        if footnote:
            print(f"✅ Found footnote for experiment '{experiment_name}': '{footnote}'")
        
        # Znajdź powiązania z ActivityExecution (co:hasScenario)
        scenario_activity_executions = self._extract_activity_execution_references(json_entity)
        scenario_data = self._extract_full_scenario_data(json_entity)
        
        additional_properties = self._create_common_properties(json_entity)
        
        # Dodaj creator, description i footnote jako PropertyIn (zgodnie z konwencją UI)
        additional_properties.append(PropertyIn(key="creator", value=creator))
        additional_properties.append(PropertyIn(key="description", value=description))
        if footnote:
            additional_properties.append(PropertyIn(key="footnote", value=footnote))
        
        # Dodaj powiązania z ActivityExecution jako PropertyIn
        if scenario_activity_executions:
            additional_properties.append(PropertyIn(
                key="has_scenario_activity_execution_ids", 
                value=",".join(scenario_activity_executions)
            ))
            print(f"✅ Found {len(scenario_activity_executions)} ActivityExecution references in experiment")
        
        # Dodaj pełne dane scenariuszy jako JSON dla nowej logiki
        if scenario_data:
            additional_properties.append(PropertyIn(
                key="has_scenario_data",
                value=json.dumps(scenario_data)
            ))
            print(f"✅ Saved {len(scenario_data)} full scenario data entries for new scenario logic")
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_CREATOR)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_DESCRIPTION)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_FOOTNOTE)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_SCENARIO)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        footnote_log = f", footnote='{footnote}'" if footnote else ""
        scenario_log = f", scenario_ae_count={len(scenario_activity_executions)}" if scenario_activity_executions else ""
        print(f"📝 Creating ExperimentIn: experiment_name='{experiment_name}', creator='{creator}', description='{description}'{footnote_log}{scenario_log}, properties={len(additional_properties)} (including common)")
        return ExperimentIn(
            experiment_name=experiment_name,
            additional_properties=additional_properties
        )
    
    def _extract_activity_execution_references(self, json_entity: Dict[str, Any]) -> List[str]:
        """
        Wyciąga referencje do ActivityExecution z pól JSON odpowiadających co:hasScenario
        """
        activity_execution_ids = []
        
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_SCENARIO:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # Wartość może być listą obiektów z @id lub prostą listą ID
                    if isinstance(entity_value, list):
                        for item in entity_value:
                            if isinstance(item, dict) and "@id" in item:
                                # Obiekt z @id
                                activity_execution_ids.append(str(item["@id"]))
                            elif isinstance(item, str):
                                # Proste ID jako string
                                activity_execution_ids.append(item)
                    elif isinstance(entity_value, dict) and "@id" in entity_value:
                        # Pojedynczy obiekt z @id  
                        activity_execution_ids.append(str(entity_value["@id"]))
                    elif isinstance(entity_value, str):
                        # Pojedyncze ID jako string
                        activity_execution_ids.append(entity_value)
        
        # Usuń duplikaty zachowując kolejność
        unique_ids = []
        for id_str in activity_execution_ids:
            if id_str and id_str not in unique_ids:
                unique_ids.append(id_str)
        
        return unique_ids

    def _extract_full_scenario_data(self, json_entity: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Wyciąga pełne dane ActivityExecution z co:hasScenario wraz z co:hasActivity.
        Zwraca listę obiektów ActivityExecution z ich pełnymi danymi.
        """
        scenario_data = []
        
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_SCENARIO:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    if isinstance(entity_value, list):
                        for item in entity_value:
                            if isinstance(item, dict):
                                # To jest pełny obiekt ActivityExecution
                                scenario_data.append(item)
                    elif isinstance(entity_value, dict):
                        # Pojedynczy obiekt ActivityExecution
                        scenario_data.append(entity_value)
        
        return scenario_data


class ActivityExecutionConverter(BaseEntityConverter[ActivityExecutionIn]):
    JSON_KEY_CANDIDATES_FOR_ACTIVITY_ID = ["hasActivity", "activity_id", "activityId"] # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_ARRANGEMENT_ID = ["hasArrangement", "arrangement_id", "arrangementId"] # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_NEXT_ID = ["hasNextActivityExecution", "nextActivityExecution", "next_activity_execution_id"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "ActivityExecution"
    
    # Klasowy licznik dla unikalnych nazw scenario execution
    _scenario_execution_counter = 0
    
    def convert(self, json_entity: Dict[str, Any]) -> ActivityExecutionIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        # Pobierz powiązane ID
        activity_id = self._extract_activity_id_from_json(json_entity)
        arrangement_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_ARRANGEMENT_ID)
        next_activity_execution_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_NEXT_ID)
        
        additional_properties = self._create_common_properties(json_entity)
        
        # ✅ DODAJ NAME z @id JSON (bez prefixu)
        if entity_id_str:
            # Usuń prefix z @id żeby mieć czyste ID jako name
            clean_name = remove_prefix(entity_id_str)
            additional_properties.append(PropertyIn(key="name", value=clean_name))
            print(f"✅ Set ActivityExecution name: '{clean_name}' from @id: {entity_id_str}")
        else:
            # Fallback name jeśli nie ma @id
            additional_properties.append(PropertyIn(key="name", value="ActivityExecution"))
            print(f"⚠️ No @id found, using fallback name: 'ActivityExecution'")
        
        # Dodaj description (generyczną)
        additional_properties.append(PropertyIn(key="description", value="Activity execution description"))
        
        # ✅ DODAJ scenarioExecutionName z GLOBALNYM LICZNIKIEM
        ActivityExecutionConverter._scenario_execution_counter += 1
        scenario_execution_name = f"Scenario Execution {ActivityExecutionConverter._scenario_execution_counter}"
        additional_properties.append(PropertyIn(key="scenarioExecutionName", value=scenario_execution_name))
        print(f"✅ Added scenarioExecutionName: '{scenario_execution_name}' to Activity Execution")
        
        # Dodaj next_activity_execution_id jako PropertyIn jeśli istnieje (będzie używane do budowania scenariuszy)
        if next_activity_execution_id:
            additional_properties.append(PropertyIn(key="next_activity_execution_id", value=next_activity_execution_id))
            print(f"✅ Found next activity execution reference: {next_activity_execution_id}")
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_ACTIVITY_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_ARRANGEMENT_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_NEXT_ID)
        # Dodaj "name", "description" i "scenarioExecutionName" do wykluczonych
        processed_clean_keys.extend(["name", "description", "scenarioExecutionName"])
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(entity_id_str) if entity_id_str else "Unknown"
        next_log = f", next_id='{next_activity_execution_id}'" if next_activity_execution_id else ""
        print(f"📝 Creating ActivityExecutionIn: name='{clean_name_for_log}', activity_id='{activity_id}', arrangement_id='{arrangement_id}'{next_log}, scenario_exec_name='{scenario_execution_name}', properties={len(additional_properties)} (including common)")
        
        return ActivityExecutionIn(
            activity_id=activity_id,
            arrangement_id=arrangement_id,
            additional_properties=additional_properties
        )
    
    def _extract_activity_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga Activity ID z JSON, mapuje go na MongoDB ID Activity lub zwraca source ID.
        """
        # Najpierw sprawdź proste przypadki (activity_id, activityId)
        simple_activity_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_ACTIVITY_ID)
        if simple_activity_id:
            print(f"✅ Found simple activity_id: {simple_activity_id}")
            return simple_activity_id
        
        # Następnie sprawdź co:hasActivity (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_ACTIVITY_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    activity_source_id = self._extract_activity_source_id(entity_value)
                    if activity_source_id:
                        print(f"✅ Found Activity source ID from {entity_key_with_prefix}: {activity_source_id}")
                        # Tutaj zapisujemy source ID, mapowanie na MongoDB ID zostanie zrobione przy zapisie
                        return activity_source_id
        
        print("⚠️ No Activity reference found in ActivityExecution")
        return None
    
    def _extract_activity_source_id(self, activity_value: Any) -> Optional[str]:
        """
        Wyciąga source ID Activity z różnych formatów JSON.
        """
        if isinstance(activity_value, list) and len(activity_value) > 0:
            # Lista obiektów Activity
            first_activity = activity_value[0]
            if isinstance(first_activity, dict) and "@id" in first_activity:
                return str(first_activity["@id"])
        elif isinstance(activity_value, dict) and "@id" in activity_value:
            # Pojedynczy obiekt Activity
            return str(activity_value["@id"])
        elif isinstance(activity_value, str):
            # Proste ID jako string
            return activity_value
        
        return None


class ParticipationConverter(BaseEntityConverter[ParticipationIn]):
    JSON_KEY_CANDIDATES_FOR_ACTIVITY_EXECUTION_ID = ["hasActivityExecution", "activity_execution_id"] # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_PARTICIPANT_STATE_ID = ["hasParticipantState", "participant_state_id"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Participation"
    
    def convert(self, json_entity: Dict[str, Any]) -> ParticipationIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        # Wyciągnij ActivityExecution ID z JSON - może być zagnieżdżony obiekt
        activity_execution_id = self._extract_activity_execution_id_from_json(json_entity)
        
        # Wyciągnij ParticipantState ID z JSON - może być zagnieżdżony obiekt  
        participant_state_id = self._extract_participant_state_id_from_json(json_entity)
        
        # UWAGA: ParticipationIn NIE obsługuje additional_properties!
        # Model ma tylko dwa pola: activity_execution_id i participant_state_id
        
        clean_name_for_log = remove_prefix(entity_id_str) if entity_id_str else "Unknown"
        print(f"📝 Creating ParticipationIn: name='{clean_name_for_log}', activity_execution_id='{activity_execution_id}', participant_state_id='{participant_state_id}' (no additional_properties - model limitation)")
        
        return ParticipationIn(
            activity_execution_id=activity_execution_id,
            participant_state_id=participant_state_id
        )
    
    def _extract_activity_execution_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga ActivityExecution ID z JSON, obsługuje zagnieżdżone obiekty.
        """
        # Najpierw sprawdź proste przypadki
        simple_ae_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_ACTIVITY_EXECUTION_ID)
        if simple_ae_id:
            print(f"✅ Found simple activity_execution_id: {simple_ae_id}")
            return simple_ae_id
        
        # Następnie sprawdź co:hasActivityExecution (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_ACTIVITY_EXECUTION_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    ae_source_id = self._extract_nested_entity_id(entity_value)
                    if ae_source_id:
                        print(f"✅ Found ActivityExecution source ID from {entity_key_with_prefix}: {ae_source_id}")
                        # Zapisujemy source ID - mapowanie na MongoDB ID zostanie zrobione później
                        return ae_source_id
        
        print("⚠️ No ActivityExecution reference found in Participation")
        return None
    
    def _extract_participant_state_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga ParticipantState ID z JSON z zagnieżdżonej struktury hasParticipantState.
        POPRAWKA: Teraz używamy prawdziwego ParticipantState ID zamiast Participant ID!
        """
        # Najpierw sprawdź proste przypadki
        simple_ps_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_PARTICIPANT_STATE_ID)
        if simple_ps_id:
            print(f"✅ Found simple participant_state_id: {simple_ps_id}")
            return simple_ps_id
        
        # Następnie sprawdź co:hasParticipantState (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_PARTICIPANT_STATE_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # To jest hasParticipantState - wyciągnij @id ParticipantState (nie hasParticipant!)
                    participant_state_id = self._extract_nested_entity_id(entity_value)
                    if participant_state_id:
                        print(f"✅ Found ParticipantState ID from {entity_key_with_prefix}: {participant_state_id}")
                        # Zapisujemy ParticipantState ID - mapowanie na MongoDB ID zostanie zrobione później
                        return participant_state_id
        
        print("⚠️ No ParticipantState reference found in Participation")
        return None
    
    def _extract_nested_entity_id(self, entity_value: Any) -> Optional[str]:
        """
        Wyciąga @id z zagnieżdżonych obiektów JSON (lista obiektów lub pojedynczy obiekt).
        """
        if isinstance(entity_value, list) and len(entity_value) > 0:
            # Lista obiektów - bierz pierwszy
            first_entity = entity_value[0]
            if isinstance(first_entity, dict) and "@id" in first_entity:
                return str(first_entity["@id"])
        elif isinstance(entity_value, dict) and "@id" in entity_value:
            # Pojedynczy obiekt
            return str(entity_value["@id"])
        elif isinstance(entity_value, str):
            # Proste ID jako string
            return entity_value
        
        return None


class RegisteredDataConverter(BaseEntityConverter[RegisteredDataIn]):
    JSON_KEY_CANDIDATES_FOR_SOURCE = ["source", "hasSource"]  # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "RegisteredData"
    
    def convert(self, json_entity: Dict[str, Any]) -> RegisteredDataIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        # Wyciągnij source - opcjonalny
        source = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_SOURCE)
        
        additional_properties = self._create_common_properties(json_entity)
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_SOURCE)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(entity_id_str) if entity_id_str else "Unknown"
        print(f"📝 Creating RegisteredDataIn: name='{clean_name_for_log}', source='{source}', properties={len(additional_properties)} (including common)")
        
        return RegisteredDataIn(
            source=source,
            additional_properties=additional_properties
        )


class RegisteredChannelConverter(BaseEntityConverter[RegisteredChannelIn]):
    JSON_KEY_CANDIDATES_FOR_CHANNEL_ID = ["hasChannel", "channel_id"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_REGISTERED_DATA_ID = ["hasRegisteredData", "registered_data_id"]  # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "RegisteredChannel"
    
    def convert(self, json_entity: Dict[str, Any]) -> RegisteredChannelIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        # Wyciągnij Channel ID z JSON - może być zagnieżdżony obiekt
        channel_id = self._extract_channel_id_from_json(json_entity)
        
        # Wyciągnij RegisteredData ID z JSON - może być zagnieżdżony obiekt  
        registered_data_id = self._extract_registered_data_id_from_json(json_entity)
        
        additional_properties = self._create_common_properties(json_entity)
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_CHANNEL_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_REGISTERED_DATA_ID)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(entity_id_str) if entity_id_str else "Unknown"
        print(f"📝 Creating RegisteredChannelIn: name='{clean_name_for_log}', channel_id='{channel_id}', registered_data_id='{registered_data_id}', properties={len(additional_properties)} (including common)")
        
        return RegisteredChannelIn(
            channel_id=channel_id,
            registered_data_id=registered_data_id
        )
    
    def _extract_channel_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga Channel ID z JSON, obsługuje zagnieżdżone obiekty.
        """
        # Najpierw sprawdź proste przypadki
        simple_channel_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_CHANNEL_ID)
        if simple_channel_id:
            print(f"✅ Found simple channel_id: {simple_channel_id}")
            return simple_channel_id
        
        # Następnie sprawdź co:hasChannel (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_CHANNEL_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    channel_source_id = self._extract_nested_entity_id(entity_value)
                    if channel_source_id:
                        print(f"✅ Found Channel source ID from {entity_key_with_prefix}: {channel_source_id}")
                        # Zapisujemy source ID - mapowanie na MongoDB ID zostanie zrobione później
                        return channel_source_id
        
        print("⚠️ No Channel reference found in RegisteredChannel")
        return None
    
    def _extract_registered_data_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga RegisteredData ID z JSON z zagnieżdżonej struktury hasRegisteredData.
        """
        # Najpierw sprawdź proste przypadki
        simple_rd_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_REGISTERED_DATA_ID)
        if simple_rd_id:
            print(f"✅ Found simple registered_data_id: {simple_rd_id}")
            return simple_rd_id
        
        # Następnie sprawdź co:hasRegisteredData (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_REGISTERED_DATA_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # To jest hasRegisteredData - wyciągnij @id RegisteredData
                    registered_data_id = self._extract_nested_entity_id(entity_value)
                    if registered_data_id:
                        print(f"✅ Found RegisteredData ID from {entity_key_with_prefix}: {registered_data_id}")
                        # Zapisujemy RegisteredData ID - mapowanie na MongoDB ID zostanie zrobione później
                        return registered_data_id
        
        print("⚠️ No RegisteredData reference found in RegisteredChannel")
        return None
    
    def _extract_nested_entity_id(self, entity_value: Any) -> Optional[str]:
        """
        Wyciąga @id z zagnieżdżonych obiektów JSON (lista obiektów lub pojedynczy obiekt).
        """
        if isinstance(entity_value, list) and len(entity_value) > 0:
            # Lista obiektów - bierz pierwszy
            first_entity = entity_value[0]
            if isinstance(first_entity, dict) and "@id" in first_entity:
                return str(first_entity["@id"])
        elif isinstance(entity_value, dict) and "@id" in entity_value:
            # Pojedynczy obiekt
            return str(entity_value["@id"])
        elif isinstance(entity_value, str):
            # Proste ID jako string
            return entity_value
        
        return None


class RecordingConverter(BaseEntityConverter[RecordingIn]):
    JSON_KEY_CANDIDATES_FOR_PARTICIPATION_ID = ["hasParticipation", "participation_id"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_REGISTERED_CHANNEL_ID = ["hasRegisteredChannel", "registered_channel_id"]  # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Recording"
    
    def convert(self, json_entity: Dict[str, Any]) -> RecordingIn:
        entity_id_str = str(json_entity.get("@id")) if json_entity.get("@id") else None

        # Wyciągnij Participation ID z JSON - może być zagnieżdżony obiekt
        participation_id = self._extract_participation_id_from_json(json_entity)
        
        # Wyciągnij RegisteredChannel ID z JSON - może być zagnieżdżony obiekt  
        registered_channel_id = self._extract_registered_channel_id_from_json(json_entity)
        
        additional_properties = self._create_common_properties(json_entity)
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_PARTICIPATION_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_REGISTERED_CHANNEL_ID)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(entity_id_str) if entity_id_str else "Unknown"
        print(f"📝 Creating RecordingIn: name='{clean_name_for_log}', participation_id='{participation_id}', registered_channel_id='{registered_channel_id}', properties={len(additional_properties)} (including common)")
        
        return RecordingIn(
            participation_id=participation_id,
            registered_channel_id=registered_channel_id,
            additional_properties=additional_properties
        )
    
    def _extract_participation_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga Participation ID z JSON, obsługuje zagnieżdżone obiekty.
        """
        # Najpierw sprawdź proste przypadki
        simple_participation_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_PARTICIPATION_ID)
        if simple_participation_id:
            print(f"✅ Found simple participation_id: {simple_participation_id}")
            return simple_participation_id
        
        # Następnie sprawdź co:hasParticipation (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_PARTICIPATION_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    participation_source_id = self._extract_nested_entity_id(entity_value)
                    if participation_source_id:
                        print(f"✅ Found Participation source ID from {entity_key_with_prefix}: {participation_source_id}")
                        # Zapisujemy source ID - mapowanie na MongoDB ID zostanie zrobione później
                        return participation_source_id
        
        print("⚠️ No Participation reference found in Recording")
        return None
    
    def _extract_registered_channel_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga RegisteredChannel ID z JSON z zagnieżdżonej struktury hasRegisteredChannel.
        """
        # Najpierw sprawdź proste przypadki
        simple_rc_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_REGISTERED_CHANNEL_ID)
        if simple_rc_id:
            print(f"✅ Found simple registered_channel_id: {simple_rc_id}")
            return simple_rc_id
        
        # Następnie sprawdź co:hasRegisteredChannel (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_REGISTERED_CHANNEL_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # To jest hasRegisteredChannel - wyciągnij @id RegisteredChannel
                    registered_channel_id = self._extract_nested_entity_id(entity_value)
                    if registered_channel_id:
                        print(f"✅ Found RegisteredChannel ID from {entity_key_with_prefix}: {registered_channel_id}")
                        # Zapisujemy RegisteredChannel ID - mapowanie na MongoDB ID zostanie zrobione później
                        return registered_channel_id
        
        print("⚠️ No RegisteredChannel reference found in Recording")
        return None
    
    def _extract_nested_entity_id(self, entity_value: Any) -> Optional[str]:
        """
        Wyciąga @id z zagnieżdżonych obiektów JSON (lista obiektów lub pojedynczy obiekt).
        """
        if isinstance(entity_value, list) and len(entity_value) > 0:
            # Lista obiektów - bierz pierwszy
            first_entity = entity_value[0]
            if isinstance(first_entity, dict) and "@id" in first_entity:
                return str(first_entity["@id"])
        elif isinstance(entity_value, dict) and "@id" in entity_value:
            # Pojedynczy obiekt
            return str(entity_value["@id"])
        elif isinstance(entity_value, str):
            # Proste ID jako string
            return entity_value
        
        return None


ENTITY_CONVERTERS = {
    "Activity": ActivityConverter,
    "Channel": ChannelConverter,
    "MeasureName": MeasureNameConverter,
    "Measure": MeasureConverter,
    "Modality": ModalityConverter,
    "LifeActivity": LifeActivityConverter,
    "Arrangement": ArrangementConverter,
    "Participant": ParticipantConverter,
    "ParticipantState": ParticipantStateConverter,
    "TimeSeries": TimeSeriesConverter,
    "Experiment": ExperimentConverter,
    "ActivityExecution": ActivityExecutionConverter,
    "Participation": ParticipationConverter,
    "Recording": RecordingConverter,
    "RegisteredData": RegisteredDataConverter,
    "RegisteredChannel": RegisteredChannelConverter,
}
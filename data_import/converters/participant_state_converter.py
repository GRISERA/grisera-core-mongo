from typing import Dict, Any, Optional, List
from grisera import ParticipantStateIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class ParticipantStateConverter(BaseEntityConverter["ParticipantStateIn"]):
    JSON_KEY_CANDIDATES_FOR_PARTICIPANT_ID = ["hasParticipant", "participant_id"]
    JSON_KEY_CANDIDATES_FOR_AGE = ["age", "hasAge"]
    JSON_KEY_CANDIDATES_FOR_PERSONALITY = ["hasPersonality", "personality_ids"]
    JSON_KEY_CANDIDATES_FOR_APPEARANCE = ["hasApperance", "hasAppearance", "appearance_ids"]  # Note: "hasApperance" jest w JSON (typo)
    DEFAULT_MAIN_FIELD_PREFIX = "ParticipantState"
    
    def convert(self, json_entity: Dict[str, Any]) -> "ParticipantStateIn":
        external_id = self._get_external_id(json_entity)
        
        # Wyciągnij participant_id z zagnieżdżonej struktury hasParticipant
        participant_id = self._extract_participant_id_from_json(json_entity)
        
        # Wyciągnij proste pola
        age = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_AGE)
        if age:
            try:
                age = int(age)
            except (ValueError, TypeError):
                age = None
        
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
        
        clean_name_for_log = remove_prefix(external_id) if external_id else "Unknown"
        
        print(f"📝 Creating ParticipantStateIn: name='{clean_name_for_log}', participant_id='{participant_id}', age={age}, external_id='{external_id}', properties={len(additional_properties)} (including common)")
        
        return ParticipantStateIn(
            participant_id=participant_id,
            personality_ids=personality_ids,
            appearance_ids=appearance_ids,
            age=age,
            external_id=external_id,
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



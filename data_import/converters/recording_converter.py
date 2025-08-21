from typing import Dict, Any, Optional
from grisera import RecordingIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class RecordingConverter(BaseEntityConverter[RecordingIn]):
    JSON_KEY_CANDIDATES_FOR_PARTICIPATION_ID = ["hasParticipation", "participation_id"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_REGISTERED_CHANNEL_ID = ["hasRegisteredChannel", "registered_channel_id"]  # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Recording"
    
    def convert(self, json_entity: Dict[str, Any]) -> RecordingIn:
        external_id = self._get_external_id(json_entity)

        # Wyciągnij Participation ID z JSON - może być zagnieżdżony obiekt
        participation_id = self._extract_participation_id_from_json(json_entity)
        
        # Wyciągnij RegisteredChannel ID z JSON - może być zagnieżdżony obiekt  
        registered_channel_id = self._extract_registered_channel_id_from_json(json_entity)
        
        additional_properties = self._create_common_properties(json_entity)
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_PARTICIPATION_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_REGISTERED_CHANNEL_ID)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(external_id) if external_id else "Unknown"
        print(f"📝 Creating RecordingIn: name='{clean_name_for_log}', participation_id='{participation_id}', registered_channel_id='{registered_channel_id}', external_id='{external_id}', properties={len(additional_properties)} (including common)")
        
        return RecordingIn(
            participation_id=participation_id,
            registered_channel_id=registered_channel_id,
            external_id=external_id,
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



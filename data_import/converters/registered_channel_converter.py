from typing import Dict, Any, Optional
from grisera import RegisteredChannelIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class RegisteredChannelConverter(BaseEntityConverter[RegisteredChannelIn]):
    JSON_KEY_CANDIDATES_FOR_CHANNEL_ID = ["hasChannel", "channel_id"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_REGISTERED_DATA_ID = ["hasRegisteredData", "registered_data_id"]  # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "RegisteredChannel"
    
    def convert(self, json_entity: Dict[str, Any]) -> RegisteredChannelIn:
        external_id = self._get_external_id(json_entity)

        # Wyciągnij Channel ID z JSON - może być zagnieżdżony obiekt
        channel_id = self._extract_channel_id_from_json(json_entity)
        
        # Wyciągnij RegisteredData ID z JSON - może być zagnieżdżony obiekt  
        registered_data_id = self._extract_registered_data_id_from_json(json_entity)
        
        registered_channel = RegisteredChannelIn(
            channel_id=channel_id,
            registered_data_id=registered_data_id
        )

        additional_properties = self._set_common_properties(json_entity, registered_channel)

        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_CHANNEL_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_REGISTERED_DATA_ID)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)

        clean_name_for_log = remove_prefix(external_id) if external_id else "Unknown"
        print(f"📝 Creating RegisteredChannelIn: name='{clean_name_for_log}', channel_id='{channel_id}', registered_data_id='{registered_data_id}', external_id='{registered_channel.external_id}', import_job_id='{registered_channel.import_job_id}', properties={len(additional_properties)} (including common)")

        return registered_channel
    
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



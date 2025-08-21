from typing import Dict, Any
from grisera import RegisteredDataIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class RegisteredDataConverter(BaseEntityConverter[RegisteredDataIn]):
    JSON_KEY_CANDIDATES_FOR_SOURCE = ["source", "hasSource"]  # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "RegisteredData"
    
    def convert(self, json_entity: Dict[str, Any]) -> RegisteredDataIn:
        external_id = self._get_external_id(json_entity)

        # Wyciągnij source - opcjonalny
        source = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_SOURCE)
        
        additional_properties = self._create_common_properties(json_entity)
        
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_SOURCE)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(external_id) if external_id else "Unknown"
        print(f"📝 Creating RegisteredDataIn: name='{clean_name_for_log}', source='{source}', external_id='{external_id}', properties={len(additional_properties)} (including common)")
        
        return RegisteredDataIn(
            source=source,
            external_id=external_id,
            additional_properties=additional_properties
        )



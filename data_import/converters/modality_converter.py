from typing import Dict, Any
from grisera import ModalityIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class ModalityConverter(BaseEntityConverter[ModalityIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["modalityName", "hasName", "name"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Modality"
    
    def convert(self, json_entity: Dict[str, Any]) -> ModalityIn:
        external_id = self._get_external_id(json_entity)
        modality_name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=external_id
        )
        
        additional_properties = self._create_common_properties(json_entity)
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        print(f"📝 Creating ModalityIn: modality_name='{modality_name}', external_id='{external_id}', properties={len(additional_properties)} (including common)")
        return ModalityIn(modality=modality_name, external_id=external_id)



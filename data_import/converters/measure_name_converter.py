from typing import Dict, Any
from grisera import MeasureNameIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class MeasureNameConverter(BaseEntityConverter[MeasureNameIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["measureName", "hasName", "name"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Measure"
    
    def convert(self, json_entity: Dict[str, Any]) -> MeasureNameIn:
        external_id = self._get_external_id(json_entity)
        measure_name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=external_id
        )
        
        additional_properties = self._create_common_properties(json_entity)
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        print(f"📝 Creating MeasureNameIn: measure_name='{measure_name}', external_id='{external_id}', properties={len(additional_properties)} (including common)")
        return MeasureNameIn(
            name=measure_name,
            type="User defined",
            external_id=external_id
        )



from typing import Dict, Any
from grisera import LifeActivityIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class LifeActivityConverter(BaseEntityConverter[LifeActivityIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["lifeActivityName", "hasName", "name"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "LifeActivity"
    
    def convert(self, json_entity: Dict[str, Any]) -> LifeActivityIn:
        external_id = self._get_external_id(json_entity)
        life_activity_name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=external_id
        )
        
        life_activity = LifeActivityIn(life_activity=life_activity_name)

        additional_properties = self._set_common_properties(json_entity, life_activity)

        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)

        print(f"📝 Creating LifeActivityIn: life_activity_name='{life_activity_name}', external_id='{life_activity.external_id}', import_job_id='{life_activity.import_job_id}', properties={len(additional_properties)} (including common)")
        return life_activity



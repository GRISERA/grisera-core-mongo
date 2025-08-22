from typing import Dict, Any
from grisera import ArrangementIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class ArrangementConverter(BaseEntityConverter[ArrangementIn]):
    JSON_KEY_CANDIDATES_FOR_TYPE_FIELD = ["arrangementType", "hasType", "type"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Arrangement"
    
    def convert(self, json_entity: Dict[str, Any]) -> ArrangementIn:
        external_id = self._get_external_id(json_entity)
        arrangement_type = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_TYPE_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=external_id
        )
        
        arrangement = ArrangementIn(arrangement_type=arrangement_type)

        additional_properties = self._set_common_properties(json_entity, arrangement)

        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_TYPE_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)

        print(f"📝 Creating ArrangementIn: arrangement_type='{arrangement_type}', external_id='{arrangement.external_id}', import_job_id='{arrangement.import_job_id}', properties={len(additional_properties)} (including common)")
        return arrangement



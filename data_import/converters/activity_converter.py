from typing import Dict, Any
from grisera import ActivityIn, PropertyIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class ActivityConverter(BaseEntityConverter[ActivityIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["hasActivityType", "activityType", "hasType", "type"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Activity"
    
    def convert(self, json_entity: Dict[str, Any]) -> ActivityIn:
        activity_type = "individual" # TODO: zmienić na wyciąganie z JSON

        activity = ActivityIn(activity=activity_type)

        additional_properties = self._set_common_properties(json_entity, activity)

        # Dodaj name z external_id (bez prefixu)
        external_id = self._get_external_id(json_entity)
        if external_id:
            clean_name = remove_prefix(external_id)
            additional_properties.append(PropertyIn(key="name", value=clean_name))
        else:
            additional_properties.append(PropertyIn(key="name", value="Activity"))

        additional_properties.append(PropertyIn(key="description", value="Activity description"))

        # Wykluczamy już przetworzone klucze
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        processed_clean_keys.extend(["name", "description"])  # Dodajemy do wykluczonych
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        activity.additional_properties = additional_properties
        print(f"📝 Creating ActivityIn: activity='{activity_type}', external_id='{activity.external_id}', import_job_id='{activity.import_job_id}', properties={len(additional_properties)} (including common)")
        return activity

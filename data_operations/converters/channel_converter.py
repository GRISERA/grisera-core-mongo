from typing import Dict, Any
from grisera import ChannelIn
from .base import BaseEntityConverter
from data_operations.utils import remove_prefix


class ChannelConverter(BaseEntityConverter[ChannelIn]):
    JSON_KEY_CANDIDATES_FOR_MAIN_FIELD = ["channelName", "hasName", "name"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Channel"
    
    def convert(self, json_entity: Dict[str, Any]) -> ChannelIn:
        external_id = self._get_external_id(json_entity)

        channel_name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            entity_id_str_for_fallback=external_id
        )
        
        channel = ChannelIn(channel_name=channel_name)

        additional_properties = self._set_common_properties(json_entity, channel)

        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MAIN_FIELD)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        channel.additional_properties = additional_properties
        print(f"📝 Creating ChannelIn: channel_name='{channel_name}', external_id='{channel.external_id}', import_job_id='{channel.import_job_id}', properties={len(additional_properties)} (including common)")
        return channel



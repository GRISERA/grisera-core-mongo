from typing import Dict, Any
from grisera import ParticipantIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class ParticipantConverter(BaseEntityConverter[ParticipantIn]):
    JSON_KEY_CANDIDATES_FOR_NAME = ["name", "hasName"] 
    JSON_KEY_CANDIDATES_FOR_SEX = ["sex", "hasSex"]
    JSON_KEY_CANDIDATES_FOR_DOB = ["dateOfBirth", "hasDateOfBirth"]
    JSON_KEY_CANDIDATES_FOR_DISORDER = ["disorder", "hasDisorder"] 
    DEFAULT_MAIN_FIELD_PREFIX = "Participant"
    
    def convert(self, json_entity: Dict[str, Any]) -> ParticipantIn:
        external_id = self._get_external_id(json_entity)

        # Wyciągnij nazwę uczestnika
        name = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_NAME,
            self.DEFAULT_MAIN_FIELD_PREFIX,
            external_id
        )
        
        # Wyciągnij pola specyficzne dla Participant
        sex = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_SEX)
        date_of_birth = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_DOB)
        disorder = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_DISORDER)
        
        # Utwórz standardowe właściwości
        additional_properties = self._create_common_properties(json_entity)
        
        # Wyklucz już przetworzone klucze z additional_properties
        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_NAME)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_SEX)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_DOB)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_DISORDER)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)
        
        clean_name_for_log = remove_prefix(external_id) if external_id else "Unknown"
        
        disorder_log_msg = f", disorder='{disorder}'" if disorder else ""
        print(f"📝 Creating ParticipantIn: name='{name}', sex='{sex}', date_of_birth='{date_of_birth}'{disorder_log_msg}, external_id='{external_id}', properties={len(additional_properties)} (including common)")
        return ParticipantIn(
            name=name,
            sex=sex,
            date_of_birth=date_of_birth,
            disorder=disorder,
            external_id=external_id,
            additional_properties=additional_properties
        )



from typing import Dict, Any, Optional
from grisera import ObservableInformationIn
from .base import BaseEntityConverter
from data_operations.utils import remove_prefix


class ObservableInformationConverter(BaseEntityConverter[ObservableInformationIn]):
    JSON_KEY_CANDIDATES_FOR_MODALITY_ID = ["hasModality", "modality_id"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_LIFE_ACTIVITY_ID = ["hasLifeActivity", "life_activity_id"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_RECORDING_ID = ["hasRecording", "recording_id"]  # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "ObservableInformation"
    
    def convert(self, json_entity: Dict[str, Any]) -> ObservableInformationIn:
        external_id = self._get_external_id(json_entity)

        modality_id = self._extract_modality_id_from_json(json_entity)
        
        life_activity_id = self._extract_life_activity_id_from_json(json_entity)
        
        recording_id = self._extract_recording_id_from_json(json_entity)
        
        clean_name_for_log = remove_prefix(external_id) if external_id else "Unknown"
        print(f"📝 Creating ObservableInformationIn: name='{clean_name_for_log}', modality_id='{modality_id}', life_activity_id='{life_activity_id}', recording_id='{recording_id}', external_id='{external_id}'")
        
        observable_information = ObservableInformationIn(
            modality_id=modality_id,
            life_activity_id=life_activity_id,
            recording_id=recording_id,
            # external_id=external_id
        )
        additional_properties = self._set_common_properties(json_entity, observable_information)
        return observable_information

    
    def _extract_modality_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga Modality ID z JSON, obsługuje zagnieżdżone obiekty.
        """
        # Najpierw sprawdź proste przypadki
        simple_modality_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_MODALITY_ID)
        if simple_modality_id:
            print(f"✅ Found simple modality_id: {simple_modality_id}")
            return simple_modality_id
        
        # Następnie sprawdź co:hasModality (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_MODALITY_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    modality_source_id = self._extract_nested_entity_id(entity_value)
                    if modality_source_id:
                        print(f"✅ Found Modality source ID from {entity_key_with_prefix}: {modality_source_id}")
                        # Zapisujemy source ID - mapowanie na MongoDB ID zostanie zrobione później
                        return modality_source_id
        
        print("⚠️ No Modality reference found in ObservableInformation")
        return None
    
    def _extract_life_activity_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga LifeActivity ID z JSON z zagnieżdżonej struktury hasLifeActivity.
        """
        # Najpierw sprawdź proste przypadki
        simple_la_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_LIFE_ACTIVITY_ID)
        if simple_la_id:
            print(f"✅ Found simple life_activity_id: {simple_la_id}")
            return simple_la_id
        
        # Następnie sprawdź co:hasLifeActivity (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_LIFE_ACTIVITY_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # To jest hasLifeActivity - wyciągnij @id LifeActivity
                    life_activity_id = self._extract_nested_entity_id(entity_value)
                    if life_activity_id:
                        print(f"✅ Found LifeActivity ID from {entity_key_with_prefix}: {life_activity_id}")
                        # Zapisujemy LifeActivity ID - mapowanie na MongoDB ID zostanie zrobione później
                        return life_activity_id
        
        print("⚠️ No LifeActivity reference found in ObservableInformation")
        return None
    
    def _extract_recording_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga Recording ID z JSON z zagnieżdżonej struktury hasRecording.
        """
        # Najpierw sprawdź proste przypadki
        simple_recording_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_RECORDING_ID)
        if simple_recording_id:
            print(f"✅ Found simple recording_id: {simple_recording_id}")
            return simple_recording_id
        
        # Następnie sprawdź co:hasRecording (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_RECORDING_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # To jest hasRecording - wyciągnij @id Recording
                    recording_id = self._extract_nested_entity_id(entity_value)
                    if recording_id:
                        print(f"✅ Found Recording ID from {entity_key_with_prefix}: {recording_id}")
                        # Zapisujemy Recording ID - mapowanie na MongoDB ID zostanie zrobione później
                        return recording_id
        
        print("⚠️ No Recording reference found in ObservableInformation")
        return None




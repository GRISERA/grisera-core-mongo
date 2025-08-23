from typing import Dict, Any, Optional
from grisera import ParticipationIn
from .base import BaseEntityConverter
from data_operations.utils import remove_prefix


class ParticipationConverter(BaseEntityConverter[ParticipationIn]):
    JSON_KEY_CANDIDATES_FOR_ACTIVITY_EXECUTION_ID = ["hasActivityExecution", "activity_execution_id"] # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_PARTICIPANT_STATE_ID = ["hasParticipantState", "participant_state_id"] # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Participation"
    
    def convert(self, json_entity: Dict[str, Any]) -> ParticipationIn:
        external_id = self._get_external_id(json_entity)

        # Wyciągnij ActivityExecution ID z JSON - może być zagnieżdżony obiekt
        activity_execution_id = self._extract_activity_execution_id_from_json(json_entity)
        
        # Wyciągnij ParticipantState ID z JSON - może być zagnieżdżony obiekt  
        participant_state_id = self._extract_participant_state_id_from_json(json_entity)
        
        # UWAGA: ParticipationIn NIE obsługuje additional_properties!
        # Model ma tylko dwa pola: activity_execution_id i participant_state_id
        
        clean_name_for_log = remove_prefix(external_id) if external_id else "Unknown"
        print(f"📝 Creating ParticipationIn: name='{clean_name_for_log}', activity_execution_id='{activity_execution_id}', participant_state_id='{participant_state_id}', external_id='{external_id}' (no additional_properties - model limitation)")
        
        return ParticipationIn(
            activity_execution_id=activity_execution_id,
            participant_state_id=participant_state_id,
            external_id=external_id
        )
    
    def _extract_activity_execution_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga ActivityExecution ID z JSON, obsługuje zagnieżdżone obiekty.
        """
        # Najpierw sprawdź proste przypadki
        simple_ae_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_ACTIVITY_EXECUTION_ID)
        if simple_ae_id:
            print(f"✅ Found simple activity_execution_id: {simple_ae_id}")
            return simple_ae_id
        
        # Następnie sprawdź co:hasActivityExecution (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_ACTIVITY_EXECUTION_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    ae_source_id = self._extract_nested_entity_id(entity_value)
                    if ae_source_id:
                        print(f"✅ Found ActivityExecution source ID from {entity_key_with_prefix}: {ae_source_id}")
                        # Zapisujemy source ID - mapowanie na MongoDB ID zostanie zrobione później
                        return ae_source_id
        
        print("⚠️ No ActivityExecution reference found in Participation")
        return None
    
    def _extract_participant_state_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga ParticipantState ID z JSON z zagnieżdżonej struktury hasParticipantState.
        POPRAWKA: Teraz używamy prawdziwego ParticipantState ID zamiast Participant ID!
        """
        # Najpierw sprawdź proste przypadki
        simple_ps_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_PARTICIPANT_STATE_ID)
        if simple_ps_id:
            print(f"✅ Found simple participant_state_id: {simple_ps_id}")
            return simple_ps_id
        
        # Następnie sprawdź co:hasParticipantState (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_PARTICIPANT_STATE_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # To jest hasParticipantState - wyciągnij @id ParticipantState (nie hasParticipant!)
                    participant_state_id = self._extract_nested_entity_id(entity_value)
                    if participant_state_id:
                        print(f"✅ Found ParticipantState ID from {entity_key_with_prefix}: {participant_state_id}")
                        # Zapisujemy ParticipantState ID - mapowanie na MongoDB ID zostanie zrobione później
                        return participant_state_id
        
        print("⚠️ No ParticipantState reference found in Participation")
        return None



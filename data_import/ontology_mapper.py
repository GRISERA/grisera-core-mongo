from typing import Dict


class OntologyPropertyMapper:
    """
    Klasa odpowiedzialna za mapowanie właściwości ontologicznych na pola GRISERA
    """
    
    _PROPERTY_MAPPINGS = {
        "Activity": {
            # Właściwości Activity - WYMAGANE POLE!
            "hasActivityType": "activity",                # hasActivityType -> activity (wymagane!)
            "activityType": "activity",                   # alternatywna nazwa  
            "hasType": "activity",                        # alternatywna nazwa
            "type": "activity",                           # alternatywna nazwa
        },
        "TimeSeries": {
            # Właściwości TimeSeries
            "hasMeasure": "measure_id",                    # hasMeasure -> measure_id  
            "hasObservableInformation": "observable_information_id",  # hasObservableInformation -> observable_information_id
            "timeSeriesSource": "source",                  # timeSeriesSource -> source
            "hasSource": "source",                         # alternatywna nazwa
            "hasType": "type",                            # hasType -> type
            "timeSeriesType": "type",                     # alternatywna nazwa
        },
        "Participant": {
            # Właściwości Participant  
            "hasName": "name",                            # hasName -> name
            "hasSex": "sex",                             # hasSex -> sex
            "dateOfBirth": "date_of_birth",              # dateOfBirth -> date_of_birth  
            "hasDateOfBirth": "date_of_birth",           # alternatywna nazwa
            "hasDisorder": "disorder",                    # hasDisorder -> disorder
        },
        "ObservableInformation": {
            # Właściwości ObservableInformation
            "hasModality": "modality_id",                 # hasModality -> modality_id
            "hasLifeActivity": "life_activity_id",        # hasLifeActivity -> life_activity_id  
            "belongsToRecording": "recording_id",         # belongsToRecording -> recording_id
        },
        "Recording": {
            # Właściwości Recording  
            "belongsToParticipation": "participation_id", # belongsToParticipation -> participation_id
            "hasRegisteredChannel": "registered_channel_id", # hasRegisteredChannel -> registered_channel_id
        },
        "Participation": {
            # Właściwości Participation
            "hasParticipantState": "participant_state_id", # hasParticipantState -> participant_state_id  
            "belongsToActivityExecution": "activity_execution_id", # belongsToActivityExecution -> activity_execution_id
        },
        "ActivityExecution": {
            # Właściwości ActivityExecution
            "hasActivity": "activity_id",                 # hasActivity -> activity_id
            "hasArrangement": "arrangement_id",           # hasArrangement -> arrangement_id
            "hasNextActivityExecution": "next_activity_execution_id", # hasNextActivityExecution -> next_activity_execution_id
        },
        "Measure": {
            # Właściwości Measure  
            "hasMeasureName": "measure_name_id",          # hasMeasureName -> measure_name_id
        },
        "MeasureName": {
            # Właściwości MeasureName
            "measureName": "measure_name",                # measureName -> measure_name (wymagane!)
            "hasName": "measure_name",                    # alternatywna nazwa
            "name": "measure_name",                       # alternatywna nazwa
        },
        "Channel": {
            # Właściwości Channel  
            "channelName": "channel_name",                # channelName -> channel_name (wymagane!)
            "hasName": "channel_name",                    # alternatywna nazwa
            "name": "channel_name",                       # alternatywna nazwa
        },
        "Modality": {
            # Właściwości Modality
            "modalityName": "modality_name",              # modalityName -> modality_name (wymagane!)
            "hasName": "modality_name",                   # alternatywna nazwa
            "name": "modality_name",                      # alternatywna nazwa
        },
        "LifeActivity": {
            # Właściwości LifeActivity
            "lifeActivityName": "life_activity_name",     # lifeActivityName -> life_activity_name (wymagane!)
            "hasName": "life_activity_name",              # alternatywna nazwa
            "name": "life_activity_name",                 # alternatywna nazwa
        },
        "Arrangement": {
            # Właściwości Arrangement
            "arrangementType": "arrangement_type",        # arrangementType -> arrangement_type (wymagane!)
            "hasType": "arrangement_type",                # alternatywna nazwa
            "type": "arrangement_type",                   # alternatywna nazwa
        },
        "RegisteredChannel": {
            # Właściwości RegisteredChannel
            "hasChannel": "channel_id",                   # hasChannel -> channel_id
            "hasRegisteredData": "registered_data_id",    # hasRegisteredData -> registered_data_id
        },
        "RegisteredData": {
            # Właściwości RegisteredData  
            "dataInfo": "data_info",                      # dataInfo -> data_info (wymagane!)
            "hasDataInfo": "data_info",                   # alternatywna nazwa
        },
        "Experiment": {
            # Właściwości Experiment
            "experimentName": "experiment_name",          # experimentName -> experiment_name (wymagane!)
            "hasName": "experiment_name",                 # alternatywna nazwa
            "name": "experiment_name",                    # alternatywna nazwa
        },
        "ParticipantState": {
            # Właściwości ParticipantState
            "hasParticipant": "participant_id",           # hasParticipant -> participant_id
        },
        "Scenario": {
            # Właściwości Scenario  
            "scenarioName": "scenario_name",              # scenarioName -> scenario_name
            "hasName": "scenario_name",                   # alternatywna nazwa
            "name": "scenario_name",                      # alternatywna nazwa
        }
        # Dodać mapowania dla innych typów encji w miarę potrzeb...
    }
    
    def get_mapping(self, entity_type: str) -> Dict[str, str]:
        """
        Pobiera mapowanie właściwości dla danego typu encji
        
        Args:
            entity_type: Typ encji (np. "Activity", "Channel")
            
        Returns:
            Słownik mapowania {ontology_property: grisera_field} lub pusty słownik
        """
        return self._PROPERTY_MAPPINGS.get(entity_type, {}) 
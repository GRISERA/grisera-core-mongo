from typing import Dict, Any, List
from grisera import TimeSeriesIn
from .base import BaseEntityConverter
from data_operations.utils import remove_prefix


class TimeSeriesConverter(BaseEntityConverter[TimeSeriesIn]):
    JSON_KEY_CANDIDATES_MEASURE_ID = ["hasMeasure", "measure_id"]
    JSON_KEY_CANDIDATES_OBS_INFO_ID = ["hasObservableInformation", "observable_information_id"]
    JSON_KEY_CANDIDATES_SOURCE = ["timeSeriesSource", "hasSource", "source"]
    JSON_KEY_CANDIDATES_TYPE = ["hasType", "timeSeriesType", "type"]

    def convert(self, json_entity: Dict[str, Any]) -> TimeSeriesIn:
        external_id = self._get_external_id(json_entity)
        # Użyj oryginalnego @id dla logowania (po usunięciu prefixu), jeśli istnieje
        raw_entity_id = json_entity.get("@id")
        clean_entity_id_for_log = remove_prefix(str(raw_entity_id)) if raw_entity_id else "unknown_timeseries_id"

        measure_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_MEASURE_ID)
        if measure_id is None:
            print(f"ℹ️ Optional field 'measure_id' not found for TimeSeries '{clean_entity_id_for_log}'.")

        observable_information_ids = self._get_observable_information_ids(json_entity)
        if observable_information_ids is None:
            print(f"ℹ️ Optional field 'observable_information_ids' not found for TimeSeries '{clean_entity_id_for_log}'.")

        source = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_SOURCE)
        if source is None:
            print(f"ℹ️ Optional field 'source' not found for TimeSeries '{clean_entity_id_for_log}'.")

        time_series_type = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_TYPE)
        if time_series_type is None:
            print(f"⚠️ Required field 'type' not found for TimeSeries '{clean_entity_id_for_log}'. Using fallback: 'Timestamp'")
            # Ustaw fallback na prawidłową wartość enum
            time_series_type = "Timestamp"

        time_series = TimeSeriesIn(
            measure_id=measure_id,
            observable_information_ids=observable_information_ids,
            observable_information_id = observable_information_ids[0] if observable_information_ids else None,
            source=source,
            type=time_series_type
        )

        additional_properties = self._set_common_properties(json_entity, time_series)

        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_MEASURE_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_OBS_INFO_ID)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_SOURCE)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_TYPE)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)

        print(f"📝 Creating TimeSeriesIn for '{clean_entity_id_for_log}': measure_id='{measure_id}', obs_info_ids='{observable_information_ids}', type='{time_series_type}', source='{source}', external_id='{time_series.external_id}', import_job_id='{time_series.import_job_id}', properties={len(additional_properties)} (including common)")
        time_series.additional_properties = additional_properties
        return time_series


    def _get_observable_information_ids(self, json_entity: Dict[str, Any]) -> List[str]:
        """
        Pobiera ID-ki z co:hasObservableInformation (które jest listą obiektów).
        Zwraca listę ID-ków jako stringi.
        """
        observable_info_ids = []

        # Szukaj klucza co:hasObservableInformation
        for entity_key, entity_value in json_entity.items():
            if remove_prefix(entity_key) == "hasObservableInformation":
                # To powinna być lista obiektów
                if isinstance(entity_value, list):
                    for obs_info in entity_value:
                        if isinstance(obs_info, dict):
                            # Szukaj @id w obiekcie ObservableInformation
                            obs_id = obs_info.get("@id")
                            if obs_id and obs_id.strip():
                                observable_info_ids.append(obs_id)
                break

        return observable_info_ids


from typing import Dict, Any, Optional
from grisera import MeasureIn
from .base import BaseEntityConverter
from data_import.utils import remove_prefix


class MeasureConverter(BaseEntityConverter[MeasureIn]):
    JSON_KEY_CANDIDATES_FOR_DATATYPE = ["measureDatatype", "hasDatatype", "datatype"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_RANGE = ["measureRange", "hasRange", "range"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_UNIT = ["measureUnit", "hasUnit", "unit"]  # Czyste klucze
    JSON_KEY_CANDIDATES_FOR_MEASURE_NAME_ID = ["hasMeasureName", "measure_name_id", "measureNameId"]  # Czyste klucze
    DEFAULT_MAIN_FIELD_PREFIX = "Measure"
    
    def convert(self, json_entity: Dict[str, Any]) -> MeasureIn:
        external_id = self._get_external_id(json_entity)

        # Wyciągnij datatype - pole wymagane
        datatype = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_DATATYPE,
            "float",  # domyślna wartość
            entity_id_str_for_fallback=external_id
        )
        
        # Wyciągnij range - pole wymagane
        range_value = self._get_main_field_value(
            json_entity,
            self.JSON_KEY_CANDIDATES_FOR_RANGE,
            "unknown",  # domyślna wartość
            entity_id_str_for_fallback=external_id
        )
        
        # Wyciągnij unit - pole wymagane, ale może nie być w JSON
        unit = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_UNIT)
        if not unit:
            unit = "unknown"  # domyślna wartość
        
        # Wyciągnij measure_name_id z zagnieżdżonej struktury co:hasMeasureName
        measure_name_id = self._extract_measure_name_id_from_json(json_entity)
        
        measure = MeasureIn(
            datatype=datatype,
            range=range_value,
            unit=unit,
            measure_name_id=measure_name_id
        )

        additional_properties = self._set_common_properties(json_entity, measure)

        processed_clean_keys = []
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_DATATYPE)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_RANGE)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_UNIT)
        processed_clean_keys.extend(self.JSON_KEY_CANDIDATES_FOR_MEASURE_NAME_ID)
        self._add_remaining_properties(json_entity, additional_properties, processed_clean_keys)

        clean_name_for_log = remove_prefix(external_id) if external_id else "Unknown"
        print(f"📝 Creating MeasureIn: name='{clean_name_for_log}', datatype='{datatype}', range='{range_value}', unit='{unit}', measure_name_id='{measure_name_id}', external_id='{measure.external_id}', import_job_id='{measure.import_job_id}', properties={len(additional_properties)} (including common)")

        return measure
    
    def _extract_measure_name_id_from_json(self, json_entity: Dict[str, Any]) -> Optional[str]:
        """
        Wyciąga MeasureName ID z JSON z zagnieżdżonej struktury co:hasMeasureName.
        Zwraca source ID który będzie mapowany na MongoDB ID później.
        """
        # Najpierw sprawdź proste przypadki
        simple_mn_id = self._get_optional_field_value(json_entity, self.JSON_KEY_CANDIDATES_FOR_MEASURE_NAME_ID)
        if simple_mn_id:
            print(f"✅ Found simple measure_name_id: {simple_mn_id}")
            return simple_mn_id
        
        # Następnie sprawdź co:hasMeasureName (zagnieżdżony obiekt)
        for clean_candidate_key in self.JSON_KEY_CANDIDATES_FOR_MEASURE_NAME_ID:
            for entity_key_with_prefix, entity_value in json_entity.items():
                if remove_prefix(entity_key_with_prefix) == clean_candidate_key:
                    # To jest hasMeasureName - wyciągnij @id MeasureName
                    measure_name_id = self._extract_nested_entity_id(entity_value)
                    if measure_name_id:
                        print(f"✅ Found MeasureName source ID from {entity_key_with_prefix}: {measure_name_id}")
                        # Zapisujemy source ID - mapowanie na MongoDB ID zostanie zrobione później
                        return measure_name_id
        
        print("⚠️ No MeasureName reference found in Measure")
        return None



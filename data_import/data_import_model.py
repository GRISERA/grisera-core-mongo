from typing import List, Optional, Any, Dict
from pydantic import BaseModel
from enum import Enum


class ImportStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class DataImportIn(BaseModel):
    file_name: str
    file_content: str
    import_type: str
    dataset_id: str
    description: Optional[str] = None
    mapping_config: Optional[Dict[str, Any]] = None  # Konfiguracja mapowania kolumn
    experiment_id: Optional[str] = None


class DataImportOut(BaseModel):
    """Model wyjściowy dla importu danych"""
    id: str
    file_name: Optional[str]
    import_type: Optional[str]
    status: ImportStatus
    description: Optional[str] = None
    imported_records: Optional[int] = None
    failed_records: Optional[int] = None
    error_messages: Optional[List[str]] = None
    created_at: Optional[str] = None
    
    
class ImportProgressOut(BaseModel):
    """Model dla statusu postępu importu"""
    import_id: str
    status: ImportStatus
    progress_percentage: float
    imported_records: int
    total_records: int
    current_message: Optional[str] = None 
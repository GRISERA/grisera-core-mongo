from typing import Union

from grisera import DatasetService
from grisera import DatasetOut, DatasetIn


from mongo_service import MongoApiService
from mongo_service.service_mixins import GenericMongoServiceMixin
from mongo_service.mongodb_api_config import mongo_database_name


class DatasetServiceMongoDB(DatasetService, GenericMongoServiceMixin):
    """
    Object to handle logic of datasets requests
    """

    def __init__(self):
        super().__init__()
        self.mongo_api_service = MongoApiService()
        self.model_out_class = DatasetOut

    def save_dataset(self, dataset: DatasetIn):
        return self.create(dataset, mongo_database_name)

    def get_datasets(self):
        return self.get_multiple(mongo_database_name)

    def get_dataset(self, dataset_id: Union[int, str]):
        return self.get_single(dataset_id, mongo_database_name)

    def delete_dataset(self, dataset_id: Union[int, str]):
        return self.delete(dataset_id, mongo_database_name)

    def _add_related_documents(self, participant: dict, dataset_name: str, depth: int, source: str):
        pass
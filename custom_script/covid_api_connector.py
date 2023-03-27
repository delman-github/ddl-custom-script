from typing import List
from uuid import UUID

import pandas as pd
import requests

from endpoints import check_keys
from utils.connector.base import SYNC_METHOD_BASE_CONFIG, BaseConnector


class COVIDDataConnector(BaseConnector):
    # https://data.covid19.go.id/ no longer works.
    # All data from that API are changed to be hardcoded.
    conn_config = {}
    import_config = {
        "jenis_data": {
            "title": "Data nasional atau provinsi",
            "required": True,
            "input_type": "dropdown",
            "options": {
                "display": "alias",
                "value": "name",
                "data": [
                    {"alias": "Nasional", "name": "nasional"},
                    {"alias": "Provinsi", "name": "provinsi"},
                ],
            },
        },
        "provinsi": {
            "title": "Nama Provinsi",
            "input_type": "dropdown",
            "depends_on": {"key": "jenis_data", "value": "provinsi"},
            "options": {
                "dynamic": True,
                "fetch_on": {"key": "jenis_data", "value": "provinsi"},
            },
        },
        "sync_method": SYNC_METHOD_BASE_CONFIG,
    }
    options_method_map = {
        "provinsi": "_get_provinces",
    }

    @classmethod
    def _validate_import(cls, import_params: dict, conn_params: dict, **kwargs):
        check_keys(("jenis_data", "sync_method"), import_params)
        if import_params["jenis_data"] == "provinsi":
            check_keys(("provinsi",), import_params)

    #
    @classmethod
    def import_(cls, import_params: dict, conn_params: dict, dest_table: str, **kwargs):
        if import_params["jenis_data"] == "nasional":
            df = cls._get_national_df()
        else:
            province = import_params.get("provinsi")
            df = cls._get_province_df(province)
        cls.store_df(df, dest_table, if_exists=import_params["sync_method"])

    # This function is used to preview data before importing it.
    @classmethod
    def preview(
        cls, preview_params: dict, source_id: UUID, page: int, page_size: int, **kwargs
    ) -> dict:
        if preview_params["jenis_data"] == "nasional":
            df = cls._get_national_df()
        else:
            province = preview_params.get("provinsi")
            df = cls._get_province_df(province)
        return cls.preview_df(df, page, page_size)

    # Helper Methods
    @staticmethod
    def _get_province_df(name: str) -> pd.DataFrame:
        # name = name.upper().replace(" ", "_")
        # base_url = f"https://data.covid19.go.id/public/api/prov_detail_{name}.json"
        # json = requests.get(base_url).json()
        return pd.DataFrame.from_records(
            [{"col_1": 1, "col_2": "foo"}, {"col_1": 2, "col_2": "bar"}]
        )

    @staticmethod
    def _get_national_df() -> pd.DataFrame:
        def _normalize_json_columns(df):
            for col in df.filter(regex="jumlah").columns:
                df[col] = pd.json_normalize(df[col])
            return df

        base_url = "https://data.covid19.go.id/public/api/update.json"
        json = requests.get(base_url).json()
        return pd.DataFrame(json).pipe(_normalize_json_columns)

    @staticmethod
    def _get_provinces(conn_params: dict) -> List[str]:
        # base_url = "https://data.covid19.go.id/public/api/prov.json"
        # json = requests.get(base_url).json()
        return {
            "data": ["Foo Province", "Bar Province"],
            "message": "Provinces retrieved",
        }

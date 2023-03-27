# utils/connector/file.py

import re
from datetime import datetime, timedelta
from typing import List, Tuple

from dateutil.relativedelta import relativedelta

import utils.csv as csv
import utils.excel as excel
import utils.json as json
import utils.parquet as parquet
from utils.connector.base import BaseConnector
from utils.exceptions import APIError

MACRO_MAP = {
    "{{ * }}": (None, 10000),
    "{{ month }}": (lambda x: x.strftime("%B"), 100, "months"),
    "{{ today }}": (lambda x: x.strftime("%Y%m%d"), 10, "days"),
    "{{ YYYY }}": (lambda x: x.strftime("%Y"), 1000, "years"),
    "{{ MM }}": (lambda x: x.strftime("%m"), 100, "months"),
    "{{ DD }}": (lambda x: x.strftime("%d"), 10, "days"),
}


class FileConnector(BaseConnector):
    @classmethod
    def _validate_filename(cls, filename: str) -> Tuple[str, List[str]]:
        # check whether there are templated section in the filename
        formats = re.findall(r"({{[^`]*?}})", filename)
        if formats:
            # check for invalid formats
            invalid_formats = set(formats) - set(MACRO_MAP)
            if invalid_formats:
                raise APIError(
                    "Invalid format filename: {}".format(", ".join(invalid_formats))
                )

        return filename, formats

    @classmethod
    def _format_macro(cls, filename: str) -> str:
        """Format filename based on macro.

        Current supported macro(s):
            - {{ today }}: convert to today date (Now)
            - {{ month }}: convert to this month
            - {{ YYYY }} : change to current year (1971 - Now)
            - {{ MM }} : change to current month (01 - 12)
            - {{ DD }} : change to current date (01 - 31)
        """
        filename, formats = cls._validate_filename(filename)
        if not formats:
            return filename

        # get format with the minimum period (e.g. YEAR > MONTH > DAY)
        minimum_format = min(formats, key=lambda x: MACRO_MAP[x][1])
        diff_period = {
            MACRO_MAP[minimum_format][2]: 1
            if minimum_format not in ("{{ today }}", "{{ month }}")
            else 0
        }

        # set timezone as GMT+7
        final_dt = datetime.today() + timedelta(hours=7) - relativedelta(**diff_period)
        for k, v in MACRO_MAP.items():
            if k != "{{ * }}":
                filename = filename.replace(k, v[0](final_dt))
        return filename

    @classmethod
    def filter_filenames_by_pattern(cls, filenames: List[str], pattern: str):
        inputed_filename = pattern.replace("{{ * }}", ".*")
        return [f for f in filenames if re.search(rf"^{inputed_filename}", f)]

    @classmethod
    def _validate_sheets_all_files(cls, sheets_all_files: List, filename: str):
        compared_sheets_list = sheets_all_files[0]
        for sheets in sheets_all_files[1:]:
            sheet_found = set(compared_sheets_list) & set(sheets)
            if not sheet_found:
                raise APIError(
                    f"All the excel files with the format '{filename}'"
                    + " must have the same sheet name"
                )
            compared_sheets_list = sheets

    GET_FILE_COLS_FUNCS = {
        "csv": csv.get_columns,
        "plaintext": csv.get_columns,
        "excel": excel.get_columns,
        "parquet": parquet.get_columns,
        "json": json.get_columns,
    }

    @classmethod
    def _get_next_file_schema(cls, file_paths: List, file_type: str, **kwargs) -> List:
        get_cols_f = cls.GET_FILE_COLS_FUNCS.get(file_type)
        for f_path in file_paths:
            yield list(get_cols_f(f_path, **kwargs))

    @classmethod
    def _validate_schema_all_files(
        cls, file_paths: List, filename: str, file_type: str, **kwargs
    ):
        """Validate schema multiple import file"""
        base_schema = None
        for schema in cls._get_next_file_schema(file_paths, file_type, **kwargs):
            if not base_schema:
                base_schema = schema
            elif base_schema != schema:
                raise APIError(
                    f"All the files with the format '{filename}'"
                    + " must have identical columns and they must be appear in the same order"
                )

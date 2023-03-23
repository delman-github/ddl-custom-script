# This is a sample script for combining data

from time import sleep

import pandas as pd
from analytics.combine.base import Combiner
from app import current_ds


class JoinNodesCombiner(Combiner):
    """This combiner requires two tables and does the following procedures:
    - join between 2 nodes (data_A and data_B) by specified column identifier
    """

    """
    tables is a variable that is useful for indicating which dataset will be used in this combiner. 
    In the example below, two datasets are required to be used in this combiner.
    """
    tables = [{"name": "data_A"}, {"name": "data_B"}]
    params = {
        "match_column_A": {
            "description": "match column on the data_A",
            "default": "id",
        },
        "match_column_B": {
            "description": "match column on the data_B",
            "default": "id",
        },
    }

    def run(self):
        sleep(70)
        # T0 -> represent data_A
        # T1 -> represent data_B
        T0, T1 = self.SOURCES
        # create a new table whose content is all columns of A after joining
        # NOTE: you can customize the query below, just adjust with your requirements
        df = pd.read_sql(
            " ".join(
                [
                    "SELECT A.*",
                    'FROM "{}" AS A'.format(T0),
                    'JOIN "{}" AS B'.format(T1),
                    '   ON A."{}" = B."{}"'.format(
                        self["match_column_A"], self["match_column_B"]
                    ),
                ]
            ),
            current_ds.engine,
        )

        # insert to sql
        df.to_sql(self.RESULT, current_ds.engine, index=False)

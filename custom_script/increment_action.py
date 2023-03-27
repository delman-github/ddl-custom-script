from time import sleep

import utils.db as db

params = {
    "field": {"required": True, "input_type": "dropdown", "init": "populate_columns"},
    "inc": {"default": 1},
    "delay": {"default": 0.1},
}


def run(self):
    field = self["field"]
    inc = int(self["inc"])

    sleep(float(self["delay"]))
    db.copy_data_from_db(self.RESULT, source_table=self.SOURCE)
    self.q(f'UPDATE "{self.RESULT}" SET "{field}" = "{field}" + {inc}')

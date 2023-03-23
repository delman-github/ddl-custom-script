# analytics/action/increment.py

from time import sleep
from copy import deepcopy

from sqlalchemy import select

import utils.db as db

# Inherit "Action" class on you custom class
from .base import Action


class IncrementAction(Action):
    """
    "Params", commonly referred to as configuration parameters,
    are variables that will be displayed on the UI to store the parameters needed for the custom action you will create.
    You can think of params as a parameter in a function that will be used in that function.

    The contents of the params variable are a dictionary that contains "key" and "value". For example:
    {
    "params_name":{...configuration}
    }.
    For instance, in the example below, there are 3 params used in this Action Example, namely:
    "fields", "inc" and "delay"

    The value of each key contains a configuration for each key or parameter, for example, "fields" is required, "inc" has a default value of 1, and so on.

    Here are some basic keys that you can use to configure your parameters:
    # - "type": Python type of parameter's value
    # - "default": if given, then use this as default value
    # - "title": will be rendered in front end as params title
    # - "description": description of this parameter (default: "")
    # - "required": if True, mark this parameter as required (default: False)i
    # - "input_type": this value tells the frontend what kind of input should be rendered, for example: text, number, textarea, dropdown, file etc.
    In addition to the basic keys, there are also advanced keys for parameter configuration. Here are some examples:
        If you use input type: dropdown, you can add:
         - "init": if given, init will generate data to be displayed on your dropdown parameter, there are 3 types for the value of this:
            "populate_columns": to populate all columns in your dataset
            "populate_json_columns": to populate all columns that have a json type in your dataset
            "populate_datetime_columns": to populate all columns that have a datetime type in your dataset

         - "options": used to replace the "init" key for generating data, this key contains a dictionary that must have the following 3 keys:
            "display": from the data, what is displayed on the front end
            "value": the value to be taken from the data
            "data": the data to be used for this parameter, which contains a list of dictionaries.
    """

    params = {
        "field": {
            "required": True,
            "input_type": "dropdown",
            "init": "populate_columns",
        },
        "inc": {"default": 1},
        "delay": {"default": 0.3},
        "insert_type": {"default": 0.3},
    }

    # this just for the title of this action on UI
    ui_name = "Increment"

    # This function is the main function in the Action class,
    # and it is responsible for transforming data when you perform the action.
    def run(self):
        field = self["field"]
        inc = int(self["inc"])

        # self.stop() is a function that is useful for stopping your action and providing an error message.
        if inc < 1:
            self.stop("Invalid increment value")

        # remove selected fields based ignored fields on config.
        source_table = db.get_table(self.SOURCE)
        stmt = select(
            [source_table.c[col["name"]] for col in db.get_columns(self.SOURCE)]
        )

        sleep(float(self["delay"]))

        # you can use this function to log anything on your action
        # you can see the log result on your monitoring page
        self.log(source_table)

        """
        self.q is used to execute a query. In the following query, we create a table with the name self.RESULT. What is self.RESULT?

        self.RESULT is the result table that will become new data in our dataset.
        """
        self.q(
            (
                f'CREATE TABLE "{self.RESULT}"',
                f"  AS {stmt};",
                f'UPDATE "{self.RESULT}"',
                f'   SET "{field}" = "{field}" + {inc}',
            )
        )
        """
        actually, there is another way besides using self.q, which is by using a Pandas dataframe with the function self.store_df(dataframe).
        But the question is, how can I get the old data in the form of a dataframe? We also provide a function called self.DF(query, **kwargs)
        that is useful for getting old data in the form of a dataframe. 
        
        For example, this is how to use self.DF and self.store_df:

        df = self.DF()
        df[field] = df[field] + inc
        self.store_df(df)
        """

    # you can modify how you store your params here
    # this function is optional
    def process_params_to_store(params: dict, node_id) -> dict:
        return {**params, "node_name": str(node_id)}

    # you can modify how you show your params on UI here
    # this function is also optional
    def read_params(params: dict) -> dict:
        displayed_params = deepcopy(params)
        displayed_params.update(
            {
                "node_name": db.get_att(
                    "name", "project_nodes", {"id": displayed_params["node_name"]}
                )
            }
        )
        return displayed_params

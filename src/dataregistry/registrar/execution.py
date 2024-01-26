from datetime import datetime

from dataregistry.db_basic import add_table_row

from .base_table_class import BaseTable
from .registrar_util import _read_configuration_file


class ExecutionTable(BaseTable):
    def __init__(self, db_connection, root_dir, owner, owner_type):
        super().__init__(db_connection, root_dir, owner, owner_type)

        self.which_table = "execution"

    def create(
        self,
        name,
        description=None,
        execution_start=None,
        locale=None,
        configuration=None,
        input_datasets=[],
        input_production_datasets=[],
        max_config_length=None,
    ):
        """
        Create a new execution entry in the DESC data registry.

        Any args marked with '**' share their name with the associated column
        in the registry schema. Descriptions of what these columns are can be
        found in `schema.yaml` or the documentation.

        Parameters
        ----------
        name** : str
        description** : str, optional
        execution_start** : datetime, optional
        locale** : str, optional
        configuration** : str, optional
        input_datasets** : list, optional
        input_production_datasets** : list, optional
        max_config_length : int, optional
            Maxiumum number of lines to read from a configuration file

        Returns
        -------
        my_id : int
            The execution ID of the new row relating to this entry
        """

        # Set max configuration file length
        if max_config_length is None:
            max_config_length = self._DEFAULT_MAX_CONFIG

        # Put the execution information together
        values = {"name": name}
        if locale:
            values["locale"] = locale
        if execution_start:
            values["execution_start"] = execution_start
        if description:
            values["description"] = description
        values["register_date"] = datetime.now()
        values["creator_uid"] = self._uid

        exec_table = self._get_table_metadata("execution")
        dependency_table = self._get_table_metadata("dependency")

        # Read configuration file. Enter contents as a raw string.
        if configuration:
            values["configuration"] = _read_configuration_file(
                configuration, max_config_length
            )

        # Enter row into data registry database
        with self._engine.connect() as conn:
            my_id = add_table_row(conn, exec_table, values, commit=False)

            # handle dependencies
            for d in input_datasets:
                values["register_date"] = datetime.now()
                values["input_id"] = d
                values["execution_id"] = my_id
                add_table_row(conn, dependency_table, values, commit=False)

            # handle production dependencies
            for d in input_production_datasets:
                values["register_date"] = datetime.now()
                values["input_production_id"] = d
                values["execution_id"] = my_id
                add_table_row(conn, dependency_table, values, commit=False)

            conn.commit()
        return my_id

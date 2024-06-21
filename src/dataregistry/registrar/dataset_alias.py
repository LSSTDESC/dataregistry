from datetime import datetime

from dataregistry.db_basic import add_table_row
from sqlalchemy import update, select
from .registrar_util import _read_configuration_file

from .base_table_class import BaseTable


class DatasetAliasTable(BaseTable):
    def __init__(self, db_connection, root_dir, owner, owner_type):
        super().__init__(db_connection, root_dir, owner, owner_type)

        self.which_table = "dataset_alias"
        self.entry_id = "dataset_alias_id"

    def register(self, aliasname, dataset_id, ref_alias_id=None,
                 access_api=None, access_api_configuration=None,
                 supersede=False):
        """
        Create a new `dataset_alias` entry in the DESC data registry.
        It may refer to a dataset (default) or another alias

        Any args marked with '**' share their name with the associated column
        in the registry schema. Descriptions of what these columns are can be
        found in `schema.yaml` or the documentation.

        Parameters
        ----------
        aliasname                  : str  alias name
        dataset_id**               : int  not None if alias refers to dataset
        ref_alias_id**             : int  not None if alias refers to
                                          another alias
        access_api**               : str  api, if any, which can read the
                                          dataset
        access_api_configuration** : str  extra information for access_api
        supersede                  : bool if True, create a new entry with
                                          this alias name even if old ones
                                          exist

        Returns
        -------
        prim_key : int
            The dataset_alias ID of the new row relating to this entry
        """

        if not dataset_id and not ref_alias_id:
            raise ValueError("""DatasetAliasTable.register: one of dataset_id,
                                ref_alias_id must have a value""")

        now = datetime.now()
        values = {"alias": aliasname}
        if dataset_id:
            values["dataset_id"] = dataset_id
        else:
            values["ref_alias_id"] = ref_alias_id
        if access_api:
            values["access_api"] = access_api
        if access_api_configuration:
            values["access_api_configuration"] = _read_configuration_file(
                access_api_configuration, None
            )
        values["register_date"] = now
        values["creator_uid"] = self._uid

        alias_table = self._get_table_metadata("dataset_alias")

        # If not supersede, check if alias name has already been used
        with self._engine.connect() as conn:
            if not supersede:
                q = select(alias_table.c.alias).where(
                    alias_table.c.alias == aliasname)
                result = conn.execute(q)
                if result.fetchone():
                    print(f"Alias {aliasname} already exists. Specify 'supersede=True' to override")
                    return None
            prim_key = add_table_row(conn, alias_table, values)

            # Update any other alias rows which have been superseded
            stmt = (
                update(alias_table)
                .where(
                    alias_table.c.alias == aliasname,
                    alias_table.c.dataset_alias_id != prim_key,
                )
                .values(supersede_date=now)
            )
            conn.execute(stmt)
            conn.commit()
        return prim_key

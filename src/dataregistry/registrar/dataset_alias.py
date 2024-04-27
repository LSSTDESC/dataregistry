from datetime import datetime

from dataregistry.db_basic import add_table_row
from sqlalchemy import update

from .base_table_class import BaseTable


class DatasetAliasTable(BaseTable):
    def __init__(self, db_connection, root_dir, owner, owner_type):
        super().__init__(db_connection, root_dir, owner, owner_type)

        self.which_table = "dataset_alias"
        self.entry_id = "dataset_alias_id"

    def register(self, aliasname, dataset_id):
        """
        Create a new `dataset_alias` entry in the DESC data registry.

        Any args marked with '**' share their name with the associated column
        in the registry schema. Descriptions of what these columns are can be
        found in `schema.yaml` or the documentation.

        Parameters
        ----------
        aliasname** : str
        dataset_id** : int

        Returns
        -------
        prim_key : int
            The dataset_alias ID of the new row relating to this entry
        """

        now = datetime.now()
        values = {"alias": aliasname}
        values["dataset_id"] = dataset_id
        values["register_date"] = now
        values["creator_uid"] = self._uid

        alias_table = self._get_table_metadata("dataset_alias")
        with self._engine.connect() as conn:
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

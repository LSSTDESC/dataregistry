from datetime import datetime

from dataregistry.db_basic import add_table_row
from sqlalchemy import update

# Default maximum allowed length of configuration file allowed to be ingested
_DEFAULT_MAX_CONFIG = 10000


class RegistrarDatasetAlias:
    def __init__(self, parent):
        """
        Wrapper class to register/modify/delete execution entries.

        Parameters
        ----------
        parent : Registrar class
            Contains db_connection, engine, etc
        """

        self.parent = parent

    def create(self, aliasname, dataset_id):
        """
        Register a new dataset alias in the DESC data registry.

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
        values["creator_uid"] = self.parent._uid

        alias_table = self.parent._get_table_metadata("dataset_alias")
        with self.parent._engine.connect() as conn:
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

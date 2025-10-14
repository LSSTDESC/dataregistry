import os
from datetime import datetime
from typing import Literal

from sqlalchemy import select

from dataregistry.db_basic import add_table_row
from dataregistry.registrar.base_table_class import BaseTable


class KeywordsTable(BaseTable):

    """
    The Keywords class is used to manage keywords in the data registry.
    It provides methods to add, remove, and list keywords associated with datasets.

    Parameters
    ----------
    db_connection : DbConnection object
        Encompasses sqlalchemy engine, dialect (database backend)
        and schema version
    owner : str
        To set the default owner for all registered keywords in this
        instance.
    """
    def __init__(self, db_connection, root_dir, owner, owner_type):
        super().__init__(db_connection, root_dir, owner, owner_type)
        self.which_table = "keyword"
        self.entry_id = "keyword"

    def add_keyword(
            self,
            keyword: str,
            user_type: Literal["user", "group", "project"] = "user",
            system: bool = False):
        """
        Add a keyword to a dataset.

        Parameters
        ----------
        keyword : str
            The keyword to add.
        """
        owner =  self._owner or os.getenv("USER")
        if not isinstance(keyword, str):
            raise ValueError(f"Keyword {keyword} is not a valid keyword string.")
        kwargs_dict = {"keyword": keyword.lower(),
                       "creator_uid": owner,
                       "system": system,
                       "active": True,
                       "creation_date": datetime.now()}
        # Implementation for adding a keyword to the dataset in the database
        keywords_table = self._get_table_metadata("keyword")
        with self._engine.connect() as conn:
            add_table_row(conn, keywords_table, kwargs_dict, commit=True)

    def disable_keyword(self, keyword: str):
        """
        Disable a keyword in the registry.

        Parameters
        ----------
        keyword : str
            The keyword to disable.
        """
        self._set_enable_keyword(keyword, enable=False)

    def enable_keyword(self, keyword: str):
        """
        Enable a keyword in the registry.

        Parameters
        ----------
        keyword : str
            The keyword to enable.
        """
        self._set_enable_keyword(keyword, enable=True)

    def _set_enable_keyword(self, keyword: str, enable: bool = True):
        """
        Enable a keyword in the registry.

        Parameters
        ----------
        keyword : str
            The keyword to enable.
        """
        keywords_table = self._get_table_metadata("keyword")
        stmt = select(keywords_table.c.creator_uid).where(
                      keywords_table.c.keyword == keyword)
        with self._engine.connect() as conn:
            result = conn.execute(stmt).fetchone()
            if result is None:
                raise ValueError(f"Keyword {keyword} does not exist in the registry.")
            owner: str = result[0]
            if owner != self._owner:
                raise ValueError(f"Keyword {keyword} is owned by another user.")
        modify_fields = {"active": enable}
        self.modify(keyword, modify_fields)

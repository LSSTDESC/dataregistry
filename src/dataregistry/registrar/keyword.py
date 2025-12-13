import os
from datetime import datetime
from typing import Literal

from sqlalchemy import select

from dataregistry.db_basic import add_table_row
from dataregistry.registrar.base_table_class import BaseTable
from dataregistry.db_basic import DbConnection


class KeywordTable(BaseTable):

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
    def __init__(
            self,
            db_connection: DbConnection,
            root_dir,
            owner,
            owner_type
    ) -> None:
        super().__init__(db_connection, root_dir, owner, owner_type)
        self.which_table = "keyword"
        self.entry_id = "keyword_id"

    def create_keywords(
            self,
            keywords: list[str],
            user_type: Literal["user", "group", "project", "production"] = "user",
            system: bool = False,
            commit: bool = True
    ) -> None:
        """
        Add multiple keywords to the registry.

        Parameters
        ----------
        keywords : list[str]
            The keywords to add.
        user_type : string.
            A known user type. Not currently used for anything
        system : bool.
            system keywords can only be created by a privileged user. They
            are intended to be global
        """
        owner = self._owner or os.getenv("USER")
        keywords_table = self._get_table_metadata("keyword")
        with self._engine.connect() as conn:
            for keyword in keywords:
                if not isinstance(keyword, str):
                    raise ValueError(f"Keyword {keyword} is not a valid keyword string.")
                kwargs_dict = {"keyword": keyword.lower(),
                               "creator_uid": owner,
                               "system": system,
                               "active": True,
                               "creation_date": datetime.now()}
                add_table_row(conn, keywords_table, kwargs_dict, commit=False)
            if commit:
                conn.commit()

    def disable_keyword(self, keyword: str):
        """
        Disable a keyword from the registry.

        keyword must exist and must be owned by the current user.

        Parameters
        ----------
        keyword : str
            The keyword to disable.
        """
        self._set_enable_keyword(keyword, enable=False)

    def enable_keyword(self, keyword: str) -> None:
        """
        Enable a keyword in the registry.

        Keyword must exist and must be owned by the current user.

        Parameters
        ----------
        keyword : str
            The keyword to enable.
        """
        self._set_enable_keyword(keyword, enable=True)

    def get_keywords_from_dataset(
            self,
            dataset_id: int
    ) -> list[str]:
        """
        Get the list of keywords associated with a dataset.

        Parameters
        ----------
        dataset_id : int

        Returns
        -------
        keywords : list[str]
        """

        keywords = []

        # Link to the dataset-keyword association table
        dataset_keyword_table = self._get_table_metadata("dataset_keyword")
        keyword_table = self._get_table_metadata("keyword")

        stmt = (
            select(keyword_table.c.keyword)
            .select_from(
                dataset_keyword_table.join(
                    keyword_table,
                    dataset_keyword_table.c.keyword_id == keyword_table.c.keyword_id,
                )
            )
            .where(dataset_keyword_table.c.dataset_id == dataset_id)
        )

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

        for r in result:
            keywords.append(r.keyword)

        return keywords

    def add_keywords_to_dataset(
            self,
            dataset_id: int,
            keywords: list[str]
    ) -> None:
        """
        Add/append keywords to an already existing dataset.

        First check the keywords are valid, then append. If the dataset already
        has one or more of the passed keywords attributed to it, the keyword(s)
        will not be duplicated.

        Parameters
        ----------
        dataset_id : int
        keywords : list[str]
        """

        # Make sure things are valid
        if not isinstance(keywords, list):
            raise ValueError("Passed keywords object must be a list")

        if len(keywords) == 0:
            return

        # Validate (make sure keywords are in the `keyword` table & active)
        keyword_ids = self.validate_keywords(keywords)

        # Link fo the dataset-keyword association table
        dataset_keyword_table = self._get_table_metadata("dataset_keyword")

        with self._engine.connect() as conn:
            # Loop over each keyword in the list
            for keyword_id in keyword_ids:
                # Check if this dataset already has this keyword
                stmt = (
                    select(dataset_keyword_table)
                    .where(dataset_keyword_table.c.dataset_id == dataset_id)
                    .where(dataset_keyword_table.c.keyword_id == keyword_id)
                )

                result = conn.execute(stmt)
                rows = result.fetchall()

                # If we don't have the keyword, add it
                if len(rows) == 0:
                    add_table_row(
                        conn,
                        dataset_keyword_table,
                        {"dataset_id": dataset_id, "keyword_id": keyword_id},
                        commit=False,
                    )
            conn.commit()

    def remove_keywords_from_dataset(
            self,
            dataset_id: int,
            keywords: list[str]
    ) -> None:
        """
        Remove keywords from a dataset.

        Parameters
        ----------
        dataset_id : int
        keywords : list[str]
        """

        raise NotImplementedError()

    def _set_enable_keyword(self, keyword: str, enable: bool = True):
        keywords_table = self._get_table_metadata("keyword")
        stmt = select(keywords_table.c.keyword_id,
                      keywords_table.c.active).where(
                      keywords_table.c.keyword == keyword)
        with self._engine.connect() as conn:
            result = conn.execute(stmt).fetchone()
            if result is None:
                raise ValueError(f"Keyword {keyword} does not exist in the registry.")
            if result.active == enable:   # nothing to do
                return
        modify_fields = {"active": enable}
        self._modify(modify_fields, result.keyword_id)

    def validate_keywords(self, keywords: list) -> list[int]:
        """
        Validate a list of keywords.

            - Ensure they are strings
            - Ensure the chosen keywords are registered in the keywords table

        If any keyword is invalid an exception is raised.

        Parameters
        ----------
        keywords : list[str]

        Returns
        -------
        keyword_ids : list[int]
            The associated `keyword_id`s from the `keyword` table
        """

        keyword_ids = []

        for k in keywords:
            # Make sure keyword is a string
            if not isinstance(k, str):
                raise ValueError(f"{k} is not a valid keyword string")

        # Make sure keywords are all in the keywords table
        keyword_table = self._get_table_metadata("keyword")

        stmt = select(keyword_table.c.keyword_id,
                      keyword_table.c.active).where(
            keyword_table.c.keyword.in_([x.lower() for x in keywords])
        )

        with self._engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()

        # See if keyword not found
        rows = result.fetchall()   # sqlite doesn't support rowcount
        if len(rows) < len(keywords):
            raise ValueError("Not all keywords selected are registered")

        # Keyword found
        for r in rows:
            if r.active:
                keyword_ids.append(r.keyword_id)

        return keyword_ids

import os

from dataregistry.db_basic import SCHEMA_VERSION, create_db_engine
from dataregistry.query import Filter, Query
from dataregistry.registrar import Registrar


class DREGS:

    def __init__(self, config_file=None, schema_version=None, root_dir=None, verbose=False):

        # If no schema specified, go to the default one
        if schema_version is None:
            self.schema_version = SCHEMA_VERSION
        else:
            self.schema_version = schema_version

        # Establish connection to database
        engine, dialect = create_db_engine(config_file=config_file, verbose=verbose)

        # Create query object
        self.Query = Query(engine, dialect, schema_version=self.schema_version)

        # Create registrar object
        self.Registrar = Registrar(engine, dialect, schema_version=self.schema_version, root_dir=root_dir) 

    def gen_filter(self, property_name, bin_op, value):
        """
        Generate a binary filter for a DREGS query.

        These construct SQL WHERE clauses.

        Parameters
        ----------
        property_name : str
            Database property to be queried on
        bin_op : str
            Binary operation to perform, e.g., "==" or ">="
        value : -
            Comparison value

        Example usage
        -------------
        f = gen_filter("dataset.name", "==", "my_dataset")
        f = gen_filter("dataset.version_major", ">", 1)

        Returns
        -------
        - : namedtuple
            The filter tuple
        """
        
        return Filter(property_name, bin_op, value)

if __name__ == "__main__":
    dregs = DREGS()

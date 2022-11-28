import os
from dataregistry.registrar import Registrar
from dataregistry.db_basic import create_db_engine, ownertypeenum

engine, dialect = create_db_engine(config_file=os.path.join(os.getenv('HOME'),
                                                            '.config_reg_writer'))

registrar = Registrar(engine, dialect, ownertypeenum.user, owner='jrbogart')

new_id = registrar.register_execution('my_program', 'imaginary program',
                                      locale='NERSC')
print(f'Created execution entry with id {new_id}')

new_id = registrar.register_dataset('my_favorite_dataset',
                                    'some_subdir/no_such_dataset.parquet',
                                    1,0,0,version_suffix='junk',
                                    description='Non-existent dataset')

print(f'Created execution entry with id {new_id}')

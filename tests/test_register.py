import os
import sys
from dataregistry.registrar import Registrar
from dataregistry.db_basic import create_db_engine, ownertypeenum, SCHEMA_VERSION

engine, dialect = create_db_engine(config_file=os.path.join(os.getenv('HOME'),
                                                            '.config_reg_writer'))
if len(sys.argv) > 1:
    schema = sys.argv[1]
else:
    schema = SCHEMA_VERSION

registrar = Registrar(engine, dialect, ownertypeenum.user, owner='jrbogart',
                      schema_version=schema)


new_id = registrar.register_execution('my_execution1', 'imaginary execution 1',
                                      locale='NERSC')
if new_id:
    print(f'Created execution entry with id {new_id}')
else:
    print(f'Failed to create execution entry')

new_id = registrar.register_execution('my_execution2', 'imaginary execution 2',
                                      locale='NERSC')
if new_id:
    print(f'Created execution entry with id {new_id}')
else:
    print(f'Failed to create execution entry')

config_path = os.path.join(os.getenv('HOME'), 'desc_git/dataregistry/tests',
                           'a_config.yaml')
new_id = registrar.register_execution('my_execution3', 'imaginary execution 3',
                                      locale='NERSC',
                                      configuration=config_path)
if new_id:
    print(f'Created execution entry with id {new_id}')
else:
    print(f'Failed to create execution entry')

new_id = registrar.register_execution('my_execution4', 'imaginary execution 4',
                                      locale='NERSC')
if new_id:
    print(f'Created execution entry with id {new_id}')
else:
    print(f'Failed to create execution entry')

new_id = registrar.register_dataset('some_subdir/no_such_dataset1.parquet',
                                    '1.1.0',version_suffix='junk',
                                    name='my_favorite_dataset',
                                    description='Non-existent dataset',
                                    is_overwritable=True, is_dummy=True)
if new_id:
    print(f'Created dataset entry with id {new_id}')
else:
    print(f'Failed to create dataset entry')

new_id = registrar.register_dataset('some_subdir/my_favorite_dataset.parquet',
                                    '1.1.0',version_suffix='junk',
                                    description='Non-existent dataset',
                                    is_overwritable=True, is_dummy=True)
if new_id:
    print(f'Created dataset entry with id {new_id}')
else:
    print(f'Failed to create dataset entry')


new_id = registrar.register_dataset('some_subdir/no_such_dataset3.parquet',
                                    'patch',version_suffix='junk',
                                    name='my_favorite_dataset',
                                    execution_id=2,
                                    description='Non-existent dataset',
                                    is_overwritable=True, is_dummy=True)
if new_id:
    print(f'Created dataset entry with id {new_id}')
else:
    print(f'Failed to create dataset entry')


new_id = registrar.register_dataset('some_subdir/no_such_dataset3.parquet',
                                    'minor',version_suffix='junk',
                                    name='my_favorite_dataset',
                                    execution_id=3,
                                    description='Non-existent dataset',
                                    is_overwritable=True, is_dummy=True)
if new_id:
    print(f'Created dataset entry with id {new_id}')
else:
    print(f'Failed to create dataset entry')


new_id = registrar.register_dataset('some_subdir/no_such_dataset3.parquet',
                                    'major',version_suffix='junk',
                                    name='my_favorite_dataset',
                                    execution_id=4,
                                    description='Non-existent dataset',
                                    is_overwritable=True, is_dummy=True)
if new_id:
    print(f'Created dataset entry with id {new_id}')
else:
    print(f'Failed to create dataset entry')

new_id = registrar.register_dataset('some_subdir/another_nondataset4.parquet',
                                    'major',version_suffix='junk',
                                    name='another_favorite_dataset',
                                    execution_id=4,
                                    description='Non-existent dataset',
                                    is_overwritable=False, is_dummy=True)
if new_id:
    print(f'Created dataset entry with id {new_id}')
else:
    print(f'Failed to create dataset entry')

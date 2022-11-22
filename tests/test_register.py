import os
from dataregistry.registrar import Registrar
from dataregistry.db_basic import create_db_engine, OwnershipEnum

engine = create_db_engine(config_file=os.path.join(os.getenv('HOME'),
                                                   '.config_reg_writer'))

registrar = Registrar(engine, OwnershipEnum.user, owner='jrbogart')

new_id = registrar.register_execution('my_program', 'imaginary program',
                                      locale='NERSC')

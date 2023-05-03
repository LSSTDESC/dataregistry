import os
import sys
import argparse
from dataregistry.registrar import Registrar
from dataregistry.db_basic import create_db_engine, ownertypeenum, SCHEMA_VERSION

_lookup = {"production" : ownertypeenum.production,
           "group" : ownertypeenum.group, "user" : ownertypeenum.user}

def make_entry(args):
    engine, dialect = create_db_engine(config_file=os.path.join(os.getenv('HOME'),
                                                            '.config_reg_writer'))
    if args.schema_version:
        schema = args.schema_version
    else:
        schema = SCHEMA_VERSION

    owner = args.owner
    if args.owner_type == 'production':
        owner = 'production'

    if not owner:
        owner = os.getenv('USER')
    registrar = Registrar(engine, dialect, _lookup[args.owner_type],
                          owner=owner, schema_version=schema)

    #new_id = registrar.register_execution('my_program', 'imaginary program',
    #                                      locale='NERSC')
    #print(f'Created execution entry with id {new_id}')

    v = args.version.split('.')
    suffix = None
    if len(v) > 3 and owner_type != 'production':
        suffix = ''.join(v[3:])
    new_id = registrar.register_dataset(args.name, args.relpath,
                                        v[0], v[1], v[2],
                                        version_suffix=suffix,
                                        creation_date=args.creation_date,
                                        description=args.description,
                                        old_location=args.old_location,
                                        copy=(not args.make_sym_link))

    # new_id = registrar.register_dataset('my_favorite_dataset',
    #                                     'some_subdir/no_such_dataset.parquet',
    #                                     1,0,0,version_suffix='junk',
    #                                     description='Non-existent dataset',
    #                                     is_overwritable=True)

    print(f'Created dataset entry with id {new_id}')

parser = argparse.ArgumentParser(description='''Register datasets with dataregistry''',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('name', help='A name for the dataset')
parser.add_argument('relpath', help='''destination for dataset relative
  to <registry root>/<owner_type>/<owner>''')
parser.add_argument('version', help='''Semantic version string of form m.n.p
                 or m.n.p.stuff where m n p are nonneg. ints,
                 stuff is an arbitrary string''')
parser.add_argument('--owner-type', choices=['production', 'group', 'user'],
                    default='user')
parser.add_argument('--owner', default=None,
                    help='defaults to current user for owner type user')
parser.add_argument('--locale', default='NERSC')
parser.add_argument('--is-overwritable', default=None,
                    help='Default is True for user and group. Must be False for production')
parser.add_argument('--creation_date', default=None)
parser.add_argument('--description', default=None)
parser.add_argument('--old-location', default=None,
                    help='if provided, dataset will be copied from here')
parser.add_argument('--make-sym-link', action='store_true',
                    help='''Make sym link at relpath rather than
                            (default behavior) copy file from old location
                            Ignored if old-location not specified.''')
parser.add_argument('--schema-version', default=None,
                    help='By default use newest available schema version')

args = parser.parse_args()

make_entry(args)

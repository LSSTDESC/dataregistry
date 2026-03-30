import argparse
from dataregistry.globus.authorizer import Authorizer


parser = argparse.ArgumentParser(
    description="Authorize with globus",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--force", action="store_true",
    help="if set (True), force reacquisition of tokens",
)

args = parser.parse_args()

authorizer = Authorizer()
authorizer.authorize(force=args.force)

print("all done")

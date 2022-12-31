import argparse

import subprocess

import utils as utils

from sync import *


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def is_path(path):
    if not Path(path).exists():
        Path(path).mkdir(parents=True, exist_ok=True)
    return Path(path)


def argparser():
    try:

        parser = argparse.ArgumentParser(
            prog="syncdir",
            description="Test sync project by Spyros", add_help=False)

        parser.add_argument(
            '-s', '--source',
            help='Source folder path:  <string>',
            type=is_path,
            default="source",
        )

        parser.add_argument(
            '-r', '--replica',
            help='Destination folder path:  <string>',
            type=is_path,
            default="replica",
        )

        parser.add_argument(
            '-l', '--log',
            help='Log file path:  <string>',
            type=is_path,
            default="log",
        )
        parser.add_argument(
            '-i', '--interval',
            help="Sync interval in seconds : <integer>")

        parser.add_argument(
            '-d', '--defaults', action="store_false",
            default=False,
            help='Enable default settings.')

        parser.print_help()
        args, _ = parser.parse_known_args()
        args = args.__dict__
        return args
    except Exception as e:
        print(e)
        exit()


## few rather generic  constants
ARGS = argparser()


class Enviroment():
    def __init__(self, ):
        self.Sync = self.sync()


    def sync(self, ):
        return Sync(
            source_path=ARGS.get("source", None),
            replica_path=ARGS.get("replica", None),
            interval=ARGS.get("interval", 600),
            log_path=ARGS.get("log", None),
            context=self,
        )


utils = utils.Utils()
main = Enviroment().sync()
a = main._copy_intersection()
# l.set_source(utils.directory_path(title="source"))
# l.set_replica(utils.directory_path(title="replica"))

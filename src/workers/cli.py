# -*- coding: utf-8 -*-
"""
"""

import sys
import os.path
import argparse
import re

class ChangeWorker(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        namespace.worker = 'dsp'
        setattr(namespace, self.dest, values)

def create_parser():
    parser = argparse.ArgumentParser(description="""
    CLI to control workers execution. It is also useful for initializing
    the database and serving a web app where it is possible to have a
    report.
    """)
    parser.add_argument("action", type=str,
        help="""It might be 'init for initialize the database or 'serve' for
            serving the web app. The default is work, which is about puting
            a worker to run its task.""",
        default="work", nargs='?', const=1)
    parser.add_argument("--worker", "-w", type=str,
        help="The worker to execute")
    parser.add_argument("--dsp", "-d", type=str,
        help="The specific DSP worker to execute", action=ChangeWorker)
    parser.add_argument("--generate-report", "-g",
        help="Generate report", required=False, default=False,
        action='store_true')
    parser.add_argument("--port", "-p", type=int,
        help="The port for serve the web app", default=80, required=False)
    return parser

def manager(args):
    """this function will execute the proper commands according to the
    arguments

    Params
    ------
    args : array_like
        a list of arguments
    
    """
    if args.action == "init":
        print("Initializing...")
    elif args.action == "work":
        if args.generate_report:
            print("Generating...")
        else:
            print("Working...")
    elif args.action == "serve":
        print("Serving...")

def main():
    """
    entry point for setup tools
    """
    parser = create_parser()
    if not sys.stdin.isatty():
        input_string = sys.stdin.read()
        args = parser.parse_args([input_string] + sys.argv[1:])
    else:
        args = parser.parse_args()
    manager(args)

if __name__ == '__main__':
    main()
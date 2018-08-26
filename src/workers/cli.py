# -*- coding: utf-8 -*-
"""
"""

# python standard
import sys
import time
import argparse
import logging

# local imports
from utils.sql_helper import initialize_database
from utils.bucket_helper import BucketHelper
from workers.worker import DcmWorker, DspWorker, Manager, generate_report

############################################################################
str_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG,
                    format=str_format,
                    datefmt='%Y-%m-%d %H:%M',
                    filename='/tmp/dspreview_application.log',
                    filemode='w')
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.CRITICAL)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
logger = logging.getLogger('dspreview_application')
############################################################################


class ChangeWorker(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        namespace.worker = "dsp"
        setattr(namespace, self.dest, values)


class ChangeManager(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        namespace.action = "manage"
        setattr(namespace, self.dest, values)


def create_parser():
    parser = argparse.ArgumentParser(description="""
    CLI to control workers execution. It is also useful for initializing
    the database and serving a web app where it is possible to have a
    report.
    """)
    parser.add_argument("action", type=str,
                        help="""It might be 'init for initialize the database
                        or 'serve' for serving the web app. The default is
                        work, which is about puting a worker to run its
                        task.""", default="work", nargs='?', const=1)
    parser.add_argument("--worker", "-w",
                        type=str, help="The worker to execute",
                        choices=['dcm', 'dsp'])
    parser.add_argument("--dsp", "-d", type=str, default='',
                        help="The specific DSP worker to execute",
                        action=ChangeWorker)
    parser.add_argument("--generate-report", "-g", help="Generate report",
                        required=False, default=False, action='store_true')
    parser.add_argument("--port", "-p", type=int,
                        help="The port for serve the web app",
                        default=8080, required=False)
    parser.add_argument("--delay", "-t", type=int,
                        help="Delay for restart in case of error",
                        default=20, required=False)
    parser.add_argument("--poke", "-k", type=str, help="Add msg to queue",
                        required=False, action=ChangeManager)
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
        logger.info("Initializing the environment")
        initialize_database()

    elif args.action == "manage":
        with Manager() as m:
            m.schedule_task(args.poke)

    elif args.action == "work":
        if args.generate_report:
            logger.info("Generating report")
            generate_report()
        else:
            workers = []
            if args.worker == 'dcm':
                logger.info("Triggering DCM worker")
                workers.append(DcmWorker())
            elif args.dsp:
                logger.info("Triggering DSP worker [{}]".format(args.dsp))
                workers.append(DspWorker(args.dsp))
            else:
                logger.info("Triggering DSP workers")
                dsp_opts = BucketHelper.dsp_available()
                logger.info("Found [{}]".format(", ".join(dsp_opts)))
                for opt in dsp_opts:
                    logger.info("Creating structure for [{}]".format(opt))
                    workers.append(DspWorker(opt))
            for w in workers:
                w.extract().transform().load()

    elif args.action == "serve":
        # keep these together for sanity reasons
        try:
            logger.info("Server requested")
            from webapp.run import app
            app.run(port=args.port)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Server shutdown")
        except Exception as err:
            logger.exception(err)

    elif args.action == "operate":
        logger.info("Operation requested")
        delay = args.delay
        while True:
            try:
                with Manager() as m:
                    m.check_schedule()
            except KeyboardInterrupt:
                break
            except Exception as err:
                logger.exception(err)
                time.sleep(delay)
        logger.info("Operation stopped")

    logger.info("Finish")


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

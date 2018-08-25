# -*- coding: utf-8 -*-
"""
"""

# python standard
import os
import unittest
import logging

# local imports
from workers import cli

logging.disable(logging.CRITICAL)


class TestCli(unittest.TestCase):
    def setUp(self):
        os.environ["FLASK_CONFIG"] = "testing"
        self.parser = cli.create_parser()

    def test_init(self):
        """ init does not require other params """
        args = self.parser.parse_args(['init'])
        self.assertTrue(args.action == "init", "Even init is failing...")

    def test_run_worker(self):
        args = self.parser.parse_args(["--worker", "dcm"])
        self.assertTrue(args.action == "work", "Action should be work!")
        self.assertTrue(args.worker == "dcm", "Worker name is wrong")

    def test_run_worker_with_dsp(self):
        args = self.parser.parse_args(["--worker", "dsp", "--dsp", "dbm"])
        self.assertTrue(args.action == "work", "Action should be work!")
        self.assertTrue(args.dsp == "dbm", "DSP name is wrong!")

    def test_run_dsp_without_worker(self):
        args = self.parser.parse_args(["--dsp", "dbm"])
        self.assertTrue(args.action == "work", "Action should be work!")
        self.assertTrue(args.dsp == "dbm", "DSP name is wrong!")
        self.assertTrue(args.worker == "dsp", "It should be a dsp worker!")

    def test_generate_report(self):
        args = self.parser.parse_args(['--generate-report'])
        self.assertTrue(args.action == "work", "Action should be work!")
        self.assertTrue(args.generate_report, "It should be True!")

    def test_server_with_port(self):
        args = self.parser.parse_args(['serve', '--port', '8080'])
        self.assertTrue(args.action == "serve", "Action should be serve!")
        self.assertTrue(type(args.port) is int, "Port must be an integer!")

    def test_server_without_port(self):
        args = self.parser.parse_args(['serve'])
        self.assertTrue(args.action == "serve", "Action should be serve!")
        self.assertTrue(type(args.port) is int, "Port must be an integer!")

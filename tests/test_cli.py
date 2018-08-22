# -*- coding: utf-8 -*-
"""
"""

import os
import unittest
import unittest.mock as mock
from unittest.mock import patch, mock_open
from src.workers import cli 

class TestCli(unittest.TestCase):
    def setUp(self):
        self.parser = cli.create_parser()

    def test_init(self):
        """ init does not require other params """
        args = self.parser.parse_args(['init'])
        self.assertTrue(args.action=="init", "Even init is failing...")
        cli.manager(args)
    
    def test_run_worker(self):
        args = self.parser.parse_args(["--worker", "dcm"])
        self.assertTrue(args.action == "work", "Action should be work!")
        self.assertTrue(args.worker == "dcm", "Worker name is wrong")
        cli.manager(args)

    def test_run_worker_with_dsp(self):
        args = self.parser.parse_args(["--worker", "dsp", "--dsp", "dbm"])
        self.assertTrue(args.action == "work", "Action should be work!")        
        self.assertTrue(args.dsp == "dbm", "DSP name is wrong!")
        cli.manager(args)

    def test_run_dsp_without_worker(self):
        args = self.parser.parse_args(["--dsp", "dbm"])
        self.assertTrue(args.action == "work", "Action should be work!")
        self.assertTrue(args.dsp == "dbm", "DSP name is wrong!")
        self.assertTrue(args.worker == "dsp", "It should be a dsp worker!")
        cli.manager(args)

    def test_generate_report(self):
        args = self.parser.parse_args(['--generate-report'])
        self.assertTrue(args.action == "work", "Action should be work!")
        self.assertTrue(args.generate_report, "It should be True!")
        cli.manager(args)
    
    def test_server_with_port(self):
        args = self.parser.parse_args(['serve', '--port', '8080'])
        self.assertTrue(args.action == "serve", "Action should be serve!")
        self.assertTrue(type(args.port) is int, "Port must be an integer!")
        cli.manager(args)

    def test_server_without_port(self):
        args = self.parser.parse_args(['serve'])
        self.assertTrue(args.action == "serve", "Action should be serve!")
        self.assertTrue(type(args.port) is int, "Port must be an integer!")
        cli.manager(args)
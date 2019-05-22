#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# Nathaniel Watson
# nathanielwatson@stanfordhealthcare.org
# 2019-05-21
###

"""
Tests functions in the ``sruns_monitor.utils`` module. 
"""

import hashlib
import json
import os
import unittest

from sruns_monitor.tests import WATCH_DIR, TMP_DIR
from sruns_monitor import utils


class TestUtils(unittest.TestCase):
    """
    Tests functions in the ``sruns_monitor.utils`` module.
    """

    def setUp(self):
        self.test_rundir = os.path.join(WATCH_DIR, "CompletedRun1")
        self.test_tarfile_rundir = os.path.join(WATCH_DIR, "test_run_dir.tar.gz")

    def get_md5sum(self, infile):
        m = hashlib.md5()
        fh = open(infile, 'rb')
        m.update(fh.read())
        fh.close()
        return m.hexdigest()

    def test_tar(self):
        """
        Tests the function ``tar()`` for success. Calls the function to tar the test directory
        in ./data/TEST_RUN_DIR, then checks whether the md5sum of the output tarball is equal to
        the md5sum of ./data/test_run_dir.tar.gz (the latter of which was created with `tar -zcf`
        command on Mac OS X. 
        """
        output_file = os.path.join(TMP_DIR, self.test_rundir + ".tar.gz")
        utils.tar(input_dir=self.test_rundir, tarball_name=output_file)
        md5_output_file = self.get_md5sum(output_file)
        md5_test_tarfile_rundir = self.get_md5sum(self.test_tarfile_rundir)
        self.assertEqual(md5_output_file, md5_test_tarfile_rundir)



if __name__ == "__main__":
    unittest.main()

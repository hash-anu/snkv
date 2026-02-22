# SPDX-License-Identifier: Apache-2.0
"""
pytest configuration: add the python/ directory to sys.path so that
`import snkv` works when pytest is invoked from any working directory
without needing PYTHONPATH=.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

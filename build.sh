#!/bin/bash
rm -rf dist
rm -rf build
python setup.py build && python setup.py install

#!/usr/bin/env python
# coding: utf-8
import pathlib,sys,os
import file_parser_gdp

_parentdir = pathlib.Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(_parentdir))

from config.definitions import ROOT_DIR

input = os.path.join(ROOT_DIR,'landing_area','india_gdp.json')
print('Passing input file :' + input)
file_parser_gdp.main(["--inputFile","{inpfile}".format(inpfile=input)])
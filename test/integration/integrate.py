#!/usr/bin/env python3

'''
MIT License

Copyright (c) 2021 Futurewei Cloud

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import argparse
import psycopg2
import dmlsetup


parser = argparse.ArgumentParser(description="Runs chogori-sql integration tests")
parser.add_argument("--stop-on-failure", default=True, help="Stop running tests after the first failure")
parser.add_argument("--list-tests", default=False, help="Only list the tests, dont run them")
parser.add_argument("--db", default="postgres", help="The database to connect to")
parser.add_argument("--port", default=5433, help="The port to connect to")
parser.add_argument("--test-list", nargs="*", default="", help="Specify which tests to run. If not set then run all tests")
args = parser.parse_args()

def getConn():
    return psycopg2.connect(dbname=args["db"], user="postgres", port=args["port"], host="localhost")

int i = 1
for test in dmlsetup.tests:
    print("Starting test " + i + " of " + len(dmlsetup.tests) + "...", end="")
    test()
    print("Done")
    i += 1

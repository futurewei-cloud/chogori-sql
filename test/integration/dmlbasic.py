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

'''
This file has tests for basic ddl statements (not joins, aggregates, or isolation tests)
It depends on the dmlsetup tests.
'''

import unittest
import psycopg2
from helper import commitSQL, selectOneRecord, getConn

class TestDMLBasic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        commitSQL(getConn, "CREATE TABLE dmlbasic (id integer PRIMARY KEY, dataA integer);")

    def test_basicRead(self):
        commitSQL(getConn, "INSERT INTO dmlbasic VALUES (13, 33);")
        conn = getConn()
        record = selectOneRecord(conn, "SELECT * FROM dmlbasic WHERE id=13;")
        self.assertEqual(record[0], 13)
        self.assertEqual(record[1], 33)
        conn.close()

    # TODO delete table on teardown

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

import unittest
import psycopg2
from helper import commitSQL, selectOneRecord, getConn, tableExists


class TestDDL(unittest.TestCase):
    sharedConn = None

    @classmethod
    def setUpClass(cls):
        cls.sharedConn = getConn()

    @classmethod
    def tearDownClass(cls):
        # TODO delete table
        cls.sharedConn.close()

    def test_makeBasicTable(self):
        commitSQL(self.sharedConn, "CREATE TABLE ddltest1 (id integer PRIMARY KEY, dataA integer);")

    def test_makeTableWithManyTypes(self):
        commitSQL(self.sharedConn, "CREATE TABLE ddltest2 (id integer PRIMARY KEY, dataA integer, dataB boolean, dataC real, dataD numeric, dataE text, dataF char[36]);")

    def test_makeTableWithoutPrimaryKey(self):
        commitSQL(self.sharedConn, "CREATE TABLE ddltest3 (id integer, dataA integer);")

    def test_alterTable(self):
        commitSQL(self.sharedConn, "CREATE TABLE ddltest4 (id integer, dataA integer);")
        with self.assertRaises(psycopg2.errors.InternalError):
            commitSQL(self.sharedConn, "ALTER TABLE ddltest4 ADD txtcol text;")

    def test_dropBasicTable(self):
        commitSQL(self.sharedConn, "CREATE TABLE ddltest5 (id integer, dataA integer);")
        commitSQL(self.sharedConn, "DROP TABLE ddltest5 CASCADE;")
        exists = tableExists(self.sharedConn, "ddltest5")
        self.assertEqual(exists, False)
        commitSQL(self.sharedConn, "CREATE TABLE ddltest5 (id integer, dataA text, dataB text);")

# TODO add table already exists error case after #216 is fixed

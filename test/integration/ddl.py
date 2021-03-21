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
from helper import commitSQL, commitSQLWithNewConn, selectOneRecord, getConn


class TestDDL(unittest.TestCase):
    def test_makeBasicTable(self):
        commitSQLWithNewConn(getConn, "CREATE TABLE ddltest1 (id integer PRIMARY KEY, dataA integer);")

    def test_makeTableWithManyTypes(self):
        commitSQLWithNewConn(getConn, "CREATE TABLE ddltest2 (id integer PRIMARY KEY, dataA integer, dataB boolean, dataC real, dataD numeric, dataE text, dataF char[36]);")

    def test_makeTableWithoutPrimaryKey(self):
        commitSQLWithNewConn(getConn, "CREATE TABLE ddltest3 (id integer, dataA integer);")

    def test_alterTable(self):
        #with self.assertRaises(psycopg2.errors.InternalError):
        conn = getConn()
        commitSQL(conn, "CREATE TABLE ddltest4 (id integer, dataA integer);")
        commitSQL(conn, "ALTER TABLE ddltest4 ADD txtcol text;")
        commitSQL(conn, "INSERT INTO ddltest4 VALUES(1, 1, 'mytext')")

        record = selectOneRecord(conn, "SELECT dataB FROM dmlbasic WHERE id=1;")
        self.assertEqual(record[0], 1)
        self.assertEqual(record[1], 1)
        self.assertEqual(record[2], "mytext")

        conn.close()

# TODO add table already exists error case after #216 is fixed
# TODO add drop table


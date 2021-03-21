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
This file has tests for basic dml statements (not joins, aggregates, or isolation tests)
'''

import unittest
import psycopg2
from helper import commitSQL, commitSQLWithNewConn, selectOneRecord, getConn

class TestDMLBasic(unittest.TestCase):
    sharedConn = None

    @classmethod
    def setUpClass(cls):
        commitSQLWithNewConn(getConn, "CREATE TABLE dmlbasic (id integer PRIMARY KEY, dataA integer, dataB integer);")
        commitSQLWithNewConn(getConn, "CREATE TABLE dmlbasic2 (id integer PRIMARY KEY, dataA integer, dataB integer);")
        cls.sharedConn = getConn()

    @classmethod
    def tearDownClass(cls):
        # TODO delete table
        cls.sharedConn.close()

    def test_basicInsertAndRead(self):
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (1, 33, 43);")
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id=1;")
        self.assertEqual(record[0], 1)
        self.assertEqual(record[1], 33)
        self.assertEqual(record[2], 43)

    def test_readNonExistentRecord(self):
        with self.sharedConn:
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM dmlbasic WHERE id=7777;")
                result = cur.fetchall()
                self.assertEqual(len(result), 0)

    def test_basicProjection(self):
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (2, 1, 777);")
        record = selectOneRecord(self.sharedConn, "SELECT dataB FROM dmlbasic WHERE id=2;")
        self.assertEqual(record[0], 777)

    def test_singleRecordUpdate(self):
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (3, 33, 43);")
        commitSQL(self.sharedConn, "UPDATE dmlbasic SET dataA=10, dataB=10 WHERE id=3;")
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id=3;")
        self.assertEqual(record[0], 3)
        self.assertEqual(record[1], 10)
        self.assertEqual(record[2], 10)

    def test_insertOverExisting(self):
        # TODO check this and maybe try vanilla PG to see if there is a more specific error we
        # should be throwing here: https://www.psycopg.org/docs/errors.html
        with self.assertRaises(psycopg2.errors.InternalError):
            commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (4, 1, 1);")
            commitSQLWithNewConn(getConn, "INSERT INTO dmlbasic VALUES (4, 2, 2);")

    def test_insertOverExistingDoNothing(self):
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (5, 1, 1);")
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (5, 2, 2) ON CONFLICT DO NOTHING;")
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id=5;")
        self.assertEqual(record[0], 5)
        self.assertEqual(record[1], 1)
        self.assertEqual(record[2], 1)

    # This tests:
    # 1. Large scan that requires pagination on the chogori server
    # 2. >= scan on primary key (as part of 1)
    # 3. Scan with filter on non-primary key
    # These are in one test to make the inserts easier and faster
    def test_scan(self):
        offset = 5000 # Try to make it easier to not conflict keys with other tests

        # Insert enough records so that pagination will be needed on Chogori
        # TODO double check pagination size
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                for i in range(1, 151):
                    cur.execute("INSERT INTO dmlbasic VALUES (%s, %s, 1);", (i+offset, i))

        # Read them all back
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM dmlbasic WHERE id >= %s;", (offset,))
                for i in range(1, 151):
                    record = cur.fetchone()
                    self.assertNotEqual(record, None)
                    self.assertEqual(record[0], i+offset)
                    self.assertEqual(record[1], i)
                    self.assertEqual(record[2], 1)

        # Scan with filter on non-primary key
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id >= 5000 AND dataA = 50;")
        self.assertEqual(record[0], 50+offset)
        self.assertEqual(record[1], 50)
        self.assertEqual(record[2], 1)
       
    def test_bulkUpdate(self):
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (6, 1, 1);")
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (7, 1, 1);")
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (8, 1, 1);")
        commitSQL(self.sharedConn, "UPDATE dmlbasic SET dataA=11 WHERE id >= 6 AND id <= 8;")
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id=6;")
        self.assertEqual(record[0], 6)
        self.assertEqual(record[1], 11)
        self.assertEqual(record[2], 1)
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id=7;")
        self.assertEqual(record[0], 7)
        self.assertEqual(record[1], 11)
        self.assertEqual(record[2], 1)
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id=8;")
        self.assertEqual(record[0], 8)
        self.assertEqual(record[1], 11)
        self.assertEqual(record[2], 1)

    def test_updateWithFieldReference(self):
        # TODO multi record update with compound key
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (9, 33, 43);")
        commitSQL(self.sharedConn, "UPDATE dmlbasic SET dataA=10+dataA, dataB=10 WHERE id=9;")
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id=9;")
        self.assertEqual(record[0], 9)
        self.assertEqual(record[1], 43)
        self.assertEqual(record[2], 10)

    def test_selectWithFieldReference(self):
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (10, 33, 33);")
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (11, 4, 33);")
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id>=10 AND id <= 11 AND dataA=dataB;")
        self.assertEqual(record[0], 10)
        self.assertEqual(record[1], 33)
        self.assertEqual(record[2], 33)

    def test_selectWithFunction(self):
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (12, 32, 33);")
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (13, 4, 33);")
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id>=12 AND id <= 13 AND dataB=dataA+1;")
        self.assertEqual(record[0], 12)
        self.assertEqual(record[1], 32)
        self.assertEqual(record[2], 33)

    def test_delete(self):
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (14, 1, 1);")
        commitSQL(self.sharedConn, "DELETE FROM dmlbasic WHERE id=14;")
        with self.sharedConn:
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM dmlbasic WHERE id = 14;")
                records = cur.fetchall()
                self.assertEqual(len(records), 0)

    def test_null(self):
        commitSQL(self.sharedConn, "INSERT INTO dmlbasic VALUES (15, NULL, NULL);")
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id=15;")
        self.assertEqual(record[0], 15)
        self.assertEqual(record[1], None)
        self.assertEqual(record[2], None)

    def test_insertIntoTwoTables(self):
        with self.sharedConn:
            with self.sharedConn.cursor() as cur:
                cur.execute("INSERT INTO dmlbasic VALUES (16, 3, 3);")
                cur.execute("INSERT INTO dmlbasic2 VALUES (17, 3, 3);")
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic WHERE id=16;")
        self.assertEqual(record[0], 16)
        self.assertEqual(record[1], 3)
        self.assertEqual(record[2], 3)
        record = selectOneRecord(self.sharedConn, "SELECT * FROM dmlbasic2 WHERE id=17;")
        self.assertEqual(record[0], 17)
        self.assertEqual(record[1], 3)
        self.assertEqual(record[2], 3)


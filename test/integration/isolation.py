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
from helper import commitSQL, selectOneRecord, getConn

class TestIsolation(unittest.TestCase):
    sharedConn = None

    @classmethod
    def setUpClass(cls):
        cls.sharedConn = getConn()
        commitSQL(cls.sharedConn, "CREATE TABLE isolation (id integer PRIMARY KEY, dataA integer, dataB integer);")

    @classmethod
    def tearDownClass(cls):
        # TODO delete table
        cls.sharedConn.close()

    def test_readYourWrite(self):
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                cur.execute("INSERT INTO isolation VALUES (1, 1, 1);")
                cur.execute("SELECT * FROM isolation WHERE id=1;")
                records = cur.fetchall()
                self.assertEqual(len(records), 1)
                record = records[0]
                self.assertEqual(record[0], 1)
                self.assertEqual(record[1], 1)
                self.assertEqual(record[2], 1)

    def test_conflict(self):
        conn = getConn()
        with conn.cursor() as cur:
            cur.execute("INSERT INTO isolation VALUES (2, 2, 2);")

        conn2 = getConn()
        with conn2.cursor() as cur:
            cur.execute("INSERT INTO isolation VALUES (3, 3, 3);")
            cur.execute("SELECT * FROM isolation WHERE id=2;")
            records = cur.fetchall()
            self.assertEqual(len(records), 0)

        with self.assertRaises(psycopg2.errors.InternalError):
            conn.commit()
            conn2.commit()
        conn.close()
        conn2.close()

    def test_mvccRead(self):
        cur1 = self.sharedConn.cursor()
        cur1.execute("INSERT INTO isolation VALUES (4, 2, 2);")

        conn2 = getConn()
        with conn2.cursor() as cur:
            cur.execute("INSERT INTO isolation VALUES (5, 3, 3);")

        cur1.execute("SELECT * FROM isolation WHERE id=5;")
        records = cur1.fetchall()
        self.assertEqual(len(records), 0)
        cur1.close()

        self.sharedConn.commit()
        conn2.commit()
        conn2.close()

    def test_rollback(self):
        with self.sharedConn.cursor() as cur:
            cur.execute("INSERT INTO isolation VALUES (6, 1, 1);")
        self.sharedConn.rollback()

        with self.sharedConn:
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM isolation WHERE id=6;")
                records = cur.fetchall()
                self.assertEqual(len(records), 0)

    def test_readYourDelete(self):
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                cur.execute("INSERT INTO isolation VALUES (7, 7, 7);")
                cur.execute("DELETE FROM isolation WHERE id=7;")
                cur.execute("SELECT * FROM isolation WHERE id=7;")
                records = cur.fetchall()
                self.assertEqual(len(records), 0)

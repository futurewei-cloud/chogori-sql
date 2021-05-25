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
from helper import commitSQL, selectOneRecord, getConn, secIndexExists

class TestIndex(unittest.TestCase):
    sharedConn = None

    @classmethod
    def setUpClass(cls):
        cls.sharedConn = getConn()
        commitSQL(cls.sharedConn, "CREATE TABLE table1 (id integer, dataA text, dataB integer);")
        commitSQL(cls.sharedConn, "CREATE INDEX table1_idx1 ON table1 (id, dataB);")
        commitSQL(cls.sharedConn, "CREATE INDEX table1_idx2 ON table1 (dataA);")

        exists = secIndexExists(cls.sharedConn, "table1", "table1_idx1")
        cls.assertTrue(exists, "Index table1_idx1 should exists")
     
        exists = secIndexExists(cls.sharedConn, "table1", "table1_idx2")
        cls.assertTrue(exists, "Index table1_idx2 should exists")

        with cls.sharedConn: # commits at end of context if no errors
            with cls.sharedConn.cursor() as cur:
                for i in range(1, 1001):
                    cur.execute("INSERT INTO table1 VALUES (%s, %s, %s);", (i, str(i), i + 1))
 
    @classmethod
    def tearDownClass(cls):
        commitSQL(cls.sharedConn, "DROP TABLE table1 CASCADE;")
        cls.sharedConn.close()

    def test_baseSecondaryIndex(self):
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM table1 where id = 100 and dataB = 101;")
                records = cur.fetchall()
                self.assertEqual(len(records), 1)
                cur.execute("SELECT * FROM table1 where dataA = '100';")
                records = cur.fetchall()
                self.assertEqual(len(records), 1)

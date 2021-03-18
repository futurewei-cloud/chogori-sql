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

class TestJoin(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        commitSQL(getConn, "CREATE TABLE join1 (id integer PRIMARY KEY, dataA integer, dataB integer);")
        commitSQL(getConn, "CREATE TABLE join2 (id integer PRIMARY KEY, dataC integer, dataD integer);")

        conn = getConn()
        with conn: # commits at end of context if no errors
            with conn.cursor() as cur:
                for i in range(1, 1001):
                    cur.execute("INSERT INTO join1 VALUES (%s, %s, 1);", (i, i))
                    cur.execute("INSERT INTO join2 VALUES (%s, %s, 1);", (i, i))
        conn.close()

    # Make sure we can do a basic scan over the tables, so we know that any problems in 
    # other tests are caused by the joins
    def test_joinPreTest(self):
        conn = getConn()
        with conn: # commits at end of context if no errors
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM join1;")
                records = cur.fetchall()
                self.assertEqual(len(records), 1000)

        conn.close()

    def test_innerJoin(self):
        conn = getConn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT dataB FROM join1 INNER JOIN join2 ON join1.dataA=join2.dataC AND join1.dataB = join2.dataD;")
                result = cur.fetchall()
                self.assertEqual(len(result), 1000)
        conn.close()

    # TODO delete table on teardown

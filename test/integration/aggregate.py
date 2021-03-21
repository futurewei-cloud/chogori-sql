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

class TestAggregation(unittest.TestCase):
    sharedConn = None

    @classmethod
    def setUpClass(cls):
        commitSQLWithNewConn(getConn, "CREATE TABLE aggregate (id integer PRIMARY KEY, dataA integer, dataB integer);")
        cls.sharedConn = getConn()

        with cls.sharedConn: # commits at end of context if no errors
            with cls.sharedConn.cursor() as cur:
                for i in range(1, 21):
                    b = 1
                    if i % 2 == 0:
                        b = 0
                    cur.execute("INSERT INTO aggregate VALUES (%s, %s, 1);", (i, b))

    @classmethod
    def tearDownClass(cls):
        # TODO delete table
        cls.sharedConn.close()

    def test_count(self):
        record = selectOneRecord(self.sharedConn, "SELECT COUNT(id) FROM aggregate;")
        self.assertEqual(record[0], 20)

    def test_sum(self):
        record = selectOneRecord(self.sharedConn, "SELECT SUM(dataA) FROM aggregate;")
        self.assertEqual(record[0], 10)

    def test_min(self):
        record = selectOneRecord(self.sharedConn, "SELECT MIN(dataA) FROM aggregate;")
        self.assertEqual(record[0], 0)

    def test_max(self):
        record = selectOneRecord(self.sharedConn, "SELECT MAX(dataA) FROM aggregate;")
        self.assertEqual(record[0], 1)

    def test_countWithFilter(self):
        record = selectOneRecord(self.sharedConn, "SELECT COUNT(id) FROM aggregate WHERE dataA=0;")
        self.assertEqual(record[0], 10)


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

@unittest.skip("Fails and causes other tests to fail. See issue #219 and #216")
class TestCompoundKey(unittest.TestCase):
    sharedConn = None

    @classmethod
    def setUpClass(cls):
        commitSQLWithNewConn(getConn, "CREATE TABLE compoundkey (id integer, idstr text, id3 integer, dataA integer, PRIMARY KEY(id, idstr, id3));")
        cls.sharedConn = getConn()

    @classmethod
    def tearDownClass(cls):
        # TODO delete table
        cls.sharedConn.close()

    def test_prefixScan(self):
        # Populate some records for the tests
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkey VALUES (1, 'sometext', %s, 1);", (i,))
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkey VALUES (2, 'someothertext', %s, 2);", (i,))
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkey VALUES (3, 'somemoretext', %s, 3);", (i,))

        # Prefix scan with first two keys specified with =
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM compoundkey WHERE id = 2 AND idstr = 'someothertext';")
                for i in range(1, 11):
                    record = cur.fetchone()
                    self.assertNotEqual(record, None)
                    self.assertEqual(record[0], 2)
                    self.assertEqual(record[1], "someothertext")
                    self.assertEqual(record[2], i)
                    self.assertEqual(record[3], 2)

        # Partial prefix scan with extra filter that is not a prefix
        record = selectOneRecord(self.sharedConn, "SELECT * FROM compoundkey WHERE id = 1 AND id3 = 5;")
        self.assertEqual(record[0], 1)
        self.assertEqual(record[1], "sometext")
        self.assertEqual(record[2], 5)
        self.assertEqual(record[1], 1)

    # TODO add other key types       

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

class TestCompoundKey(unittest.TestCase):
    sharedConn = None

    @classmethod
    def setUpClass(cls):
        cls.sharedConn = getConn()
        commitSQL(cls.sharedConn, "CREATE TABLE compoundkey (id integer, idstr text, id3 integer, dataA integer, PRIMARY KEY(id, idstr, id3));")
        commitSQL(cls.sharedConn, "CREATE TABLE compoundkeyintint (id integer, id2 integer, dataA integer, PRIMARY KEY(id, id2));")
        commitSQL(cls.sharedConn, "CREATE TABLE compoundkeytxttxt (id text, id2 text, dataA integer, PRIMARY KEY(id, id2));")
        commitSQL(cls.sharedConn, "CREATE TABLE compoundkeyboolint (id bool, id2 integer, dataA integer, PRIMARY KEY(id, id2));")

    @classmethod
    def tearDownClass(cls):
        # TODO delete table
        cls.sharedConn.close()

    def test_prefixScanThreeKeys(self):
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

        # Parital Prefix scan with keys specified by inequality
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM compoundkey WHERE id >= 3 AND id3 > 1;")
                for i in range(2, 11):
                    record = cur.fetchone()
                    self.assertNotEqual(record, None)
                    self.assertEqual(record[0], 3)
                    self.assertEqual(record[1], "somemoretext")
                    self.assertEqual(record[2], i)
                    self.assertEqual(record[3], 3)

        # Partial prefix scan with extra filter that is not a prefix
        record = selectOneRecord(self.sharedConn, "SELECT * FROM compoundkey WHERE id = 1 AND id3 = 5;")
        self.assertEqual(record[0], 1)
        self.assertEqual(record[1], "sometext")
        self.assertEqual(record[2], 5)
        self.assertEqual(record[3], 1)

    def test_prefixScanIntInt(self):
        # Populate some records for the tests
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkeyintint VALUES (1, %s, 1);", (i,))
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkeyintint VALUES (2, %s, 2);", (i,))
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkeyintint VALUES (3, %s, 3);", (i,))

        # Prefix scan with first key specified with =
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM compoundkeyintint WHERE id = 2;")
                for i in range(1, 11):
                    record = cur.fetchone()
                    self.assertNotEqual(record, None)
                    self.assertEqual(record[0], 2)
                    self.assertEqual(record[1], i)
                    self.assertEqual(record[2], 2)

    def test_prefixScanTxtTxt(self):
        # Populate some records for the tests
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkeytxttxt VALUES ('1', %s, 1);", (str(i),))
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkeytxttxt VALUES ('2', %s, 2);", (str(i),))
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkeytxttxt VALUES ('3', %s, 3);", (str(i),))

        # Prefix scan with first key specified with =
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM compoundkeytxttxt WHERE id = '2';")
                # The result set is sorted lexographically on the second text key, so here
                # just check that each key is present
                keys = [str(i) for i in range(1,11)]
                for i in range(1, 11):
                    record = cur.fetchone()
                    self.assertNotEqual(record, None)
                    self.assertEqual(record[0], '2')
                    self.assertEqual(str(i) in keys, True)
                    keys.remove(str(i))
                    self.assertEqual(record[2], 2)


    def test_prefixScanBoolInt(self):
        # Populate some records for the tests
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkeyboolint VALUES (TRUE, %s, 1);", (i,))
                for i in range(1, 11):
                    cur.execute("INSERT INTO compoundkeyboolint VALUES (FALSE, %s, 2);", (i,))

        # Prefix scan with first key specified with =
        with self.sharedConn: # commits at end of context if no errors
            with self.sharedConn.cursor() as cur:
                cur.execute("SELECT * FROM compoundkeyboolint WHERE id = FALSE;")
                for i in range(1, 11):
                    record = cur.fetchone()
                    self.assertNotEqual(record, None)
                    self.assertEqual(record[0], False)
                    self.assertEqual(record[1], i)
                    self.assertEqual(record[2], 2)

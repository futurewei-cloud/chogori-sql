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
import argparse

parser = argparse.ArgumentParser(description="Runs chogori-sql integration tests. Also accepts unittest args")
parser.add_argument("--db", default="postgres", help="The database to connect to")
parser.add_argument("--port", default=5433, help="The port to connect to")
args, unknown = parser.parse_known_args()

def getConn():
    return psycopg2.connect(dbname=args.db, user="postgres", port=args.port, host="localhost")

# Helper function for tests. Executes given SQL and commits.
def commitSQLWithNewConn(connFunc, sql):
    conn = connFunc()
    with conn: # commits at end of context if no errors
        with conn.cursor() as cur:
            cur.execute(sql)
    conn.close()

# Helper function for tests. Executes given SQL and commits.
def commitSQL(conn, sql):
    with conn: # commits at end of context if no errors
        with conn.cursor() as cur:
            cur.execute(sql)

# Executes a SQL statement, expected to get one row back and return it
def selectOneRecord(conn, sql):
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
            if len(result) != 1:
                raise RuntimeError("Expected one record but did not get it")
            return result[0]

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
This file has tests for basic ddl statements (not joins, aggregates, or isolation tests)
It depends on the dmlsetup tests.
'''

import psycopg2
from helper import commitSQL, expectEQ

tests = []

def basicRead(connFunc):
    code = commitSQL(connFunc, "INSERT INTO test1 VALUES (13, 33);")
    if code < 0:
        return code
    try:
        conn = connFunc()
        cur = conn.cursor()
        cur.execute("SELECT * FROM test1 WHERE id=13;")
        result = cur.fetchall()
        expectEQ(len(result), 1)
        record = result[0]
        expectEQ(record[0], 13)
        expectEQ(record[1], 33)
    except Exception as exc:
        print("Test failed, exception: " + str(exc))
        code = -1
    finally:
        cur.close()
        conn.close()
    return code
    

tests.append(basicRead)

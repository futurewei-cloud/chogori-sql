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

import psycopg2
import inspect

# Helper function for tests. Executes given SQL and commits.
def commitSQL(connFunc, sql):
    conn = connFunc()
    cur = conn.cursor()
    code = 0
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as exc:
        print("Test failed, exception: " + str(exc))
        code = -1
    finally:
        cur.close()
        conn.close()
    return code

def expectEQ(actual, expected):
    if a == b:
        return
    upframe = inspect.getframeinfo(inspect.currentframe().f_back)
    line = upframe.lineno
    filename = upframe.filename
    
    print(filename + ": " + str(line) + " EQ expectation failed, actual: " + str(actual) + " expected: " + str(expected))
    raise RuntimeError

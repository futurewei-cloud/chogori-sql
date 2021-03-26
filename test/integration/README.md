## chogori-sql integration tests

This directory contains the chogori-sql integration tests. Run it with "./integrate.py" or with 
"./integrate.sh" which will run initDB and postmaster first. Requires python3 and 
"pip3 install psycopg2-binary". It uses python's unittest library and you can pass arguments to it directly 
on the command line.

The tests must be run after initDB and with postmaster running.

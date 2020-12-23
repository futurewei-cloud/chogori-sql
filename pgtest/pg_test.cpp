// compile with
// YB_COMPILER_TYPE=gcc
// g++ -O3 -std=c++17 -I../src/k2/postgres/include/ -L../src/k2/postgres/lib/ pg_test.cpp Logging.cpp -o pg_test -lpq
#include <libpq-fe.h>
#include "Logging.h"

void exit_nicely(PGconn* conn) {
    PQfinish(conn);
    exit(1);
}

PGresult* checkResult(PGresult* res, PGconn* conn, std::string msg) {
    auto status = PQresultStatus(res);
    PQclear(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        K2ERROR("Command: " << msg << ", failed: " << PQerrorMessage(conn));
        exit_nicely(conn);
    }
    K2INFO((msg) << " succeeded");
    return res;
}

int main(int argc, char** argv) {
    k2::logging::LogEntry::procName = argv[0];
    const char* conninfo;
    if (argc > 1)
        conninfo = argv[1];
    else
        conninfo = "dbname = postgres";
    PGconn* conn = PQconnectdb(conninfo);

    if (PQstatus(conn) != CONNECTION_OK) {
        K2ERROR("Connection to database failed: " << PQerrorMessage(conn));
        exit_nicely(conn);
    }
    K2INFO("Connected...");

    /* Start a transaction block */
    checkResult(PQexec(conn, "BEGIN"), conn, "BEGIN");

    checkResult(PQexec(conn, "CREATE TABLE IF NOT EXISTS TBL1(user_id int4, tag_id int4, name text, enabled boolean, PRIMARY KEY(user_id, tag_id))"), conn, "CREATE table");

    checkResult(PQexec(conn, "INSERT INTO TBL1 VALUES(1, 2, 'Alpha', TRUE) ON CONFLICT DO NOTHING"), conn, "Insert 1");
    checkResult(PQexec(conn, "INSERT INTO TBL1 VALUES(2, 2, 'Beta', TRUE) ON CONFLICT DO NOTHING"), conn, "Insert 2");
    checkResult(PQexec(conn, "INSERT INTO TBL1 VALUES(3, 3, 'Gamma', TRUE) ON CONFLICT DO NOTHING"), conn, "Insert 3");

    auto res = PQexec(conn, "SELECT * FROM TBL1 WHERE user_id=1 and tag_id=2");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        K2ERROR("SELECT failed: " << PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    // print the rows
    for (int i = 0; i < PQntuples(res); i++) {
        std::ostringstream os;
        os << "{";
        for (int j = 0; j < PQnfields(res); j++) {
            os << "\"" << PQfname(res, j) << "\"=" << PQgetvalue(res, i, j) << (PQnfields(res)-1 == j ? "" : ", ");
        }
        os << "}";
        K2INFO(os.str());
    }

    PQclear(res);

    /* end the transaction */
    checkResult(PQexec(conn, "END"), conn, "END");

    /* close the connection to the database and cleanup */
    PQfinish(conn);
    return 0;
}

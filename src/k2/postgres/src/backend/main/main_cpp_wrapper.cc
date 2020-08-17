#include <k2/appbase/Appbase.h>
extern "C" {
int PostgresServerProcessMain(int argc, char** argv);
}

int main(int argc, char** argv) {
	k2::App("PG");
	return PostgresServerProcessMain(argc, argv);
}

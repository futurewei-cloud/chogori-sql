#include "postmaster/postmaster_hook.h"

void (*k2_init_func)(int, char **) = 0; /* k2 hook function */

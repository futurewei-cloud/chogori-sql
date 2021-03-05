#include "postmaster/postmaster_hook.h"

void (*k2_init_func)() = 0; /* k2 hook function */
void (*k2_kill_func)(int, unsigned long) = 0; /* k2 hook function */

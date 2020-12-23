#ifndef _POSTMASTER_HOOK_H

/* ----------------------------------------------------------------
 *				external hook support
 * ----------------------------------------------------------------
 */
/*
 * K2 hook support
 * If this hook is set, the PG process will call it during the initialization of a backend worker
 * The hook is used to bootstrap the k2 seastar-based application in a separate thread so that PG
 * can use k2 as a backend storage.
 */
extern void (*k2_init_func)(int, char **);
extern void (*k2_kill_func)(int, unsigned long);

#endif /* _POSTMASTER_HOOK_H */

#ifndef _POSTMASTER_HOOK_H

/* ----------------------------------------------------------------
 *				external hook support
 * ----------------------------------------------------------------
 */
/*
 * K2 hook support
 * If this hook is set, the PG process will call it during the initialization of a backend worker
 * The hook is used to bootstrap
 */
extern void (*k2_init_func)(int, char **);

#endif /* _POSTMASTER_HOOK_H */

#include <sys/types.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <direct.h>
extern int getopt(int, char * const *, const char *);
#else
#include <unistd.h>
#endif

#include "db.h"

#define PERF_CHECK(e)							\
	((e) ? (void)0 :						\
	(fprintf(stderr, "PERF_CHECK failure: %s:%d: \"%s\"",		\
	    __FILE__, __LINE__, #e), abort()))

#define PERF_TESTDIR	"TESTDIR"
#define PERF_DB		"a"

struct db_time {
	u_int32_t secs, usecs;
};

struct db_time	 start_time, end_time;

u_int32_t	 pagesize = 32 * 1024;
u_int		 bulkbufsize = 4 * 1024 * 1024;
u_int            logbufsize = 8 * 1024 * 1024;
u_int            cachesize = 32 * 1024 * 1024;
u_int		 datasize = 32;
u_int	 	 keysize = 8;
u_int            numitems = 0;
FILE             *fp;

char		*progname;

extern void __os_clock(DB_ENV *, u_int32_t *, u_int32_t *);

void cleanup(void);
void op_ds(u_int, int);
void op_ds_bulk(u_int, u_int *);
void op_tds(u_int, int, u_int32_t);
void res(char *, u_int);
void usage(void);

void
res(char *msg, u_int ops)
{
	double elapsed;
	struct db_time v;

	v.secs = end_time.secs - start_time.secs;
	v.usecs = end_time.usecs - start_time.usecs;
	if (start_time.usecs > end_time.usecs) {
		v.secs--;
		v.usecs += 1000000;
	}
	elapsed = v.secs + v.usecs / 1e6;
	printf("%s\n\telapsed time: %f seconds : %g key/data pairs per sec\n",
	    msg, elapsed, ops / elapsed);
}

void
op_ds(u_int ops, int update)
{
	DB *dbp;
	DBT key, data;
	DB_ENV *dbenv;
	DB_MPOOL_STAT *gsp;
	char *databuf, *keybuf;

	cleanup();

	keybuf = malloc(keysize);
	PERF_CHECK(keybuf != NULL);

	PERF_CHECK((databuf = malloc(datasize)) != NULL);
	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	key.data = keybuf;
	key.size = keysize;
	memset(keybuf, 'a', keysize);

	data.data = databuf;
	data.size = datasize;
	memset(databuf, 'b', datasize);

	PERF_CHECK(db_create(&dbp, NULL, 0) == 0);

	dbp->set_errfile(dbp, stderr);

	PERF_CHECK(dbp->set_pagesize(dbp, pagesize) == 0);
	PERF_CHECK(
	    dbp->open(dbp, NULL, NULL, NULL, DB_BTREE, DB_CREATE, 0666) == 0);

	dbenv = dbp->dbenv;
	PERF_CHECK(dbenv->memp_stat(dbenv, &gsp, NULL, DB_STAT_CLEAR) == 0);

	if (update) {	        
		__os_clock(NULL, &start_time.secs, &start_time.usecs);
		for (; ops > 0; --ops) {
			keybuf[(ops % keysize)] =
			    "abcdefghijklmnopqrstuvwxuz"[(ops % 26)];
			PERF_CHECK(dbp->put(dbp, NULL, &key, &data, 0) == 0);
		}
		__os_clock(NULL, &end_time.secs, &end_time.usecs);
	} else {
		PERF_CHECK(dbp->put(dbp, NULL, &key, &data, 0) == 0);
		__os_clock(NULL, &start_time.secs, &start_time.usecs);
		for (; ops > 0; --ops)
			PERF_CHECK(dbp->get(dbp, NULL, &key, &data, 0) == 0);
		__os_clock(NULL, &end_time.secs, &end_time.usecs);
	}

	PERF_CHECK(dbenv->memp_stat(dbenv, &gsp, NULL, 0) == 0);
	PERF_CHECK(gsp->st_cache_miss == 0 || gsp->st_cache_miss == 1);

	PERF_CHECK(dbp->close(dbp, DB_NOSYNC) == 0);
	free(keybuf);
	free(databuf);
}

void
op_ds_bulk(u_int ops, u_int *totalp)
{
	DB *dbp;
	DBC *dbc;
	DBT key, data;
	DB_ENV *dbenv;
	DB_MPOOL_STAT  *gsp;
	u_int32_t len, klen;
	u_int i, total;
	char *keybuf, *databuf;
	void *pointer, *dp, *kp;

	cleanup();

	/*
	 * Need enough keysize, or a memory corruption
	 * occurs.
	 */
	PERF_CHECK((keybuf = malloc(keysize+4)) != NULL);
	PERF_CHECK((databuf = malloc(bulkbufsize)) != NULL);

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	key.data = keybuf;
	key.size = keysize;

	data.data = databuf;
	data.size = datasize;
	memset(databuf, 'b', datasize);

	PERF_CHECK(db_create(&dbp, NULL, 0) == 0);

	dbp->set_errfile(dbp, stderr);

	PERF_CHECK(dbp->set_pagesize(dbp, pagesize) == 0);
	PERF_CHECK(dbp->set_cachesize(dbp, 0, cachesize, 1) == 0);
	PERF_CHECK(
	    dbp->open(dbp, NULL, NULL, NULL, DB_BTREE, DB_CREATE, 0666) == 0);

	for (i = 1; i <= numitems; ++i) {
		(void)sprintf(keybuf, "%10d", i);
		PERF_CHECK(dbp->put(dbp, NULL, &key, &data, 0) == 0);
	}

#if 0
	PERF_CHECK((fp = fopen("before", "w")) != NULL);
	dbp->set_msgfile(dbp, fp);
	PERF_CHECK(dbp->stat_print(dbp, DB_STAT_ALL) == 0);
	PERF_CHECK(fclose(fp) == 0);
#endif
 
	PERF_CHECK(dbp->cursor(dbp, NULL, &dbc, 0) == 0);

	data.ulen = bulkbufsize;
	data.flags = DB_DBT_USERMEM;

	dbenv = dbp->dbenv;
	PERF_CHECK(dbenv->memp_stat(dbenv, &gsp, NULL, DB_STAT_CLEAR) == 0);

	if (ops > 10000)
		ops = 10000;

	__os_clock(NULL, &start_time.secs, &start_time.usecs);
	for (total = 0; ops > 0; --ops) {
		PERF_CHECK(dbc->c_get(
		    dbc, &key, &data, DB_FIRST | DB_MULTIPLE_KEY) == 0);
		DB_MULTIPLE_INIT(pointer, &data);
		while (pointer != NULL) {
			DB_MULTIPLE_KEY_NEXT(pointer, &data, kp, klen, dp, len);
			if (kp != NULL)
				++total;
		}
	}
	__os_clock(NULL, &end_time.secs, &end_time.usecs);
	*totalp = total;

	PERF_CHECK(dbenv->memp_stat(dbenv, &gsp, NULL, 0) == 0);
	PERF_CHECK(gsp->st_cache_miss == 0 || gsp->st_cache_miss == 1);

#if 0
	PERF_CHECK((fp = fopen("before", "w")) != NULL);
	dbp->set_msgfile(dbp, fp);
	PERF_CHECK(dbp->stat_print(dbp, DB_STAT_ALL) == 0);
	PERF_CHECK(fclose(fp) == 0);
#endif

	PERF_CHECK(dbp->close(dbp, DB_NOSYNC) == 0);
	free(keybuf);
	free(databuf);
}

void
op_tds(u_int ops, int update, u_int32_t txn_flags)
{
	DB *dbp;
	DBT key, data;
	DB_ENV *dbenv;
	DB_MPOOL_STAT *gsp;
	DB_TXN *txn;
	char *keybuf, *databuf;

	cleanup();

	PERF_CHECK((keybuf = malloc(keysize)) != NULL);
	PERF_CHECK((databuf = malloc(datasize)) != NULL);

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	key.data = keybuf;
	key.size = keysize;
	memset(keybuf, 'a', keysize);

	data.data = databuf;
	data.size = datasize;
	memset(databuf, 'b', datasize);

	PERF_CHECK(db_env_create(&dbenv, 0) == 0);

	dbenv->set_errfile(dbenv, stderr);

	PERF_CHECK(
	    dbenv->set_flags(dbenv, DB_AUTO_COMMIT | txn_flags, 1) == 0);
	PERF_CHECK(dbenv->set_lg_bsize(dbenv, logbufsize) == 0);
	PERF_CHECK(dbenv->open(dbenv, PERF_TESTDIR,
	    DB_CREATE | DB_PRIVATE | DB_INIT_LOCK | DB_INIT_LOG |
	    DB_INIT_MPOOL | DB_INIT_TXN, 0666) == 0);

	PERF_CHECK(db_create(&dbp, dbenv, 0) == 0);
	PERF_CHECK(dbp->set_pagesize(dbp, pagesize) == 0);
	PERF_CHECK(dbp->open(dbp,
	    NULL, PERF_DB, NULL, DB_BTREE, DB_CREATE, 0666) == 0);

	if (update) {
		PERF_CHECK(
		    dbenv->memp_stat(dbenv, &gsp, NULL, DB_STAT_CLEAR) == 0);

		__os_clock(NULL, &start_time.secs, &start_time.usecs);
		for (; ops > 0; --ops)
			PERF_CHECK(dbp->put(dbp, NULL, &key, &data, 0) == 0);
		__os_clock(NULL, &end_time.secs, &end_time.usecs);

		PERF_CHECK(dbenv->memp_stat(dbenv, &gsp, NULL, 0) == 0);
		PERF_CHECK(gsp->st_page_out == 0);
	} else {
		PERF_CHECK(dbp->put(dbp, NULL, &key, &data, 0) == 0);
		PERF_CHECK(
		   dbenv->memp_stat(dbenv, &gsp, NULL, DB_STAT_CLEAR) == 0);

		__os_clock(NULL, &start_time.secs, &start_time.usecs);
		for (; ops > 0; --ops) {
			PERF_CHECK(
			    dbenv->txn_begin(dbenv, NULL, &txn, 0) == 0);
			PERF_CHECK(dbp->get(dbp, NULL, &key, &data, 0) == 0);
			PERF_CHECK(txn->commit(txn, 0) == 0);
		}
		__os_clock(NULL, &end_time.secs, &end_time.usecs);

		PERF_CHECK(dbenv->memp_stat(dbenv, &gsp, NULL, 0) == 0);
		PERF_CHECK(gsp->st_cache_miss == 0 || gsp->st_cache_miss == 2);
	}

	PERF_CHECK(dbp->close(dbp, DB_NOSYNC) == 0);
	PERF_CHECK(dbenv->close(dbenv, 0) == 0);
	free(keybuf);
	free(databuf);
}

void
cleanup()
{
#ifdef _WIN32
	_unlink("TESTDIR/a");
	_unlink(PERF_DB);
	_rmdir(PERF_TESTDIR);
	_mkdir(PERF_TESTDIR);
#else
	(void)system("rm -rf a TESTDIR; mkdir TESTDIR");
#endif
}

int
main(int argc, char *argv[])
{
	extern char *optarg;
	extern int optind;
	u_int ops, total;
	int ch, major, minor, patch;

	if ((progname = strrchr(argv[0], '/')) == NULL)
		progname = argv[0];
	else
		++progname;

	ops = 1000000;

	while ((ch = getopt(argc, argv, "d:k:o:p:")) != EOF)
		switch (ch) {
		case 'd':
			datasize = (u_int)atoi(optarg);
			break;
		case 'k':
			keysize = (u_int)atoi(optarg);
			break;
		case 'o':
			ops = (u_int)atoi(optarg);
			break;
		case 'p':
			pagesize = (u_int32_t)atoi(optarg);
			break;
		case '?':
		default:
			usage();
		}
	argc -= optind;
	argv += optind;

	numitems = (cachesize / (keysize + datasize - 1)) / 2;

	(void)db_version(&major, &minor, &patch);
	printf("Using Berkeley DB %d.%d.%d - ", major, minor, patch);
	printf("ops: %u; keysize: %d; datasize: %d\n", ops, keysize, datasize);

	op_ds(ops, 0);
        res("Data Store (read):", ops);

	if (keysize >= 8) {
		op_ds_bulk(ops, &total);
		res("Data Store (bulk read):", total);
	} else {
		printf("Data Store (bulk read):\n");
		printf("\tskipped: bulk get requires a key size >= 10\n");
	}

	op_ds(ops, 1);
        res("Data Store (write):", ops);

	op_tds(ops, 0, 0);
	res("Transactional Data Store (read):", ops);

	op_tds(ops, 1, DB_LOG_INMEMORY);
	res("Transactional Data Store (write, in-memory logging):", ops);

	op_tds(ops, 1, DB_TXN_NOSYNC);
	res("Transactional Data Store (write, no-sync on commit):", ops);

	op_tds(ops, 1, DB_TXN_WRITE_NOSYNC);
	res("Transactional Data Store (write, write-no-sync on commit):", ops);

	op_tds(ops, 1, 0);
	res("Transactional Data Store (write, sync on commit):", ops);

	return (EXIT_SUCCESS);
}

void
usage()
{
	fprintf(stderr,
	    "usage: %s [-d datasize] [-k keysize] [-o ops] [-p pagesize]\n",
	    progname);
	exit(1);
}

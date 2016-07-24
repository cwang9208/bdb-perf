
t: t.c
	cc t.c -o t \
	-g -I/usr/local/BerkeleyDB.4.3/include -L/usr/local/BerkeleyDB.4.3/lib -ldb-4.3

test:
	time ./t
	rm -rf ./TESTDIR

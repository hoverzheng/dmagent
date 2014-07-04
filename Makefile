CC=gcc
ARCH := $(shell uname -m)
X64 = x86_64
PROGS =	dmagent

ifeq ($(ARCH), $(X64))
	M64 = -m64
endif

LIBS = -levent -lm -L/usr/local/lib -ltcmalloc
CFLAGS = -Wall -g -O2 $(M64)


all: $(PROGS)

STPROG = dmagent.o ketama.o md5.o log.o queue.c


queue.o: queue.c queue.h common.h
	$(CC) $(CFLAGS) -c -o $@ queue.c

log.o: log.c log.h
	$(CC) $(CFLAGS) -c -o $@ log.c

md5.o: md5.c md5.h
	$(CC) $(CFLAGS) -c -o $@ md5.c

ketama.o: ketama.c md5.c ketama.h md5.h
	$(CC) $(CFLAGS) -c -o $@ ketama.c

dmagent.o: dmagent.c ketama.h md5.h
	$(CC) $(CFLAGS) -c -o $@ dmagent.c

dmagent: $(STPROG)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS) $(ALLOC_LINK)

install:
	cp $(PROGS) /usr/local/bin/
	cp ./dmagent.conf /usr/local/etc/
	cp ./utils/dmagent_init_scripts /etc/init.d/dmagentd
	cp ./utils/monitor_dmagent /usr/local/bin/
	cp ./utils/monitor_dmagentd /etc/init.d/

clean:
	rm -f *.o *~ $(PROGS)

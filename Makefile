CC=gcc
ARCH := $(shell uname -m)
X64 = x86_64
PROGS =	dmagent

ifeq ($(ARCH), $(X64))
	M64 = -m64
	LIBS = /usr/lib64/libevent.a /usr/lib64/libm.a 
else
	LIBS = -levent -lm -L/usr/local/lib
endif

#CFLAGS = -Wall -g -O2 -I/usr/local/include $(M64)
CFLAGS = -Wall -g 

all: $(PROGS)

STPROG = dmagent.o ketama.o md5.o log.o


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


clean:
	rm -f *.o *~ $(PROGS)

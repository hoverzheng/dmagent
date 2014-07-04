/* 

Copyright (c) 2008, 2010 QUE Hongyu
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:
1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
SUCH DAMAGE.
*/

/* Changelog:
 * 2008-08-20, coding started
 * 2008-09-04, v0.1 finished
 * 2008-09-07, v0.2 finished, code cleanup, drive_get_server function
 * 2008-09-09, support get/gets' multi keys(can > 7 keys)
 * 2008-09-10, ketama allocation
 * 2008-09-12, backup server added
 * 2008-09-12, try backup server for get/gets command
 * 2008-09-16, v0.3 finished
 * 2008-09-20, support unix domain socket
 * 2008-09-23, write "END\r\n" with the last packet of GET/GETS response
 * 2008-09-23, combine drive_get_server with drive_server functions -> drive_memcached_server function
 * 2008-10-05, fix header file include under BSD systems

 * hover changlog
 * 2013-11-1,  changed ketama algorithm.
 * 2013-11-20, add the function that delete node if the node is down or add node when the node restart automatically .
 */

#define _GNU_SOURCE
#include <sys/types.h>

#if defined(__FreeBSD__)
#include <sys/uio.h>
#include <limits.h>
#else
#include <getopt.h>
#endif

#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <event.h>
#include <pthread.h>

#include <assert.h>
#include <unistd.h>

#include "ketama.h"
#include "log.h"

#include "common.h"
#include "cqueue.h"
#include "dmagent.h"

// zxh added for buffer debug
int total_buffers = 0;
int total_buffer_size = 0;
// end zxh

/* static variables */
static int port = 11211;
//static unsigned long port = 11211;
static int maxconns = 4096, curconns = 0, sockfd = -1; 
int verbose_mode = 0; 
/* zxh chang to 1 */
static int use_ketama = 1;
static struct event ev_master;

static struct matrix *matrixs = NULL; /* memcached server list */
static int matrixcnt = 0;
//static struct ketama *ketama = NULL;

static struct matrix *backups= NULL; /* backup memcached server list */
static int backupcnt = 0;
//static struct ketama *backupkt = NULL;

// zxh added for ketama algorithm
// current ketama hash circle server list.
static serverinfo **ktm_slist = NULL;
static ketama_continuum ktm_kc = NULL;
static int ktm_numservers; 
extern int total_ktm_numservers; 
static unsigned long ktm_memory;

/* dead server circle queue. */
static queue_t *dead_server_q;

// conn cache, default size is 1024 struct conn pointer;
//static int conn_caches_used;
//static struct conn *conn_caches[CONN_CACHE_SIZE];

//default weight, the first weight is bigest.
//static unsigned long ktm_weight = 100;
static pthread_mutex_t  ktm_lock = PTHREAD_MUTEX_INITIALIZER; 
static volatile int ketama_mantain_signal;
static pthread_cond_t ketama_mantain_cond = PTHREAD_COND_INITIALIZER;
static volatile int do_run_mantain_ketama_thread = 1;
// end zxh

static char *socketpath = NULL;
static int unixfd = -1;
static struct event ev_unix;

static int maxidle = 20; /* max keep alive connections for one memcached server */

static struct event ev_timer;
time_t cur_ts;
char cur_ts_str[128];


//zxh added
static int creat_ktm_slist(struct matrix *m, int nmt);

static void drive_client(const int, const short, void *);
static void drive_backup_server(const int, const short, void *);
static void drive_memcached_server(const int, const short, void *);
static void finish_transcation(conn *);
static void do_transcation(conn *);
static void start_magent_transcation(conn *);
static void out_string(conn *, const char *);
static void process_update_response(conn *);
static void process_get_response(conn *, int);
static void append_buffer_to_list(list *, buffer *);
//static void try_backup_server(conn *);
static void dump(int signo);

static void
show_help(void)
{
	char *b = "memcached agent v" VERSION " Build-Date: " __DATE__ " " __TIME__ "\n"
		  "Usage:\n  -h this message\n" 
		   "  -u uid\n" 
		   "  -g gid\n"
		   "  -c configure file name\n"
		   "  -p port, default is 11211. (0 to disable tcp support)\n"
		   "  -l ip, local bind ip address, default is 0.0.0.0\n"
		   "  -n number, set max connections, default is 4096\n"
		   "  -D don't go to background\n"
		   "  -f file, unix socket path to listen on. default is off\n"
		   "  -i number, set max keep alive connections for one memcached server, default is 20\n"
		   "  -v verbose\n"
		   "\n";
	fprintf(stderr, b, strlen(b));
}

static void print_slist()
{
	int i;

	for (i = 0; i < ktm_numservers+1; i++) {
		if (NULL == ktm_slist[i])
			fprintf(stderr, "slist:%d is NULL\n", i);
		else
			fprintf(stderr, "ip:%s,port:%ld\n", ktm_slist[i]->ip, ktm_slist[i]->port);
	}
}


static int
maintain_ketama_srv_worker(void)
{
	serverinfo *psrv;
	int ret = OK;
	char ip[32]={'\0'};
	char *ps;
	int i = 0;
	int dead_server_n = dead_server_q->size;

	if (ktm_numservers == 0) {
		fprintf(stderr, "notice no availed server!\n");
		sleep(1);
		return OK;
	} else {
		if (verbose_mode) fprintf(stderr, "%d servers are available!\n", ktm_numservers);
	}

	for (i = 0; i < dead_server_n; i++) {
		if (OK != dequeue(dead_server_q, &psrv)) {
			if(verbose_mode) fprintf(stderr, "get dead server error!\n");
			dmlog("get server from dead server queue error.");
		}

		/* zxh deleted. 2014-03-13
		ps = NULL;
		strncpy(ip, psrv->ip, 32);
		ps = strchr(ip, ':');
		if (ps) *ps = '\0';
		*/
		
		if (!psrv->is_alive) continue; 					//this node is deleted
		// zxh changed. 2014/3/13
		//ret = delete_server_node(ktm_slist, &ktm_numservers, &ktm_memory, ip);
		ret = delete_server_node(ktm_slist, &ktm_numservers, &ktm_memory, psrv);
		if (FAIL == ret) {
			//dmlog("delete server :%s error!\n", ip);
			dmlog("delete server :%s error!\n", psrv->ip);
			continue;
		} 
		if(verbose_mode)
			fprintf(stderr, "delete server :%s ok, reset now!\n", psrv->ip);
		ret = ketama_reset(&ktm_kc, ktm_slist, ktm_numservers, ktm_memory);
		if (FAIL == ret) {
			dmlog("reset ketama after deleted server :%s error!\n", psrv->ip);
			continue;
		} 
		psrv->is_alive = FALSE;
		dmlog("ip:%s is deleted successfully.", psrv->ip);
	}
	return OK;
}


static int
maintain_ketama_srv_worker2(void)
{
	int sockfd;
	struct sockaddr_in servaddr;
	serverinfo *psrv;
	socklen_t servlen;
	int ret = OK;
	char ip[32]={'\0'};
	char *ps;
	int i = 0;

	if (ktm_numservers == 0) {
		fprintf(stderr, "notice no availed server!\n");
		sleep(1);
		return OK;
	} else {
		if (verbose_mode) fprintf(stderr, "%d servers are available!\n", ktm_numservers);
	}

	for (i = 0; i < matrixcnt; i++) {
		psrv = &(matrixs[i].s);

		sockfd = socket(AF_INET, SOCK_STREAM, 0); 
		if (sockfd < 0) {
			fprintf(stderr, "create socket error:%s\n",strerror(errno));
			return FAIL;
		}

		bzero(&servaddr, sizeof(servaddr));
		bzero(ip, sizeof(ip));
		ps = NULL;

		servaddr.sin_family = AF_INET;
		// zxh chang the default port to the port user set. 2014/3/13
		//servaddr.sin_port = htons(DEFAULT_PORT);
		servaddr.sin_port = htons(psrv->port);
		strncpy(ip, psrv->ip, IP_LEN);
		ps = strchr(ip, ':');
		if (ps) *ps = '\0';
		
		if (verbose_mode) 
			fprintf(stderr, "check ip =[%s] port=[%d], islive=%d\n", ip, psrv->port, psrv->is_alive);
		if (inet_pton(AF_INET, ip, &servaddr.sin_addr) < 0) {
			fprintf(stderr, "inet_pton error: %s\n", strerror(errno));
			continue;
		}
		servlen = sizeof(servaddr);

		if (-1 == connect(sockfd, (struct sockaddr *)&servaddr, servlen)) {
			if (errno != EINPROGRESS && errno != EALREADY) { 	//server is not available.
				if (!psrv->is_alive) continue; 					//this node is deleted
				if(verbose_mode)
					fprintf(stderr, "connect to %s error\n", psrv->ip);
				ret = delete_server_node(ktm_slist, &ktm_numservers, &ktm_memory, psrv);
				if (FAIL == ret) {
					fprintf(stderr, "delete server :%s error!\n", psrv->ip);
					continue;
				} 
				if(verbose_mode)
					fprintf(stderr, "delete server :%s ok, reset now!\n", psrv->ip);
				ret = ketama_reset(&ktm_kc, ktm_slist, ktm_numservers, ktm_memory);
				if (FAIL == ret) {
					fprintf(stderr, "reset ketama after deleted server :%s error!\n", psrv->ip);
					continue;
				} 
				psrv->is_alive = FALSE;
				dmlog("ip:%s is deleted successfully.", psrv->ip);
			}
		} else {
			fprintf(stderr, "connect to %s ok\n", psrv->ip);
			close(sockfd);
		}
	}
	return OK;
}



// zxh added
/*
 * Check server whether is alive or not.
 * If server is not alive delete is from ketama and reset ketama server list.
 */
static void 
maintain_ketama_srv_thread(void)
{
	sleep(1);

	int r = 0;
	pthread_mutex_lock(&ktm_lock);
	while (1) {
		if (ketama_mantain_signal == 1) {
			if(verbose_mode)
				fprintf(stderr, "maintain ketama srv working!\n");
			maintain_ketama_srv_worker();
			ketama_mantain_signal = 0;
		}

    	if (ketama_mantain_signal == 0) { 
    	    /* always hold this lock while we're running */
			if (verbose_mode)
				fprintf(stderr, "waiting for cond signal 1111 now!\n");
    	    r = pthread_cond_wait(&ketama_mantain_cond, &ktm_lock);
			if(verbose_mode)
				fprintf(stderr, "waiting for cond signal 2222 now r=%d!\n", r);
    	}
		//pthread_mutex_unlock(&ktm_lock);
	}
}


// zxh added
/*
 * Check server whether is alive or not.
 * If server is not alive delete is from ketama and reset ketama server list.
 */
static void 
maintain_add_srv_thread(void)
{
	int sockfd;
	struct sockaddr_in servaddr;
	serverinfo *psrv;
	socklen_t servlen;
	int ret = OK;
	char ip[32]={'\0'};
	char *ps;
	int i = 0;
	int fin_res = FAIL;
	struct timeval tm;
	fd_set set;
	socklen_t len;
	int error=-1;

	/*
     * initalize ketama server list
     */
	maintain_ketama_srv_worker2();

	while (1) {
		sleep(3); //every 2's check 
		if (verbose_mode) fprintf(stderr, "matrixcnt=%d,ktm_numservers=%d\n", matrixcnt, ktm_numservers);
		/*
		if (matrixcnt == ktm_numservers) {
			sleep(5);
			maintain_ketama_srv_worker();
			continue;
		}
		*/
		for (i = 0; i < matrixcnt; i++) {
			psrv = &(matrixs[i].s);
			if (psrv->is_alive)		/* here just deal with not alive host. */
				continue;	

			sockfd = socket(AF_INET, SOCK_STREAM, 0); 
			if (sockfd < 0) {
				fprintf(stderr, "create socket error:%s\n",strerror(errno));
				continue;
			}
			fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL)|O_NONBLOCK);

			bzero(&servaddr, sizeof(servaddr));
			bzero(ip, sizeof(ip));
			ps = NULL;

			servaddr.sin_family = AF_INET;
			//servaddr.sin_port = htons(DEFAULT_PORT);
			// zxh changed to use port user defined.
			servaddr.sin_port = htons(psrv->port);
			strncpy(ip, psrv->ip, IP_LEN);
			ps = strchr(ip, ':');
			if (ps) *ps = '\0';
			
			if (inet_pton(AF_INET, ip, &servaddr.sin_addr) < 0) {
				fprintf(stderr, "inet_pton error: %s\n", strerror(errno));
				continue;
			}
			servlen = sizeof(servaddr);

			ret = connect(sockfd, (struct sockaddr *)&servaddr, servlen);
			if (0 != ret) {
				do {
            		tm.tv_sec  = 0;
            		tm.tv_usec = 500000; /* 500ms */
            		FD_ZERO(&set);
            		FD_SET(sockfd, &set);
            		ret = select(sockfd+1, NULL, &set, NULL, &tm);

            		if (0 == ret) { 	/* time out */
            		    fin_res = FAIL;
            		    break;
            		}

            		if(ret < 0 && errno != EINTR) { /* error happen */
            		    fin_res = FAIL;
            		    break;
            		} else if (ret > 0) {
            		    len = sizeof(int);
            		    if(getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, (socklen_t *)&len) < 0) {
            		        fin_res = FAIL;
            		        break;
            		    }
            		    if(error != 0) { /* error happend in connect */
            		        fin_res = FAIL;
            		        break;
            		    }
            		    fin_res = OK;
            		    break;
            		} else { 	/* other error */
            		    fin_res = FAIL;
            		    break;
            		}
        		} while (1);
			} else { 	/* the host can be available. */
				fin_res = OK;
			}

			if (OK != fin_res) {
				close(sockfd);
				continue;
			}
			
			/* connect is ok */
			if(verbose_mode) fprintf(stderr, "connect to %s ok\n", ip);
			pthread_mutex_lock(&ktm_lock);
			ret = add_server_node(ktm_slist, &ktm_numservers, &ktm_memory, psrv);
			if (FAIL == ret) {
				if (verbose_mode) fprintf(stderr,"add server:[%s] to ktm_slist error!\n", psrv->ip);
				pthread_mutex_unlock(&ktm_lock);
				continue;
			}
			ret = ketama_reset(&ktm_kc, ktm_slist, ktm_numservers, ktm_memory);
			//ketama_print_continuum(ktm_kc);
			if (FAIL == ret) {
				pthread_mutex_unlock(&ktm_lock);
				if (verbose_mode) fprintf(stderr, "reset ketama after added server :%s error!\n", ip);
				continue;
			} 
			psrv->is_alive = TRUE;
			dmlog("ip:%s is added successfully.", psrv->ip);
			pthread_mutex_unlock(&ktm_lock);
			close(sockfd);
		}
	}
}
// end zxh


/* the famous DJB hash function for strings from stat_cache.c*/
static int
hashme(char *str)
{
	unsigned int hash = 5381;
	const char *s;

	if (str == NULL) return 0;

	for (s = str; *s; s++) { 
		hash = ((hash << 5) + hash) + *s;
	}
	hash &= 0x7FFFFFFF; /* strip the highest bit */
	return hash;
}

static buffer *
buffer_init_size(int size, char *s)
{
	buffer *b;

	if (size <= 0) return NULL;
	b = (struct buffer *) calloc(sizeof(struct buffer), 1);
	if (b == NULL) return NULL;

	size +=  BUFFER_PIECE_SIZE - (size %  BUFFER_PIECE_SIZE);

	b->ptr = (char *) calloc(1, size);
	if (b->ptr == NULL) {
		free(b);
		return NULL;
	}

	b->len = size;
	//zxh added for record
	total_buffers++;
	total_buffer_size += (size+sizeof(struct buffer));
	strncpy(b->bname, s, 15);
	#if 0
	if (verbose_mode) {
		//fprintf(stderr, "sizeof(buffer): %d, size=%d\n", sizeof(struct buffer), size);
		printf("%s calloc: %d\n", b->bname, size+sizeof(struct buffer));
	}
	#endif
	// end zxh
	return b;
}


// zxh added for debug
static void
buffer_free(buffer *b, char *s)
{
	if (!b) return;

	//zxh added for record
	total_buffers--; 
	total_buffer_size -= (b->len + sizeof(struct buffer));

	#if 0
	if (verbose_mode)	
		printf("%s free: %d\n", b->bname, b->len+sizeof(struct buffer));
	#endif
	//end zxh

	free(b->ptr);
	free(b);
}
// end zxh

static list *
list_init(void)
{
	list *l;

	l = (struct list *) calloc(sizeof(struct list), 1);
	return l;
}

static void
list_free(list *l, int keep_list)
{
	buffer *b, *n;

	if (l == NULL) return;

	b = l->first;
	while(b) {
		n = b->next;
		buffer_free(b, "list");
		b = n;
	}

	if (keep_list)
		l->first = l->last = NULL;
	else
		free(l);
}

static void
remove_finished_buffers(list *l)
{
	buffer *n, *b;

	if (l == NULL) return;
	b = l->first;
	while(b) {
		if (b->used < b->size) /* incompleted buffer */
			break;
		n = b->next;
		buffer_free(b, "finish list");
		b = n;
	}

	if (b == NULL) {
		l->first = l->last = NULL;
	} else {
		l->first = b;
	}
}

static void
copy_list(list *src, list *dst)
{
	buffer *b, *r;
	int size = 0;

	if (src == NULL || dst == NULL || src->first == NULL) return;

	b = src->first;
	while(b) {
		size += b->size;
		b = b->next;
	}

	if (size == 0) return;

	r = buffer_init_size(size+1, "copy_list");
	if (r == NULL) return;

	b = src->first;
	while(b) {
		if (b->size > 0 ) {
			memcpy(r->ptr + r->size, b->ptr, b->size);
			r->size += b->size;
		}
		b = b->next;
	}
	append_buffer_to_list(dst, r);
}

static void
move_list(list *src, list *dst)
{
	if (src == NULL || dst == NULL || src->first == NULL) return;

	if (dst->first == NULL)
		dst->first = src->first;
	else
		dst->last->next = src->first;

	dst->last = src->last;

	src->last = src->first = NULL;
}

static void
append_buffer_to_list(list *l, buffer *b)
{
	if (l == NULL || b == NULL)
		return;

	if (l->first == NULL) {
		l->first = l->last = b;
	} else {
		l->last->next = b;
		l->last = b;
	}
}

/* return -1 if not found
 * return pos if found
 */
static int
memstr(char *s, char *find, int srclen, int findlen)
{
	char *bp, *sp;
	int len = 0, success = 0;
	
	if (findlen == 0 || srclen < findlen) return -1;
	for (len = 0; len <= (srclen-findlen); len ++) {
		if (s[len] == find[0]) {
			bp = s + len;
			sp = find;
			do {
				if (!*sp) {
					success = 1;
					break;
				}
			} while (*bp++ == *sp++);
			if (success) break;
		}
	}

	if (success) return len;
	else return -1;
}

static size_t
tokenize_command(char *command, token_t *tokens, const size_t max_tokens)
{
	char *s, *e;
	size_t ntokens = 0;

	if (command == NULL || tokens == NULL || max_tokens < 1) return 0;

	for (s = e = command; ntokens < max_tokens - 1; ++e) {
		if (*e == ' ') {
			if (s != e) {
				tokens[ntokens].value = s;
				tokens[ntokens].length = e - s;
				ntokens++;
				*e = '\0';
			}
			s = e + 1;
		}
		else if (*e == '\0') {
			if (s != e) { 	//the last command token
				tokens[ntokens].value = s;
				tokens[ntokens].length = e - s;
				ntokens++;
			}

			break; /* string end */
		}
	}

	/*
	 * If we scanned the whole string, the terminal value pointer is null,
	 * otherwise it is the first unprocessed character.
	 */
	tokens[ntokens].value =  *e == '\0' ? NULL : e;
	tokens[ntokens].length = 0;
	ntokens++;

	return ntokens;
}

static void
server_free(struct server *s)
{
	if (s == NULL) return;

	if (s->sfd > 0) {
		event_del(&(s->ev));
		close(s->sfd);
	}

	list_free(s->request, 0);
	list_free(s->response, 0);
	free(s);
}

static void
pool_server_handler(const int fd, const short which, void *arg)
{
	struct server *s;
	struct matrix *m;
	char buf[128];
	int toread = 0, toexit = 0, i;

	if (arg == NULL) return;
	s = (struct server *)arg;

	if (!(which & EV_READ)) return;
   
	/* get the byte counts of read */
	if (ioctl(s->sfd, FIONREAD, &toread) || toread == 0) {
		toexit = 1;
	} else {
		if (toread > 128) toread = 128;

		if (0 == read(s->sfd, buf, toread)) toexit = 1;
	}

	if (toexit) {
		if (verbose_mode)
			fprintf(stderr, "%s: (%s.%d) CLOSE POOL SERVER FD %d\n", cur_ts_str, __FILE__, __LINE__, s->sfd);
		event_del(&(s->ev));
		close(s->sfd);

		list_free(s->request, 0);
		list_free(s->response, 0);
		m = s->owner;
		if (m) {
			if (s->pool_idx <= 0) {
				fprintf(stderr, "%s: (%s.%d) POOL SERVER FD %d, IDX %d <= 0\n",
						cur_ts_str, __FILE__, __LINE__, s->sfd, s->pool_idx);
			} else {
				/* remove from list */
				for (i = s->pool_idx; i < m->used; i ++) {
					m->pool[i-1] = m->pool[i];
					m->pool[i-1]->pool_idx = i;
				}
				-- m->used;
			}
		}
		free(s);
	}
}

/* put server connection into keep alive pool */
static void
put_server_into_pool(struct server *s)
{
	struct matrix *m;
	struct server **p;

	if (s == NULL) return;

	if (s->owner == NULL || s->state != SERVER_CONNECTED || s->sfd <= 0) {
		server_free(s);
		return;
	}

	list_free(s->request, 1);
	list_free(s->response, 1);
	s->pos = s->has_response_header = s->remove_trail = 0;

	m = s->owner;
	if (m->size == 0) {
		m->pool = (struct server **) calloc(sizeof(struct server *), STEP);
		if (m->pool == NULL) {
			fprintf(stderr, "%s: (%s.%d) out of memory for pool allocation\n", cur_ts_str, __FILE__, __LINE__);
			m = NULL;
		} else {
			m->size = STEP;
			m->used = 0;
		}
	} else if (m->used == m->size) {
		if (m->size < maxidle) {
			p = (struct server **)realloc(m->pool, sizeof(struct server *)*(m->size + STEP));
			if (p == NULL) {
				fprintf(stderr, "%s: (%s.%d) out of memory for pool reallocation\n", cur_ts_str, __FILE__, __LINE__);
				m = NULL;
			} else {
				m->pool = p;
				m->size += STEP;
			}
		} else {
			m = NULL;
		}
	}

	if (m != NULL) {
		if (verbose_mode)
			fprintf(stderr, "%s: (%s.%d) PUT SERVER FD %d -> POOL\n", cur_ts_str, __FILE__, __LINE__, s->sfd);
		m->pool[m->used ++] = s;
		s->pool_idx = m->used;
		event_del(&(s->ev));

		event_set(&(s->ev), s->sfd, EV_READ|EV_PERSIST, pool_server_handler, (void *) s);
		event_add(&(s->ev), 0);
	} else {
		server_free(s);
	}

}

static void
server_error(conn *c, const char *s)
{
	int i;

	if (c == NULL) return;

	if (c->srv) {
		server_free(c->srv);
		c->srv = NULL;
	}

	if (c->keys) {
		for (i = 0; i < c->keycount; i ++)
			free(c->keys[i]);
		free(c->keys);
		c->keys = NULL;
	}

	c->pos = c->keycount = c->keyidx = 0;
	list_free(c->request, 1);
	list_free(c->response, 1);
	out_string(c, s);
}


/* 
 * zxh added . 
 * time out connect 
 */
static int
socket_connect(struct server *s)
{
	socklen_t servlen;
	int fin_res = 1;
	struct timeval tm;
    fd_set set;
    fd_set rfds;
	int ret = 1;
	int error=-1, len;

	if (s == NULL || s->sfd <= 0 || s->state != SERVER_INIT) return 1;

	servlen = sizeof(s->owner->dstaddr);
	ret == connect(s->sfd, (struct sockaddr *) &(s->owner->dstaddr), servlen);
    if (0 == ret) {     /* ok */
        fin_res = 0;
		s->state = SERVER_CONNECTED;
    } else if (ret < 0 && errno != EINPROGRESS && errno != EALREADY) {    /* error */
        fin_res = 1;
	} else { 			/* connect is in process. */
        do {
            tm.tv_sec  = 0;
            tm.tv_usec = 200000; /* 200ms */
            FD_ZERO(&set);
            FD_SET(s->sfd, &set);
            ret = select(s->sfd+1, NULL, &set, NULL, &tm);

            if (0 == ret) { 	/* time out */
                fin_res = 1;
                break;
            }

            if(ret < 0 && errno != EINTR) { /* error happen */
                fin_res = 1;
                break;
            } else if (ret > 0) {
                len = sizeof(int);
                if(getsockopt(s->sfd, SOL_SOCKET, SO_ERROR, &error, (socklen_t *)&len) < 0) {
                    fin_res = 1;
                    break;
                }

                if(error != 0) { /* error happend in connect */
                    fin_res = 1;
                    break;
                }

                /* the connect is good */
                fin_res = 0;
				s->state = SERVER_CONNECTED;
                break;
            } else { 	/* other error */
                fin_res = 1;
                break;
            }
        } while (1);
    }

    if(0 != fin_res)
    {
        //close(s->sfd);
        dmlog("Cannot Connect the server: !\n");
    } 

    return fin_res;
}



static void
conn_close(conn *c)
{
	int i;

	if (c == NULL) return;
	
	/* check client connection */
	if (c->cfd > 0) {
		if (verbose_mode)
			fprintf(stderr, "%s: (%s.%d) CLOSE CLIENT CONNECTION FD %d\n", cur_ts_str, __FILE__, __LINE__, c->cfd);
		event_del(&(c->ev));
		close(c->cfd);
		curconns --;
		c->cfd = 0;
	}

	server_free(c->srv);

	if (c->keys) {
		for (i = 0; i < c->keycount; i ++) {
			if (c->keys[i]) //zxh added
				free(c->keys[i]);
		}
		free(c->keys);
		c->keys = NULL;
	}

	list_free(c->request, 0);
	list_free(c->response, 0);
	free(c);
}

/* ------------- from lighttpd's network_writev.c ------------ */

#ifndef UIO_MAXIOV
# if defined(__FreeBSD__) || defined(__APPLE__) || defined(__NetBSD__)
/* FreeBSD 4.7 defines it in sys/uio.h only if _KERNEL is specified */
#  define UIO_MAXIOV 1024
# elif defined(__sgi)
/* IRIX 6.5 has sysconf(_SC_IOV_MAX) which might return 512 or bigger */
#  define UIO_MAXIOV 512
# elif defined(__sun)
/* Solaris (and SunOS?) defines IOV_MAX instead */
#  ifndef IOV_MAX
#   define UIO_MAXIOV 16
#  else
#   define UIO_MAXIOV IOV_MAX
#  endif
# elif defined(IOV_MAX)
#  define UIO_MAXIOV IOV_MAX
# else
#  error UIO_MAXIOV nor IOV_MAX are defined
# endif
#endif

//zxh added
#ifndef SSIZE_MAX
	#define SSIZE_MAX 32767
#endif


/* return 0 if success */
static int
writev_list(int fd, list *l)
{
	size_t num_chunks, i, num_bytes = 0, toSend, r, r2;
	struct iovec chunks[UIO_MAXIOV];
	buffer *b;

	if (l == NULL || l->first == NULL || fd <= 0) return 0;

	for (num_chunks = 0, b = l->first; b && num_chunks < UIO_MAXIOV; num_chunks ++, b = b->next) ;

	for (i = 0, b = l->first; i < num_chunks; b = b->next, i ++) {
		if (b->size == 0) {
			num_chunks --;
			i --;
		} else {
			chunks[i].iov_base = b->ptr + b->used;
			toSend = b->size - b->used;

			/* protect the return value of writev() */
			if (toSend > SSIZE_MAX ||
			    (num_bytes + toSend) > SSIZE_MAX) {
				chunks[i].iov_len = SSIZE_MAX - num_bytes;

				num_chunks = i + 1;
				break;
			} else {
				chunks[i].iov_len = toSend;
			}

			num_bytes += toSend;
		}
	}

	if ((r = writev(fd, chunks, num_chunks)) < 0) {
		switch (errno) {
		case EAGAIN:
		case EINTR:
			return 0; /* try again */
			break;
		case EPIPE:
		case ECONNRESET:
			return -2; /* connection closed */
			break;
		default:
			return -1; /* error */
			break;
		}
	}

	r2 = r;

	for (i = 0, b = l->first; i < num_chunks; b = b->next, i ++) {
		if (r >= (ssize_t)chunks[i].iov_len) {
			r -= chunks[i].iov_len;
			b->used += chunks[i].iov_len;
		} else {
			/* partially written */
			b->used += r;
			break;
		}
	}

	remove_finished_buffers(l);
	return r2;
}

/* --------- end here ----------- */

static void
out_string(conn *c, const char *str)
{
	/* append str to c->wbuf */
	int len = 0;
	buffer *b;

	if (c == NULL || str == NULL || str[0] == '\0') return;
	
	len = strlen(str);

	b = buffer_init_size(len + 3, "out_string");
	if (b == NULL) return;

	memcpy(b->ptr, str, len);
	memcpy(b->ptr + len, "\r\n", 2);
	b->size = len + 2;
	b->ptr[b->size] = '\0';
	
	append_buffer_to_list(c->response, b);

	if (writev_list(c->cfd, c->response) >= 0) {
		if (c->response->first && (c->ev_flags != EV_WRITE)) {
			/* update event handler */
			event_del(&(c->ev));
			event_set(&(c->ev), c->cfd, EV_WRITE|EV_PERSIST, drive_client, (void *) c);
			event_add(&(c->ev), 0);
			c->ev_flags = EV_WRITE;
		}
	} else {
		/* client reset/close connection*/
		conn_close(c);
	}
}

/* finish proxy transcation */
static void
finish_transcation(conn *c)
{
	int i;

	if (c == NULL) return;

	if (c->keys) {
		for (i = 0; i < c->keycount; i ++) {
			if (c->keys[i])
				free(c->keys[i]);
		}
		free(c->keys);
		c->keys = NULL;
		c->keycount = c->keyidx = 0;
	}

	c->state = CLIENT_COMMAND;
	list_free(c->request, 1);
}




/* start whole memcache agent transcation */
static void
start_magent_transcation(conn *c)
{
	if (c == NULL) return;

	// zxh deleted.
	//if (c->flag.is_update_cmd  && backupcnt > 0 && c->keycount == 1)
		//start_update_backupserver(c);

	/* start first transaction to normal server */
	do_transcation(c);
}

/* start/repeat memcached proxy transcations */
static void
do_transcation(conn *c)
{
	int idx;
	struct matrix *m;
	struct server *s;
	char *key = NULL;
	buffer *b;

	if (c == NULL) return;

	c->flag.is_backup = 0;
	
	if (c->flag.is_get_cmd) { //是get命令,获取key的值
		if (c->keyidx >= c->keycount) {
			/* end of get transcation */
			finish_transcation(c);
			return;
		}
		key = c->keys[c->keyidx++];
		if (c->keyidx == c->keycount) c->flag.is_last_key = 1;
	} else {
		key = c->keys[0];  //获取key字段的值
	}

	pthread_mutex_lock(&ktm_lock);
	if (0 == ktm_numservers) {
		pthread_mutex_unlock(&ktm_lock);
		if (verbose_mode) fprintf(stderr, "now available server is 0");
		return;
	}

	idx = ketama_get_server(key, ktm_kc);
	pthread_mutex_unlock(&ktm_lock);
	if (idx < 0) {
		/* fall back to round selection */
		idx = hashme(key)%matrixcnt;
	}

	m = matrixs + idx;

	if (m->pool && (m->used > 0)) {
		s = m->pool[--m->used];
		s->pool_idx = 0;
		if (verbose_mode)
			fprintf(stderr, "%s: (%s.%d) GET SERVER FD %d <- POOL\n", cur_ts_str, __FILE__, __LINE__, s->sfd);
	} else {
		s = (struct server *) calloc(sizeof(struct server), 1);
		if (s == NULL) {
			fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
			conn_close(c);
			return;
		}
		s->request = list_init();
		s->response = list_init();
		s->state = SERVER_INIT;
	}
	s->owner = m;
	c->srv = s;

	if (verbose_mode) 
		fprintf(stderr, "%s: (%s.%d) %s KEY \"%s\" -> %s:%ld\n", 
			cur_ts_str, __FILE__, __LINE__, c->flag.is_get_cmd?"GET":"SET", key, m->s.ip, m->s.port);

	if (s->sfd <= 0) {
		s->sfd = socket(AF_INET, SOCK_STREAM, 0); 
		if (s->sfd < 0) {
			fprintf(stderr, "%s: (%s.%d) CAN'T CREATE TCP SOCKET TO MEMCACHED\n", cur_ts_str, __FILE__, __LINE__);
			server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND");
			return;
		}
		fcntl(s->sfd, F_SETFL, fcntl(c->srv->sfd, F_GETFL)|O_NONBLOCK);
		memset(&(s->ev), 0, sizeof(struct event));
	} else {
		event_del(&(c->srv->ev)); /* delete previous pool handler */
		s->state = SERVER_CONNECTED;
	}

	/* reset flags */
	s->has_response_header = 0;
	s->remove_trail = 0;
	s->valuebytes = 0;

	if (c->flag.is_get_cmd) { //若是get命令，创建一个buffer->ptr:"get <key>"，并把buffer添加到s->request队列中
		b = buffer_init_size(strlen(key) + 20, "do_transcation-isgetcmd");
		if (b == NULL) {
			fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
			server_error(c, "SERVER_ERROR OUT OF MEMORY");
			return;
		}
		b->size = snprintf(b->ptr, b->len - 1, "%s %s\r\n", c->flag.is_gets_cmd?"gets":"get", key);
		append_buffer_to_list(s->request, b);
	} else { //若是set等命令，则把c->request命令行队列复制到s->request队列中
		copy_list(c->request, s->request); //set... command. copy client request to backend memcached server.
	}

	c->state = CLIENT_TRANSCATION;
	/* server event handler */

	if (s->state == SERVER_INIT && socket_connect(s)) {
		/*
		if (backupcnt > 0)
			try_backup_server(c);
		else {
			server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER");
				
			// zxh added delete the server reset ketama circle.
		}
		*/
		server_error(c, "SERVER_ERROR CAN NOT CONNECT TO SERVER.");
		dmlog("SERVER_ERROR CAN NOT CONNECT TO SERVER:%s", s->owner->s.ip);
		pthread_mutex_lock(&ktm_lock);
		if(verbose_mode)
			fprintf(stderr, "signal to reset ketama!\n");
		if (ERROR == enqueue(dead_server_q, &(s->owner->s)))
			dmlog("enqueue dead server: %s into dead server queue error!", s->owner->s.ip);

		ketama_mantain_signal = 1;
    	pthread_cond_signal(&ketama_mantain_cond);
		pthread_mutex_unlock(&ktm_lock);
		
		return;
	}

	event_set(&(s->ev), s->sfd, EV_PERSIST|EV_WRITE, drive_memcached_server, (void *)c);
	event_add(&(s->ev), 0);
	s->ev_flags = EV_WRITE;
}


static void
drive_memcached_server(const int fd, const short which, void *arg)
{
	struct server *s;
	conn *c;
	int socket_error, r, toread;
	socklen_t servlen, socket_error_len;

	if (arg == NULL) return;
	c = (conn *)arg;

	s = c->srv;
	if (s == NULL) return;

	if (which & EV_WRITE) {
		switch (s->state) {
		case SERVER_INIT:
			servlen = sizeof(s->owner->dstaddr);
			if (-1 == connect(s->sfd, (struct sockaddr *) &(s->owner->dstaddr), servlen)) {
				if (errno != EINPROGRESS && errno != EALREADY) {
					if (verbose_mode)
						fprintf(stderr, "%s: (%s.%d) CAN'T CONNECT TO MAIN SERVER %s:%ld\n", 
							cur_ts_str, __FILE__, __LINE__, s->owner->s.ip, s->owner->s.port);

					/*
					if (backupcnt > 0)
						try_backup_server(c);
					else
						server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER");
					*/

					dmlog("SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER:%s", s->owner->s.ip);
					server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER");
					return;
				}
			}
			s->state = SERVER_CONNECTING;
			break;

		case SERVER_CONNECTING:
			socket_error_len = sizeof(socket_error);
			/* try to finish the connect() */
			if ((0 != getsockopt(s->sfd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len)) ||
					(socket_error != 0)) {
				if (verbose_mode)
					fprintf(stderr, "%s: (%s.%d) CAN'T CONNECT TO MAIN SERVER %s:%ld\n", 
						cur_ts_str, __FILE__, __LINE__, s->owner->s.ip, s->owner->s.port);

				/*
				if (backupcnt > 0)
					try_backup_server(c);
				else
					server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER");
				*/
				dmlog("SERVER_ERROR CAN NOT CONNECT TO MAIN SERVER:%s", s->owner->s.ip);
				server_error(c, "SERVER_ERROR CAN NOT CONNECT TO BACKEND SERVER");

				//zxh added
				pthread_mutex_lock(&ktm_lock);
				if (verbose_mode) 
					fprintf(stderr, "signal to reset ketama! in %s:%d\n", __FILE__, __LINE__);
				if (ERROR == enqueue(dead_server_q, &(s->owner->s)))
					dmlog("enqueue server: %s into dead server queue error!", s->owner->s.ip);

				ketama_mantain_signal = 1;
    			pthread_cond_signal(&ketama_mantain_cond);
				pthread_mutex_unlock(&ktm_lock);
				// end zxh

				return;
			}
			
			if (verbose_mode)
				fprintf(stderr, "%s: (%s.%d) CONNECTED FD %d <-> %s:%ld\n", 
					cur_ts_str, __FILE__, __LINE__, s->sfd, s->owner->s.ip, s->owner->s.port);

			s->state = SERVER_CONNECTED;
			break;

		case SERVER_CONNECTED:
			/* write request to memcached server */
			r = writev_list(s->sfd, s->request);
			if (r < 0) {
				/* write failed */
				dmlog("SERVER_ERROR CAN NOT WRITE REQUEST TO BACKEND SERVER:%s", s->owner->s.ip);
				server_error(c, "SERVER_ERROR CAN NOT WRITE REQUEST TO BACKEND SERVER");
				return;
			} else {
				if (s->request->first == NULL ) {
					/* finish writing request to memcached server */
					if (c->flag.no_reply) {
						finish_transcation(c);
					} else if (s->ev_flags != EV_READ) {
						event_del(&(s->ev));
						event_set(&(s->ev), s->sfd, EV_PERSIST|EV_READ, drive_memcached_server, arg);
						event_add(&(s->ev), 0);
						s->ev_flags = EV_READ;
					}
				}
			}
			break;

		case SERVER_ERROR:
			server_error(c, "SERVER_ERROR BACKEND SERVER ERROR");
			break;
		}
		return;
	} 
	
	if (!(which & EV_READ)) return;
   
	/* get the byte counts of read */ //从server读取数据,s->stats的值为EV_READ
	if (ioctl(s->sfd, FIONREAD, &toread) || toread == 0) {
		//if (backupcnt > 0)
			//try_backup_server(c);
		//else
		server_error(c, "SERVER_ERROR BACKEND SERVER RESET OR CLOSE CONNECTION");
		return;
	}

	if (c->flag.is_get_cmd) { //是get命令
		if (s->has_response_header == 0) {
			/* NO RESPONSE HEADER */
			if (toread > (BUFFERLEN - s->pos))
				toread = BUFFERLEN - s->pos;
		} else {
			/* HAS RESPONSE HEADER */
			if (toread > (BUFFERLEN - s->pos))
				toread = BUFFERLEN - s->pos;
			if (toread > s->valuebytes)
				toread = s->valuebytes;
		}
	} else {
		if (toread > (BUFFERLEN - s->pos))
			toread = BUFFERLEN - s->pos;
	}

	r = read(s->sfd, s->line + s->pos , toread);
	if (r <= 0) {
		if (r == 0 || (errno != EAGAIN && errno != EINTR)) {
			//if (backupcnt > 0)
			//	try_backup_server(c);
			//else
			server_error(c, "SERVER_ERROR BACKEND SERVER CLOSE CONNECTION");
		}
		return;
	}

	s->pos += r;
	s->line[s->pos] = '\0';

	if (c->flag.is_get_cmd)
		process_get_response(c, r);
	else
		process_update_response(c);
}

static void
process_get_response(conn *c, int r)
{
	struct server *s;
	buffer *b;
	int pos;

	if (c == NULL || c->srv == NULL || c->srv->pos == 0) return;
	s = c->srv;
	if (s->has_response_header == 0) {
		pos = memstr(s->line, "\n", s->pos, 1);

		if (pos == -1) return; /* not found */

		/* found \n */
		s->has_response_header = 1;
		s->remove_trail = 0;
		pos ++;

		s->valuebytes = -1;

		/* VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		 * END\r\n*/
		if (strncasecmp(s->line, "VALUE ", 6) == 0) {
			char *p = NULL;
			p = strchr(s->line + 6, ' ');
			if (p) {
				p = strchr(p + 1, ' ');
				if (p) {
					s->valuebytes = atol(p+1);  //获取返回的bytes数
					//if (s->valuebytes < 0) {
					//	try_backup_server(c); /* conn_close(c); */
					//}
				}
			}
		}

		if (s->valuebytes < 0) { //服务器返回的bytes数小于0，出错
			/* END\r\n or SERVER_ERROR\r\n
			 * just skip this transcation
			 */
			put_server_into_pool(s);
			c->srv = NULL;
			if (c->flag.is_last_key) out_string(c, "END");
			do_transcation(c); /* TO Next KEY */
			return;
		}
		s->valuebytes += 7; /* trailing \r\nEND\r\n */  //返回的值需要添加末尾的end字符

		b = buffer_init_size(pos + 1, "SAVE_VALUE"); //创建buffer,保存VALUE行的值，不包括数据段
		if (b == NULL) {
			if (verbose_mode)
				fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", 
					cur_ts_str, __FILE__, __LINE__);
			//try_backup_server(c); /* conn_close(c); */
			return;
		}
		memcpy(b->ptr, s->line, pos); //把VALUE的结果行字符串复制到buffer中
		b->size = pos;
		append_buffer_to_list(s->response, b);  //把结果buffer复制到server的response链表中

		if (s->pos > pos) { //s->pos是包括数据段的返回数据最后的位置
			memmove(s->line, s->line + pos, s->pos - pos);  //把数据段移动到s->line的开始位置处，覆盖掉原来的返回命令行
			s->pos -= pos;	//s->pos减去pos长度
		} else {
			s->pos = 0;
		}

		if (s->pos > 0) 
			s->valuebytes -= s->pos;
	} else {
		/* HAS RESPONSE HEADER */
		s->valuebytes -= r;
	}

	if (s->remove_trail) {
		s->pos = 0;
	} else if (s->pos > 0) {
		b = buffer_init_size(s->pos+1, "get_response");
		if (b == NULL) {
			fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
			//try_backup_server(c); /* conn_close(c); */
			return;
		}
		memcpy(b->ptr, s->line, s->pos);
		b->size = s->pos;

		if (s->valuebytes <= 5) {
			b->size -= (5 - s->valuebytes); /* remove trailing END\r\n */
			s->remove_trail = 1;
		}
		s->pos = 0;

		append_buffer_to_list(s->response, b);
	}

	if (s->valuebytes == 0) {
		/* GET commands finished, go on next memcached server */
		move_list(s->response, c->response);
		put_server_into_pool(s);
		c->srv = NULL;
		if (c->flag.is_last_key) {
			b = buffer_init_size(6, "get_response-lastkey");
			if (b) {
				memcpy(b->ptr, "END\r\n", 5);
				b->size = 5;
				b->ptr[b->size] = '\0'; 
				append_buffer_to_list(c->response, b);
			} else {
				fprintf(stderr, "%s: (%s.%d) OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
			}
		}

		if (writev_list(c->cfd, c->response) >= 0) {
			if (c->response->first && (c->ev_flags != EV_WRITE)) {
				event_del(&(c->ev));
				event_set(&(c->ev), c->cfd, EV_WRITE|EV_PERSIST, drive_client, (void *) c);
				event_add(&(c->ev), 0);
				c->ev_flags = EV_WRITE;
			}
			do_transcation(c); /* NEXT MEMCACHED SERVER */
		} else {
			/* client reset/close connection*/
			conn_close(c);
		}
	}

}

static void
process_update_response(conn *c)
{
	struct server *s;
	buffer *b;
	int pos;

	if (c == NULL || c->srv == NULL || c->srv->pos == 0) return;
	s = c->srv;
#if 0
	pos = memstr(s->line, "\n", s->pos, 1);
	if (pos == -1) return; /* not found */
#else
	if (s->line[s->pos-1] != '\n') return;
	pos = s->pos - 1;
#endif
	/* found \n */
	pos ++;

	b = buffer_init_size(pos + 1, "update_response");
	if (b == NULL) {
		dmlog("%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
		server_error(c, "SERVER_ERROR OUT OF MEMORY");
		return;
	}
	memcpy(b->ptr, s->line, pos);
	b->size = pos;

	append_buffer_to_list(s->response, b);
	move_list(s->response, c->response);
	put_server_into_pool(s);
	c->srv = NULL;
	if (writev_list(c->cfd, c->response) >= 0) {
		if (c->response->first && (c->ev_flags !=  EV_WRITE)) {
			event_del(&(c->ev));
			event_set(&(c->ev), c->cfd, EV_WRITE|EV_PERSIST, drive_client, (void *) c);
			event_add(&(c->ev), 0);
			c->ev_flags = EV_WRITE;
		}
		finish_transcation(c);
	} else {
		/* client reset/close connection*/
		conn_close(c);
	}
}

static void
drive_backup_server(const int fd, const short which, void *arg)
{
	struct server *s;
	int socket_error, r, toread, pos;
	socklen_t servlen, socket_error_len;

	if (arg == NULL) return;
	s = (struct server *)arg;

	if (which & EV_WRITE) {
		switch (s->state) {
		case SERVER_INIT:
			servlen = sizeof(s->owner->dstaddr);
			if (-1 == connect(s->sfd, (struct sockaddr *) &(s->owner->dstaddr), servlen)) {
				if (errno != EINPROGRESS && errno != EALREADY) {
					if (verbose_mode)
						fprintf(stderr, "%s: (%s.%d) CAN'T CONNECT TO BACKUP SERVER %s:%ld\n", 
							cur_ts_str, __FILE__, __LINE__, s->owner->s.ip, s->owner->s.port);

					server_free(s);
					return;
				}
			}
			s->state = SERVER_CONNECTING;
			break;

		case SERVER_CONNECTING:
			socket_error_len = sizeof(socket_error);
			/* try to finish the connect() */
			if ((0 != getsockopt(s->sfd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len)) ||
					(socket_error != 0)) {
				if (verbose_mode)
					fprintf(stderr, "%s: (%s.%d) CAN'T CONNECT TO BACKUP SERVER %s:%ld\n", 
						cur_ts_str, __FILE__, __LINE__, s->owner->s.ip, s->owner->s.port);

				server_free(s);
				return;
			}
			
			if (verbose_mode)
				fprintf(stderr, "%s: (%s.%d) CONNECTED BACKUP FD %d <-> %s:%ld\n", 
					cur_ts_str, __FILE__, __LINE__, s->sfd, s->owner->s.ip, s->owner->s.port);

			s->state = SERVER_CONNECTED;
			break;

		case SERVER_CONNECTED:
			/* write request to memcached server */
			r = writev_list(s->sfd, s->request);
			if (r < 0) {
				/* write failed */
				server_free(s);
				return;
			} else {
				if (s->request->first == NULL) {
					event_del(&(s->ev));
					event_set(&(s->ev), s->sfd, EV_PERSIST|EV_READ, drive_backup_server, arg);
					event_add(&(s->ev), 0);
				}
			}
			break;

		case SERVER_ERROR:
			server_free(s);
			break;
		}
		return;
	} 
	
	if (!(which & EV_READ)) return;
   
	/* get the byte counts of read */
	if (ioctl(s->sfd, FIONREAD, &toread) || toread == 0) {
		/* ioctl error or memcached server close/reset connection */
		server_free(s);
		return;
	}

	if (toread > (BUFFERLEN - s->pos))
		toread = BUFFERLEN - s->pos;

	r = read(s->sfd, s->line + s->pos , toread);
	if (r <= 0) {
		if (r == 0 || (errno != EAGAIN && errno != EINTR))
			server_free(s);
		return;
	}

	s->pos += r;
	s->line[s->pos] = '\0';

	pos = memstr(s->line, "\n", s->pos, 1);

	if (pos == -1) return; /* not found */

	/* put backup connection into pool */
	put_server_into_pool(s);
}

/* return 1 if command found
 * return 0 if not found
 */
static void
process_command(conn *c)
{
	char *p;
	int len, skip = 0, i, j;
	buffer *b;
	token_t tokens[MAX_TOKENS];
	size_t ntokens;

	if (c->state != CLIENT_COMMAND) return;

	p = strchr(c->line, '\n');
	if (p == NULL) return;

	len = p - c->line;
	*p = '\0'; /* remove \n */ //p此时指向命令行的结尾，注意不包括后面的数据段
	if (*(p-1) == '\r') {
		*(p-1) = '\0'; /* remove \r */
		len --;		//len的长度是命令行的长度，不包括数据
	}

	/* backup command line buffer first */ 	//把不包括数据段的命令行保存到buffer结构中
	b = buffer_init_size(len + 3, "backupcommand");
	memcpy(b->ptr, c->line, len);
	b->ptr[len] = '\r';
	b->ptr[len+1] = '\n';
	b->ptr[len+2] = '\0';
	b->size = len + 2;

#if 0
	if (verbose_mode) {
		fprintf(stderr, "zxhdebug: backupcommand=[%s]\n", b->ptr);
		fprintf(stderr, "%s: (%s.%d) PROCESSING COMMAND: %s", cur_ts_str, __FILE__, __LINE__, b->ptr);
	}
#endif

	memset(&(c->flag), 0, sizeof(c->flag));
	c->flag.is_update_cmd = 1;
	c->storebytes = c->keyidx = 0;

	ntokens = tokenize_command(c->line, tokens, MAX_TOKENS); //把line中的字符串(只是命令行，不包括数据)拆成几段，把位置分别保存在token.value中
	if (ntokens >= 3 && (
			(strcmp(tokens[COMMAND_TOKEN].value, "get") == 0) ||
			(strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0)
			)) {
		/*
		 * get/gets <key>*\r\n
		 *
		 * VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		 * <data block>\r\n
		 * "END\r\n"
		 */
		c->keycount = ntokens - KEY_TOKEN - 1;
		c->keys = (char **) calloc(sizeof(char *), c->keycount);
		if (c->keys == NULL) {
			c->keycount = 0;
			out_string(c, "SERVER_ERROR OUT OF MEMORY");
			skip = 1;
		} else {
			if (ntokens < MAX_TOKENS) { //把line中的命令字段，保存到c->keys中
				for (i = KEY_TOKEN, j = 0; (i < ntokens) && (j < c->keycount); i ++, j ++)
					c->keys[j] = strdup(tokens[i].value);
			} else {
				char *pp, **nn;

				for (i = KEY_TOKEN, j = 0; (i < (MAX_TOKENS-1)) && (j < c->keycount); i ++, j ++)
					c->keys[j] = strdup(tokens[i].value);

				if (tokens[MAX_TOKENS-1].value != NULL) {
					/* check for last TOKEN */
					pp = strtok(tokens[MAX_TOKENS-1].value, " ");

					while(pp != NULL) {
						nn = (char **)realloc(c->keys, (c->keycount + 1)* sizeof(char *));
						if (nn == NULL) {
							/* out of memory */
							break;
						}
						c->keys = nn;
						c->keys[c->keycount] = strdup(pp);
						c->keycount ++;
						pp = strtok(NULL, " ");
					}
				} else {
					/* last key is NULL, set keycount to actual number*/
					c->keycount = j;
				}
			}

			c->flag.is_get_cmd = 1;
			c->keyidx = 0;
			c->flag.is_update_cmd = 0;

			if (strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0)
				c->flag.is_gets_cmd = 1; /* GETS */
		}
	} else if ((ntokens == 4 || ntokens == 5) && (
				(strcmp(tokens[COMMAND_TOKEN].value, "decr") == 0) ||
				(strcmp(tokens[COMMAND_TOKEN].value, "incr") == 0)
				)) {
		/*
		 * incr <key> <value> [noreply]\r\n
		 * decr <key> <value> [noreply]\r\n
		 *
		 * "NOT_FOUND\r\n" to indicate the item with this value was not found 
		 * <value>\r\n , where <value> is the new value of the item's data,
		 */
		c->flag.is_incr_decr_cmd = 1;
	} else if (ntokens >= 3 && ntokens <= 5 && (strcmp(tokens[COMMAND_TOKEN].value, "delete") == 0)) {
		/*
		 * delete <key> [<time>] [noreply]\r\n
		 *
		 * "DELETED\r\n" to indicate success 
		 * "NOT_FOUND\r\n" to indicate that the item with this key was not
		 */
	} else if ((ntokens == 7 || ntokens == 8) && 
			(strcmp(tokens[COMMAND_TOKEN].value, "cas") == 0)) {
		/*
		 * cas <key> <flags> <exptime> <bytes> <cas unqiue> [noreply]\r\n
		 * <data block>\r\n
		 *
		 * "STORED\r\n", to indicate success.  
		 * "NOT_STORED\r\n" to indicate the data was not stored, but not
		 * "EXISTS\r\n" to indicate that the item you are trying to store with
		 * "NOT_FOUND\r\n" to indicate that the item you are trying to store
		 */
		c->flag.is_set_cmd = 1;
		c->storebytes = atol(tokens[BYTES_TOKEN].value);
		c->storebytes += 2; /* \r\n */
	} else if ((ntokens == 6 || ntokens == 7) && (
			(strcmp(tokens[COMMAND_TOKEN].value, "add") == 0) ||
			(strcmp(tokens[COMMAND_TOKEN].value, "set") == 0) ||
			(strcmp(tokens[COMMAND_TOKEN].value, "replace") == 0) ||
			(strcmp(tokens[COMMAND_TOKEN].value, "prepend") == 0) ||
			(strcmp(tokens[COMMAND_TOKEN].value, "append") == 0)
			)) {
		/*
		 * <cmd> <key> <flags> <exptime> <bytes> [noreply]\r\n
		 * <data block>\r\n
		 *
		 * "STORED\r\n", to indicate success.  
		 * "NOT_STORED\r\n" to indicate the data was not stored, but not
		 * "EXISTS\r\n" to indicate that the item you are trying to store with
		 * "NOT_FOUND\r\n" to indicate that the item you are trying to store
		 */
		c->flag.is_set_cmd = 1;
		c->storebytes = atol(tokens[BYTES_TOKEN].value); //获取数据段长度值
		c->storebytes += 2; /* \r\n */ //数据长度字段还包括\r\n这两个字段
	} else if (ntokens >= 2 && (strcmp(tokens[COMMAND_TOKEN].value, "stats") == 0)) {
		/* END\r\n
		 */
		/*
		char tmp[128];
		out_string(c, "memcached agent v" VERSION);
		for (i = 0; i < matrixcnt; i ++) {
			snprintf(tmp, 127, "matrix %d -> %s:%ld, pool size %d", 
					i+1, matrixs[i].s.ip, matrixs[i].s.port, matrixs[i].used);
			out_string(c, tmp);
		}
		*/
		char tmp[512];

		//zxh added static variable. 2014-03-17
		snprintf(tmp, sizeof(tmp), "current total connects: %d\n", curconns);
		out_string(c, tmp);

		// added buffer statistic
		snprintf(tmp, sizeof(tmp), "buffers: %d, buffersize=%d\n", total_buffers, total_buffer_size);
		out_string(c, tmp);
		// end zxh

		snprintf(tmp, sizeof(tmp), "total servers: %d\n current available servers: %d", 
			total_ktm_numservers, ktm_numservers);
		out_string(c, tmp);

		for (i = 0; i < total_ktm_numservers; i++) {
			if (NULL != *(ktm_slist+i)) {
				snprintf(tmp, sizeof(tmp), "ip:port->%s:%d, weight:%ld", 
					(*(ktm_slist+i))->ip, (*(ktm_slist+i))->port, (*(ktm_slist+i))->weight);
				out_string(c, tmp);
			}
		}
		
		out_string(c, "END");
		skip = 1;
	} else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "quit") == 0)) {
		conn_close(c);
		// zxh added for bug: memory leak. 2014-03-19
		buffer_free(b, "backupcommand");
		// end zxh
		return;
	} else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "version") == 0)) {
		out_string(c, "VERSION memcached agent v" VERSION);
		skip = 1;
	} else {
		out_string(c, "UNSUPPORTED COMMAND");
		skip = 1;
	}

	/* finish process commands */
	if (skip == 0) {
		/* append buffer to list */
		append_buffer_to_list(c->request, b); //把命令行buffer复制到c->request中

		if (c->flag.is_get_cmd == 0) { //非get命令
			if (tokens[ntokens-2].value && strcmp(tokens[ntokens-2].value, "noreply") == 0)
				c->flag.no_reply = 1;
			c->keycount = 1;
			c->keys = (char **) calloc(sizeof(char *), 1);
			if (c->keys == NULL) {
				fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
				dmlog("%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
				conn_close(c);
				return;
			}
			c->keys[0] = strdup(tokens[KEY_TOKEN].value);
		}
	} else {
		buffer_free(b, "backupcommand");
	}

	i = p - c->line + 1;  //这里是i计算出来的是命令行字符串的长度
	if (i < c->pos) { //c->pos是整个接收的数据包的长度最后位置
		memmove(c->line, p+1, c->pos - i); //把数据段移动到c->line的开始位置
		c->pos -= i;	//把c->pos的值移动到数据段的开始位置,若没有数据段,此时c->pos的值是数据段的长度
	} else {
		c->pos = 0;		//若没有数据段，则为0
	}

	if (c->storebytes > 0) {  //是set命令
		if (c->pos > 0) { //此时pos指向数据段的开始位置，若存在数据段
			/* append more buffer to list */ //data content and data length
			b = buffer_init_size(c->pos + 1, "setcmd_datalen");  //创建buffer结构,长度是数据段的长度
			if (b == NULL) {
				fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
				conn_close(c);
				return;
			}
			memcpy(b->ptr, c->line, c->pos);  //把数据段的数据复制到buffer的ptr中
			b->size = c->pos;
			c->storebytes -= b->size;		// 存储数据长度减去c->pos，若数据已经读完了，storebytes应该为0
			append_buffer_to_list(c->request, b); //把数据段buffer复制到c->request中
			c->pos = 0;						//c->pos指向0的位置
		}
		if (c->storebytes > 0) 			//data is not complete, have some data to read.
			c->state = CLIENT_NREAD;
		else
			start_magent_transcation(c);  //数据和命令都已经处理完毕，开始传输命令和数据
	} else { //若是get命令,且没有出错
		if (skip == 0)
			start_magent_transcation(c);
	}
}

/* drive machine of client connection */
static void
drive_client(const int fd, const short which, void *arg)
{
	conn *c;
	int r, toread;
	buffer *b;


	c = (conn *)arg;
	if (c == NULL) return;

	if (which & EV_READ) {
		/* get the byte counts of read */
		if (ioctl(c->cfd, FIONREAD, &toread) || toread == 0) {
			conn_close(c);
			return;
		}

		switch(c->state) {
		case CLIENT_TRANSCATION:
		case CLIENT_COMMAND:
			r = BUFFERLEN - c->pos;
			if (r > toread) r = toread;

			toread = read(c->cfd, c->line + c->pos, r);
			if ((toread <= 0) && (errno != EINTR && errno != EAGAIN))  {
				conn_close(c);
				return;
			}
			c->pos += toread;
			c->line[c->pos] = '\0';
			process_command(c);
			break;
		case CLIENT_NREAD:
			/* we are going to read */
			if (c->flag.is_set_cmd == 0) {
				fprintf(stderr, "%s: (%s.%d) WRONG STATE, SHOULD BE SET COMMAND\n", cur_ts_str, __FILE__, __LINE__);
				conn_close(c);
				return;
			}

			if (toread > c->storebytes) toread = c->storebytes;

			b = buffer_init_size(toread + 1, "drive_client_NREAD");
			if (b == NULL) {
				fprintf(stderr, "%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
				dmlog("%s: (%s.%d) SERVER OUT OF MEMORY\n", cur_ts_str, __FILE__, __LINE__);
				conn_close(c);
				return;
			}

			r = read(c->cfd, b->ptr, toread);
			if ((r <= 0) && (errno != EINTR && errno != EAGAIN))  {
				buffer_free(b, "drive client read");
				conn_close(c);
				return;
			}
			b->size = r;
			b->ptr[r] = '\0';
			/* append buffer to list */
			append_buffer_to_list(c->request, b);
			c->storebytes -= r;
			if (c->storebytes <= 0)
				start_magent_transcation(c);
			break;
		}
	} else if (which & EV_WRITE) {
		/* write to client */
		r = writev_list(c->cfd, c->response);
		if (r < 0) {
			conn_close(c);
			return;
		}

		if (c->response->first == NULL) {
			/* finish writing buffer to client
			 * switch back to reading from client
			 */
			event_del(&(c->ev));
			event_set(&(c->ev), c->cfd, EV_READ|EV_PERSIST, drive_client, (void *) c);
			event_add(&(c->ev), 0);
			c->ev_flags = EV_READ;
		}
	}
}

static void
server_accept(const int fd, const short which, void *arg)
{
	conn *c = NULL;
	int newfd, flags = 1;
	struct sockaddr_in s_in;
	socklen_t len = sizeof(s_in);
	struct linger ling = {0, 0};

	UNUSED(arg);
	UNUSED(which);

	memset(&s_in, 0, len);

	newfd = accept(fd, (struct sockaddr *) &s_in, &len);
	if (newfd < 0) {
		fprintf(stderr, "%s: (%s.%d) ACCEPT() FAILED\n", cur_ts_str, __FILE__, __LINE__);
		dmlog("%s: (%s.%d) ACCEPT() FAILED\n", cur_ts_str, __FILE__, __LINE__);
		return ;
	}

	// zxh added for if no host is alive, return immediately. 2014/3/12
	if (ktm_numservers <= 0) {
		dmlog("Alert: all memcached cluster hosts are down.\n");
		write(newfd, NONEALIVE, sizeof(NONEALIVE));
		close(newfd);
		return;
	}
	// end zxh

	if (curconns >= maxconns) {
		/* out of connections */
		write(newfd, OUTOFCONN, sizeof(OUTOFCONN));
		close(newfd);
		return;
	}

	c = (struct conn *) calloc(sizeof(struct conn), 1);
	if (c == NULL) {
		fprintf(stderr, "%s: (%s.%d) OUT OF MEMORY FOR NEW CONNECTION\n", cur_ts_str, __FILE__, __LINE__);
		dmlog("%s: (%s.%d) OUT OF MEMORY FOR NEW CONNECTION\n", cur_ts_str, __FILE__, __LINE__);
		close(newfd);
		return;
	}
	c->request = list_init();
	c->response = list_init();
	c->cfd = newfd;
	curconns ++;

	if (verbose_mode)
		fprintf(stderr, "%s: (%s.%d) NEW CLIENT FD %d\n", cur_ts_str, __FILE__, __LINE__, c->cfd);

	fcntl(c->cfd, F_SETFL, fcntl(c->cfd, F_GETFL)|O_NONBLOCK);
	setsockopt(c->cfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
	setsockopt(c->cfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
	setsockopt(c->cfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));

	/* setup client event handler */
	memset(&(c->ev), 0, sizeof(struct event));
	event_set(&(c->ev), c->cfd, EV_READ|EV_PERSIST, drive_client, (void *) c);
	event_add(&(c->ev), 0);
	c->ev_flags = EV_READ;
	
	return;
}

static void
free_matrix(matrix *m)
{
	int i;
	struct server *s;

	if (m == NULL) return;

	for (i = 0; i < m->used; i ++) {
		s = m->pool[i];
		if (s->sfd > 0) close(s->sfd);
		list_free(s->request, 0);
		list_free(s->response, 0);
		free(s);
	}

	free(m->pool);
}

static void
server_exit(int sig)
{
	int i;

	if (verbose_mode)
		fprintf(stderr, "\nexiting\n");

	UNUSED(sig);

	if (sockfd > 0) close(sockfd);
	if (unixfd > 0) close(unixfd);

	//free_ketama(ketama);
	//free_ketama(backupkt);

	for (i = 0; i < matrixcnt; i ++) {
		free_matrix(matrixs + i);
	}

	free(matrixs);

	for (i = 0; i < backupcnt; i ++) {
		free_matrix(backups + i);
	}

	free(backups);

	exit(0);
}

static void
server_socket_unix(void)
{
	struct linger ling = {0, 0};
	struct sockaddr_un addr;
	struct stat tstat;
	int flags = 1;
	int old_umask;

	if (socketpath == NULL)
		return ;

	if ((unixfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
		fprintf(stderr, "%s: (%s.%d) CAN NOT CREATE UNIX DOMAIN SOCKET", cur_ts_str, __FILE__, __LINE__);
		return ;
	}

	fcntl(unixfd, F_SETFL, fcntl(unixfd, F_GETFL, 0) | O_NONBLOCK);

	/*
	 * Clean up a previous socket file if we left it around
	 */
	if (lstat(socketpath, &tstat) == 0) {
		if (S_ISSOCK(tstat.st_mode))
			unlink(socketpath);
	}

	setsockopt(unixfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
	setsockopt(unixfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
	setsockopt(unixfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

	/*
	 * the memset call clears nonstandard fields in some impementations
	 * that otherwise mess things up.
	 */
	memset(&addr, 0, sizeof(addr));

	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, socketpath);
	old_umask=umask( ~(0644&0777));
	if (bind(unixfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
		fprintf(stderr, "%s: (%s.%d) bind errno = %d: %s\n", cur_ts_str, __FILE__, __LINE__, errno, strerror(errno));
		close(unixfd);
		unixfd = -1;
		umask(old_umask);
		return;
	}

	umask(old_umask);

	if (listen(unixfd, 512) == -1) {
		fprintf(stderr, "%s: (%s.%d) listen errno = %d: %s\n", cur_ts_str, __FILE__, __LINE__, errno, strerror(errno));
		close(unixfd);
		unixfd = -1;
	}
}

static void
timer_service(const int fd, short which, void *arg)
{
	struct timeval tv;
	
	cur_ts = time(NULL);
	strftime(cur_ts_str, 127, "%Y-%m-%d %H:%M:%S", localtime(&cur_ts));
	
	tv.tv_sec = 1; tv.tv_usec = 0; /* check for every 1 seconds */
	event_add(&ev_timer, &tv);
}


/*
 * zxh added.
 * read server list. 
 * format: ip:port weight
 */
static int read_server_list(char *line)
{
	assert(line != NULL);
	struct matrix *m;
	char *ps = line;
	char *pv = NULL;

	/* for magent matrix information 
     * This is basic server information, and these server information will not changed.
     */
	if (0 == matrixcnt) {
		matrixs = (struct matrix *) calloc(1, sizeof(struct matrix));
		if (matrixs == NULL) {
			fprintf(stderr, "out of memory for %s:%d\n", __FILE__,__LINE__);
			return FAIL;
		}
		m = matrixs;
		matrixcnt = 1;
	} else  {
		m = (struct matrix *)realloc(matrixs, sizeof(struct matrix)*(matrixcnt+1));
		if (m == NULL) {
			fprintf(stderr, "out of memory for %s\n", optarg);
			exit(1);
		}
		matrixs = m;
		m = matrixs + matrixcnt;
		matrixcnt ++;
	}

	dm_ltrim(ps, "\t ");
	pv = strchr(ps, ':');	
	if (NULL == pv)
		goto error;

	// get ip and port
	*pv++ = '\0';
	strncpy(m->s.ip, ps, IP_LEN);
	ps = pv;
	pv = strpbrk(ps, " \t");
	if (NULL == pv) goto error;
	*pv++ =  '\0';
	m->s.port = atoi(ps);
	if (m->s.port <= 0) m->s.port = DEFAULT_PORT;

	// add weight 
	dm_ltrim(pv, " \t");
	m->s.weight = atoi(pv);
	if (m->s.weight <= 0) m->s.weight = 10; //default weight
	m->s.is_alive = TRUE;

	// for socket
	m->dstaddr.sin_family = AF_INET;
	m->dstaddr.sin_addr.s_addr = inet_addr(m->s.ip);
	m->dstaddr.sin_port = htons(m->s.port);

	// server index in matrixs server list.
	m->s.idx = matrixcnt - 1;

	return OK;
error:
	if (matrixs) free(matrixs);
	if (ktm_slist) free(ktm_slist);
	return FAIL;	
}

static int creat_ktm_slist(struct matrix *m, int nmt)
{
	int i = 0;

	if (!m || !nmt)
		return FAIL;

	ktm_slist = (serverinfo **) calloc(nmt, sizeof(serverinfo *));
	if (ktm_slist == NULL) {
		fprintf(stderr, "out of memory for %s\n", optarg);
		return FAIL;
	}
	
	for (i = 0; i < nmt; i++) {
		// add ketama weight
		ktm_slist[ktm_numservers] = &(m[i].s);
		ktm_memory += ktm_slist[ktm_numservers]->weight;
		ktm_numservers ++;
		if(verbose_mode)
			fprintf(stderr, "add server ip=[%s], port=[%d], weight=[%d]\n", m[i].s.ip, m[i].s.port,m[i].s.weight);
	}
	
	total_ktm_numservers = ktm_numservers;
	return 0;
}


/* 
 * delete prev chars before real value.
 */
static void  
dm_ltrim(register char *str, const char *charlist)
{
    register char *p;

    if( !str || !charlist || !*str || !*charlist ) return;

    for( p = str; *p && NULL != strchr(charlist,*p); p++ );

    if( p == str )  return;
    
    while( *p ) 
    {    
        *str = *p;
        str++;
        p++; 
    }    

    *str = '\0';
}


/*
 * remove the right charlist in the string: str.
 * 
 */
static void 
dm_rtrim(char *str, const char *charlist)
{
    register char *p;

    if( !str || !charlist || !*str || !*charlist ) return;

    for( 
        p = str + strlen(str) - 1; 
        p >= str && NULL != strchr(charlist,*p);
        p--) 
            *p = '\0';
}



/* 
 * zxh added for read configure file
 */
static int
read_config(char *filename)
{
	#define LLEN 128
	assert(filename != NULL);
	char buffer[LLEN];
	FILE *fp;
	char *ps = NULL;
	read_config_state rst=READ_OTHERS;
	
	if (0 != access(filename, R_OK)) {
		fprintf(stderr, "file : %s not exist or permission is denied\n", filename);
		exit(1);
	}
	
	fp = fopen(filename, "r");
	if (!fp) {
		fprintf(stderr, "open file: %s error!\n", filename);
		exit(1);
	}
	while (fgets(buffer, LLEN, fp)) {
		buffer[LLEN-1] = '\0';
		if (buffer[0] == '#') continue; //comment
		ps = buffer;
		dm_ltrim(ps, "\t \n#");
		if (*ps == '#') continue;		//comment
		dm_rtrim(ps, "\t\n ");

		if (*ps == '[') {
			if (!strncmp(ps,"[server]",strlen("[server]"))) {
				rst = READ_SERVER;
				continue;
			}
			if (!strncmp(ps,"[backup]",strlen("[backup]"))) {
				rst = READ_BACKUP;
				continue;
			}
		}
	
		/* read content */
		switch(rst) {
		case READ_SERVER:
			if (FAIL == read_server_list(buffer))
				goto fmt_error;
			break;
		default:
			fprintf(stderr, "read other list!\n");
			break;
		}
	}
	if (fp)	 fclose(fp);
	return OK;

fmt_error:
	if (fp)	 fclose(fp);
	return FAIL;	
}


static void 
daemonize(void) {
    int fd;

    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If Redis is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }    
}

int
main(int argc, char **argv)
{
	char *bindhost = NULL;
	int uid, gid, todaemon = 1, flags = 1, c;
	struct sockaddr_in server;
	struct linger ling = {0, 0};
	struct timeval tv;
	
	while(-1 != (c = getopt(argc, argv, "p:u:g:s:Dhvn:l:f:i:c:"))) {
		switch (c) {
		case 'u':
			uid = atoi(optarg);
			if (uid > 0) {
				setuid(uid);
				seteuid(uid);
			}
			break;
		case 'g':
			gid = atoi(optarg);
			if (gid > 0) {
				setgid(gid);
				setegid(gid);
			}
			break;
		case 'v':
			verbose_mode = 1;
			todaemon = 0;
			break;
		case 'D':
			todaemon = 0;
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'i':
			maxidle = atoi(optarg);
			if (maxidle <= 0) maxidle = 20;
			break;
		case 'n':
			maxconns = atoi(optarg);
			if (maxconns <= 0) maxconns = 4096;
			break;
		case 'f':
			socketpath = optarg;
			break;
		case 'l':
			bindhost = optarg;
			break;
		case 'c':
			if (FAIL == read_config(optarg)) {
				fprintf(stderr, "read configure file error, please check configure file format.\n");
				exit(1);
			}

			if (FAIL == creat_ktm_slist(matrixs, matrixcnt)) {
				fprintf(stderr, "create ketama server list error!\n");
				exit(1);
			}
			break;

		case 'h':
		default:
			show_help();
			return 1;
		}
	}

	// zxh added ignore SIGPIPE signal. 2014-4-30
	signal(SIGPIPE, SIG_IGN);

	signal(SIGTERM, server_exit);
	signal(SIGINT, server_exit);
	signal(SIGSEGV, &dump);

	if (matrixcnt == 0) {
		fprintf(stderr, "please provide -s \"ip:port\" argument\n\n");
		show_help();
		exit(1);
	}

	if (port == 0 && socketpath == NULL) {
		fprintf(stderr, "magent must listen on tcp or unix domain socket\n");
		exit(1);
	}

	//if (todaemon && daemon(0, 0) == -1) {
	if (todaemon)
		daemonize();

	cur_ts = time(NULL);
	strftime(cur_ts_str, 127, "%Y-%m-%d %H:%M:%S", localtime(&cur_ts));
	dmlog("server start: %s", cur_ts_str);

	if (use_ketama) {
		// zxh added for use ketama algorithm.
		pthread_mutex_lock(&ktm_lock);
		dead_server_q = create_queue(matrixcnt);
		if (NULL == dead_server_q) {
			fprintf(stderr, "init dead server queue error!\n");
			exit(1);
		}

		if (OK != ketama_reset(&ktm_kc, ktm_slist, ktm_numservers, ktm_memory)) {
			fprintf(stderr, "init ketama error!\n");
			exit(1);
		}
		pthread_mutex_unlock(&ktm_lock);
		// end zxh
	}

	if (port > 0) {
		sockfd = socket(AF_INET, SOCK_STREAM, 0);
		if (sockfd < 0) {
			fprintf(stderr, "CAN'T CREATE NETWORK SOCKET\n");
			return 1;
		}

		fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL)|O_NONBLOCK);

		setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
		setsockopt(sockfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
		setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));

		memset((char *) &server, 0, sizeof(server));
		server.sin_family = AF_INET;
		if (bindhost == NULL)
			server.sin_addr.s_addr = htonl(INADDR_ANY);
		else
			server.sin_addr.s_addr = inet_addr(bindhost);

		server.sin_port = htons(port);

		if (bind(sockfd, (struct sockaddr *) &server, sizeof(server))) {
			if (errno != EINTR) 
				fprintf(stderr, "bind errno = %d: %s\n", errno, strerror(errno));
			close(sockfd);
			exit(1);
		}

		if (listen(sockfd, 512)) {
			fprintf(stderr, "listen errno = %d: %s\n", errno, strerror(errno));
			close(sockfd);
			exit(1);
		}

	}


	if (socketpath) 
		server_socket_unix();


	pthread_t ptid;
	pthread_t add_pid;
	// create maintain ketama server list thread
	pthread_create(&ptid, NULL, (void *)&maintain_ketama_srv_thread, NULL);
	// maintain server list when server restart
	pthread_create(&add_pid, NULL, (void *)&maintain_add_srv_thread, NULL);

	event_init();

	if (sockfd > 0) {
		if (verbose_mode)
			fprintf(stderr, "memcached agent listen at port %d\n", port);
		event_set(&ev_master, sockfd, EV_READ|EV_PERSIST, server_accept, NULL);
		event_add(&ev_master, 0);
	}

	if (unixfd > 0) {
		if (verbose_mode)
			fprintf(stderr, "memcached agent listen at unix domain socket \"%s\"\n", socketpath);
		event_set(&ev_unix, unixfd, EV_READ|EV_PERSIST, server_accept, NULL);
		event_add(&ev_unix, 0);
	}

	evtimer_set(&ev_timer, timer_service, NULL);
	tv.tv_sec = 1; tv.tv_usec = 0; /* check for every 1 seconds */
	event_add(&ev_timer, &tv);

	event_loop(0);
	server_exit(0);
	return 0;
}


/* 
 * dump the stack when recieved signo 
 */
static void dump(int signo)
{
    void *array[10];
    size_t size;
    char **strings;
    size_t i;

    size = backtrace (array, 10);
    strings = backtrace_symbols (array, size);

    fprintf(stderr, "Obtained %zd stack frames.\n", size);

    for (i = 0; i < size; i++)
            fprintf(stderr, "%s\n", strings[i]);

    free (strings);

    exit(0);
}

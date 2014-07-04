#ifndef  _DMAGENT_H_INCLUDED_
#define  _DMAGENT_H_INCLUDED_

#define VERSION "0.6"

#define OUTOFCONN "SERVER_ERROR OUT OF CONNECTION"
#define NONEALIVE "NO SERVERS ARE ALIVE."

#define BUFFERLEN 2048
#define MAX_TOKENS 8
#define COMMAND_TOKEN 0
#define KEY_TOKEN 1
#define BYTES_TOKEN 4
#define KEY_MAX_LENGTH 250
#define BUFFER_PIECE_SIZE 16

#define UNUSED(x) ( (void)(x) )
#define STEP 5

//zxh added 
#define DEFAULT_PORT 11211
#define CONN_CACHE_SIZE 1024
// end zxh

/* structure definitions */
typedef struct conn conn;
typedef struct matrix matrix;
typedef struct list list;
typedef struct buffer buffer;
typedef struct server server;

// zxh added
static void  dm_ltrim(register char *str, const char *charlist);

// zxhadded
typedef enum {
	READ_SERVER,
	READ_BACKUP,
	READ_OTHERS
} read_config_state;

typedef enum {
	CLIENT_COMMAND,
	CLIENT_NREAD, /* MORE CLIENT DATA */
	CLIENT_TRANSCATION
} client_state_t;

typedef enum {
	SERVER_INIT,
	SERVER_CONNECTING,
	SERVER_CONNECTED,
	SERVER_ERROR
} server_state_t;

struct buffer {
	char *ptr;

	size_t used;
	size_t size;
	size_t len; /* ptr length */

	//zxh added
	char bname[32];
	// end zxh

	struct buffer *next;
};

/* list to buffers */
struct list {
	buffer *first;
	buffer *last;
};

/* connection to memcached server */
struct server {
	int sfd;
	server_state_t state;
	struct event ev;
	int ev_flags;
	
	matrix *owner;

	/* first response line
	 * NOT_FOUND\r\n
	 * STORED\r\n
	 */
	char line[BUFFERLEN];
	int pos;

	/* get/gets key ....
	 * VALUE <key> <flags> <bytes> [<cas unique>]\r\n 
	 */
	int valuebytes;
	int has_response_header:1;
	int remove_trail:1;

	/* input buffer */
	list *request;
	/* output buffer */
	list *response;

	int pool_idx;
};

struct conn {
	/* client part */
	int cfd;
	client_state_t state;
	struct event ev;
	int ev_flags;

	/* command buffer */
	char line[BUFFERLEN+1];
	int pos;

	int storebytes; /* bytes stored by CAS/SET/ADD/... command */

	struct flag {
		unsigned int is_get_cmd:1;
		unsigned int is_gets_cmd:1;
		unsigned int is_set_cmd:1;
		unsigned int is_incr_decr_cmd:1;
		unsigned int no_reply:1;
		unsigned int is_update_cmd:1;
		unsigned int is_backup:1;
		unsigned int is_last_key:1;
	} flag;

	int keycount; /* GET/GETS multi keys */
	int keyidx;
	char **keys;

	/* input buffer */
	list *request;
	/* output buffer */
	list *response;

	struct server *srv;
};

/* memcached server structure */
struct matrix {
	/*
	char ip[IP_LEN];
	int  weight;
	int port;
	*/
	serverinfo s;
	struct sockaddr_in dstaddr;

	int size;
	int used;
	struct server **pool;
};

typedef struct token_s {
	char *value;
	size_t length;
} token_t;

#endif

/*
 * log.c
 * 	hover added 2013-11-20
 * 
 */
#include "log.h"
#include "common.h"

static pthread_mutex_t  log_lock = PTHREAD_MUTEX_INITIALIZER; 
//static int MAX_LOG_FILE_SIZE = 2097152;  /* 2M */
static int MAX_LOG_FILE_SIZE = 1024;  /* 2M */

/* log file managment structure. */
static log_man_t *lgm;


FILE *get_cur_fp(void)
{
	struct stat fst;

	if (NULL == lgm) { 	//init status
		lgm = (log_man_t *)calloc(1, sizeof(log_man_t));
		if (lgm == NULL) {
			fprintf(stderr, "init: calloc log file fp error: %s\n", strerror(errno));
			return NULL;
		}
		lgm->path = DEF_LOG_FILE;
		lgm->fileid = DEF_LOG_ID;

		lgm->fp = fopen(lgm->path, "w+");
		if (NULL == lgm->fp) {
			fprintf(stderr, "open log file: %s error: %s", lgm->path, strerror(errno));
		}
	}

	/* stat */
	if (stat(lgm->path, &fst) < 0) {
		if (NULL != lgm->fp) {
			return lgm->fp; 		/* return current FILE. */
		} else {
			lgm->path = DEF_LOG_FILE;
			lgm->fileid = DEF_LOG_ID;
		}
	} else {
		if (fst.st_size >= MAX_LOG_FILE_SIZE) { 	/* log file size > 2M */
			if (lgm->fp) {
				fclose(lgm->fp);					/* close old log file. */
				lgm->fp = NULL;
			}

			if (lgm->fileid == DEF_LOG_ID) { 		/* current is defualt log file, now change it to backup log file. */
				lgm->path = BAK_LOG_FILE;
				lgm->fileid = BAK_LOG_ID;
			} else if(lgm->fileid == BAK_LOG_ID) {
				lgm->path = DEF_LOG_FILE;
				lgm->fileid = DEF_LOG_ID;
			}

			lgm->fp = fopen(lgm->path, "w+");
			if (NULL == lgm->fp) {
				fprintf(stderr, "open log file: %s error: %s", lgm->path, strerror(errno));
			}
		} 
	}

	return lgm->fp;
}

//void dmlog(int level, const char *fmt, ...) {
void dmlog(const char *fmt, ...) 
{
    va_list ap;
    char msg[DM_MAX_LOGMSG_LEN];

    //if ((level&0xff) < server.verbosity) return;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap); 
    va_end(ap);

    //dm_log_raw(level,msg);
    dm_log_raw(LOG_ERR,msg);
}


/* Low level logging. To use only for very big messages, otherwise
 * redisLog() is to prefer. */
void dm_log_raw(int level, const char *msg) {
    const char *c = ".-*#";
    char buf[64];
    int off;
    struct timeval tv;
	FILE *fp = NULL;

	/*
	if (gfp == NULL) {
		gfp = fopen(DEF_LOG_FILE, "a+");
    	if (!gfp) return;
	}
	*/
	
    gettimeofday(&tv,NULL);
    off = strftime(buf,sizeof(buf),"%Y-%m-%d %H:%M:%S.",localtime(&tv.tv_sec));
    snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
	pthread_mutex_lock(&log_lock);
	fp = get_cur_fp();
	if (!fp) {
		fprintf(stderr, "open log file error!\n");
		goto end;
	}
	
    fprintf(fp,"[%d] %s %c %s\n", (int)getpid(), buf, c[level], msg);
    fflush(fp);

end:
	pthread_mutex_unlock(&log_lock);
}

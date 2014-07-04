#ifndef _DM_LOG_H_
#define _DM_LOG_H_

#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>


#define LOG_EMERG   0   /* system is unusable */
#define LOG_ALERT   1   /* action must be taken immediately */
#define LOG_CRIT    2   /* critical conditions */
#define LOG_ERR     3   /* error conditions */
#define LOG_WARNING 4   /* warning conditions */
#define LOG_NOTICE  5   /* normal but significant condition */
#define LOG_INFO    6   /* informational */
#define LOG_DEBUG   7   /* debug-level messages */


#define DM_MAX_LOGMSG_LEN 1024
#define DEF_LOG_FILE "/tmp/dmagent.log"
#define BAK_LOG_FILE "/tmp/dmagent1.log"

#define DM_LOG_RAW (1<<10) 

/* log file id */
#define ERR_LOG_ID -1
#define DEF_LOG_ID 0
#define BAK_LOG_ID 1

typedef struct log_man {
	FILE *fp;
	char *path;	
	int fileid;
}log_man_t;



void dmlog(const char *fmt, ...);
void dm_log_raw(int level, const char *msg);

#endif



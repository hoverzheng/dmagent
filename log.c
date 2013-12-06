/*
 * log.c
 * 	hover added 2013-11-20
 * 
 */

#include "log.h"

static  FILE *gfp = NULL;


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
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#";
    char buf[64];
    int rawmode = (level & DM_LOG_RAW);

    //level &= 0xff; /* clear flags */
    //if (level < server.verbosity) return;

    //fp = (server.logfile == NULL) ? stdout : fopen(server.logfile,"a");
	if (gfp == NULL) {
		gfp = fopen(DEF_LOG_FILE, "a+");
    	if (!gfp) return;
	}

    if (rawmode) {
        fprintf(gfp,"%s",msg);
    } else {
        int off;
        struct timeval tv;

        gettimeofday(&tv,NULL);
        //off = strftime(buf,sizeof(buf),"%d %b %H:%M:%S.",localtime(&tv.tv_sec));
        off = strftime(buf,sizeof(buf),"%Y-%m-%d %H:%M:%S.",localtime(&tv.tv_sec));
        snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
        fprintf(gfp,"[%d] %s %c %s\n",(int)getpid(),buf,c[level],msg);
    }
    fflush(gfp);
}

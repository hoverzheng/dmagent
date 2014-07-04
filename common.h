#ifndef _COMMON_H_INCLUDED__
#define _COMMON_H_INCLUDED__

#include <google/tcmalloc.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


#define OK 0
#define FAIL -1
#define ERROR -1

#define malloc(size) tc_malloc((size)) 
#define calloc(count,size) tc_calloc((count),(size))
#define realloc(ptr,size) tc_realloc((ptr),(size))
#define free(ptr) tc_free((ptr))

#endif

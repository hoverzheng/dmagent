/*
    Copyright (C) 2007 by                                          
       Christian Muehlhaeuser <chris@last.fm>
       Richard Jones <rj@last.fm>

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; version 2 only.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/
#ifndef KETAMA_H_
#define KETAMA_H_

#include <sys/sem.h>    /* semaphore functions and structs. */
#include <math.h>

//#ifdef __cplusplus  extern "C"{ #endif

#define MC_SHMSIZE  524288  // 512KB should be ample.

#define IP_LEN 32
#define OK 0
#define FAIL -1


#define TRUE 1
#define FALSE 0

union semun
{
    int val;              /* used for SETVAL only */
    struct semid_ds *buf; /* for IPC_STAT and IPC_SET */
    ushort *array;        /* used for GETALL and SETALL */
};

typedef int (*compfn)( const void*, const void* );

typedef struct
{
        char ip[IP_LEN];
		unsigned int srvid;	 // zxh added server id in array
        unsigned int point;  // point on circle
} mcs;

typedef struct
{
    char ip[IP_LEN];
    unsigned long weight;
	unsigned long port;
	unsigned int is_alive;
	unsigned int idx;	
    //unsigned long memory;
} serverinfo;

typedef struct
{
		unsigned int nsrv;	
        int numpoints;
        void* modtime;
        //void* array; //array of mcs structs
        mcs* array; //array of mcs structs
} continuum;

typedef continuum* ketama_continuum;


/** \brief Get a continuum struct that contains a reference to the server list.
  * \param contptr The value of this pointer will contain the retrieved continuum.
  * \param filename The server-definition file which defines our continuum.
  * \return 0 on failure, 1 on success. */
int ketama_roll( ketama_continuum* contptr, char* filename );

/** \brief Frees any allocated memory.
  * \param contptr The continuum that you want to be destroy. */
void ketama_smoke( ketama_continuum contptr );

/** \brief Maps a key onto a server in the continuum.
  * \param key The key that you want to map to a specific server.
  * \param cont Pointer to the continuum in which we will search.
  * \return The mcs struct that the given key maps to. */
int ketama_get_server( char*, ketama_continuum );

/** \brief Print the server list of a continuum to stdout.
  * \param cont The continuum to print. */
void ketama_print_continuum( ketama_continuum c );

/** \brief Compare two server entries in the circle.
  * \param a The first entry.
  * \param b The second entry.
  * \return -1 if b greater a, +1 if a greater b or 0 if both are equal. */
int ketama_compare( mcs*, mcs* );

/** \brief Hashing function, converting a string to an unsigned int by using MD5.
  * \param inString The string that you want to hash.
  * \return The resulting hash. */
unsigned int ketama_hashi( char* inString );

/** \brief Error method for error checking.
  * \return The latest error that occured. */
char* ketama_error();

int ketama_reset(ketama_continuum *contptr,serverinfo **slist, 
				unsigned int numservers, unsigned long memory);


int delete_server_node(serverinfo **slist, unsigned int *numservers, 
			unsigned long *memory, char *del_ip);

int add_server_node(serverinfo **slist, unsigned int *numservers, 
			unsigned long *memory, serverinfo *sri);

//#ifdef __cplusplus } #endif


#endif

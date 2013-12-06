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

#include "ketama.h"
#include "md5.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>           /* for reading last time modification   */
#include <unistd.h>         /* needed for usleep                    */
#include <math.h>           /* floor & floorf                       */
#include <sys/stat.h>       /* various type definitions             */
#include <sys/shm.h>        /* shared memory functions and structs  */
#include <stdlib.h>

//#ifdef __cplusplus  extern "C"{ #endif

#define OK 0
#define FAIL -1
#define ERROR -1

extern int verbose_mode;

char k_error[255] = "";

int total_ktm_numservers; 

/** \brief Locks the semaphore.
  * \param sem_set_id The semaphore handle that you want to lock. */
void
sem_lock( int sem_set_id )
{
    union semun sem_val;
    sem_val.val = 2;
    semctl( sem_set_id, 0, SETVAL, sem_val );
}


/** \brief Unlocks the semaphore.
  * \param sem_set_id The semaphore handle that you want to unlock. */
void
sem_unlock( int sem_set_id )
{
    union semun sem_val;
    sem_val.val = 1;
    semctl( sem_set_id, 0, SETVAL, sem_val );
}


/** \brief Initialize a semaphore.
  * \param key Semaphore key to use.
  * \return The freshly allocated semaphore handle. */
int
sem_init( key_t key )
{
    int sem_set_id;

    sem_set_id = semget( key, 1, 0 );
    if ( sem_set_id == -1 )
    {
        // create a semaphore set with ID SEM_ID
        sem_set_id = semget( key, 1, IPC_CREAT | 0666 );
        if ( sem_set_id == -1 )
        {
            strcpy( k_error, "Could not open semaphore!" );
            return 0;
        }

        sem_unlock( sem_set_id );
    }

    return sem_set_id;
}


void
ketama_md5_digest( char* inString, unsigned char md5pword[16] )
{
    md5_state_t md5state;

    md5_init( &md5state );
    md5_append( &md5state, inString, strlen( inString ) );
    md5_finish( &md5state, md5pword );
}


/** \brief Retrieve the modification time of a file.
  * \param filename The full path to the file.
  * \return The timestamp of the latest modification to the file. */
time_t file_modtime( char* filename )
{
    struct tm* clock;
    struct stat attrib;

    stat( filename, &attrib );
    clock = gmtime( &( attrib.st_mtime ) );

    return mktime( clock );
}


/** \brief Retrieve a serverinfo struct for one a sever definition.
  * \param line The entire server definition in plain-text.
  * \return A serverinfo struct, parsed from the given definition. */
serverinfo
read_server_line( char* line )
{
    char* delim = "\t";
    serverinfo server;
	memset(&server, 0, sizeof(serverinfo));

    server.weight = 0;

    char* tok = strtok( line, delim );
    if ( ( strlen( tok ) - 1 ) < 23 )
    {
        char* mem = 0;
        char* endptr = 0;

        strncpy( server.ip, tok, strlen( tok ) );
    
        tok = strtok( 0, delim );
        mem = (char *)malloc( strlen( tok ) );
		//zxh added
		memset(mem, '\0', strlen(tok));
        strncpy( mem, tok, strlen( tok ) - 1 );
        mem[ strlen( tok ) - 1 ] = '\0';
    
        errno = 0;
        server.weight = strtol( mem, &endptr, 10 );
        if ( errno == ERANGE || endptr == mem )
        {
            strcpy( k_error, "Invalid memory value in server definitions!" );
            server.weight = 0;
        }

        free( mem );
    }

    return server;
}


/** \brief Retrieve all server definitions from a file.
  * \param filename The full path to the file which contains the server definitions.
  * \param count The value of this pointer will be set to the amount of servers which could be parsed.
  * \param memory The value of this pointer will be set to the total amount of allocated memory across all servers.
  * \return A serverinfo array, containing all servers that could be parsed from the given file. */
serverinfo*
read_server_definitions( char* filename, unsigned int* count, unsigned long* memory )
{
    serverinfo* slist = 0;
    unsigned int numservers = 0;
    unsigned long memtotal = 0;

    FILE* fi = fopen( filename, "r" );
    while ( fi && !feof( fi ) )
    {
        char sline[128] = "";
        fgets( sline, 127, fi );
        if ( strlen( sline ) < 2 || sline[0] == '#' )
            continue;

        serverinfo server = read_server_line( sline );
        if ( strlen( server.ip ) && server.weight > 0 )
        {
            slist = (serverinfo*)realloc( slist, sizeof( serverinfo ) * ( numservers + 1 ) );
            memcpy( &slist[numservers], &server, sizeof( serverinfo ) );
            numservers++;
            memtotal += server.weight;
        }
    }

    if ( !fi )
    {
        sprintf( k_error, "File %s doesn't exist!", filename );
        *count = 0;
        return (serverinfo *)NULL;
    }
    fclose( fi );

    *count = numservers;    
    *memory = memtotal;
    return slist;
}


unsigned int
ketama_hashi( char* inString )
{
    unsigned char digest[16];
    ketama_md5_digest( inString, digest );
        unsigned int ret = ( digest[3] << 24 )
                                 | ( digest[2] << 16 )
                                 | ( digest[1] <<  8 )
                                 |   digest[0];

        return ret;
}

//zxh added  for get server index in array who got by -s;
int ketama_get_server( char* key, ketama_continuum cont )
{
   	unsigned int h = ketama_hashi( key );
	int highp = cont->numpoints;
   	//mcs (*mcsarr)[cont->numpoints] = cont->array;
   	mcs *mcsarr = cont->array;
   	int maxp = highp, lowp = 0, midp;
	unsigned int midval, midval1;

        // divide and conquer array search to find server with next biggest
        // point after what this key hashes to
        while ( 1 )
        {
                midp = (int)( ( lowp+highp ) / 2 );
                if ( midp == maxp ) {
            		if ( midp == cont->numpoints )
                		midp = 1; // if at the end, roll back to zeroth
                    //return &( mcsarr[midp-1] );//zxh changed
                    return (mcsarr[midp-1]).srvid;
        		}
                //midval = (*mcsarr)[midp].point;
                midval = mcsarr[midp].point;
                //midval1 = midp == 0 ? 0 : (*mcsarr)[midp-1].point;
                midval1 = midp == 0 ? 0 : mcsarr[midp-1].point;

                if ( h <= midval && h > midval1 )
                        //return &(mcsarr[midp]); //zxh changed
                        return (mcsarr[midp]).srvid;

                if ( midval < h )
                        lowp = midp + 1;
                else
                        highp = midp - 1;

                if ( lowp > highp )
                	//return &(mcsarr[0]);
                	return (mcsarr[0]).srvid;
        }
}


mcs*
ketama_get_server2( char* key, ketama_continuum cont )
{
   	unsigned int h = ketama_hashi( key );
	int highp = cont->numpoints;
   	//mcs (*mcsarr)[cont->numpoints] = cont->array;
   	mcs *mcsarr = cont->array;
   	int maxp = highp, lowp = 0, midp;
	unsigned int midval, midval1;

        // divide and conquer array search to find server with next biggest
        // point after what this key hashes to
        while ( 1 )
        {
                midp = (int)( ( lowp+highp ) / 2 );
                if ( midp == maxp ) {
            		if ( midp == cont->numpoints )
                		midp = 1; // if at the end, roll back to zeroth

                    //return &( (*mcsarr)[midp-1] );
                    return &( mcsarr[midp-1] );
        		}
                //midval = (*mcsarr)[midp].point;
                midval = mcsarr[midp].point;
                //midval1 = midp == 0 ? 0 : (*mcsarr)[midp-1].point;
                midval1 = midp == 0 ? 0 : mcsarr[midp-1].point;

                if ( h <= midval && h > midval1 )
                        //return &( (*mcsarr)[midp] );
                        return &( mcsarr[midp] );

                if ( midval < h )
                        lowp = midp + 1;
                else
                        highp = midp - 1;

                if ( lowp > highp )
                        return &( mcsarr[0] );
                        //return &( (*mcsarr)[0] );
        }
}


/** \brief Generates the continuum of servers (each server as many points on a circle).
  * \param key Shared memory key for storing the newly created continuum.
  * \param filename Server definition file, which will be parsed to create this continuum.
  * \return 0 on failure, 1 on success. */
int
ketama_create_continuum( key_t key, char* filename )
{
        int shmid;
        int* data; // pointer to shmem location
        unsigned int numservers;
        unsigned long memory;
        serverinfo* slist;

        slist = read_server_definitions( filename, &numservers, &memory );
        if ( numservers < 1 )
        {
                sprintf( k_error, "No valid server definitions in file %s!", filename );
                return 0;
        }
//     syslog( LOG_INFO, "Server definitions read: %d servers, total memory: %d.\n", numservers, memory );

        // continuum will hold one mcs for each point on the circle:
        mcs continuum[ numservers * 160 ];
        int i, k, cont = 0;

        for( i = 0; i < numservers; i++ )
        {
        float pct = (float)slist[i].weight / (float)memory;
        int hpct = floorf( pct * 100.0 );
        int ks = floorf( pct * 40.0 * (float)numservers );

//         syslog( LOG_INFO, "Server no. %d: %s (mem: %d = %d%% or %d of %d)\n", i, slist[i].addr,
//                           slist[i].memory, hpct, ks, numservers * 40 );

                for( k = 0; k < ks; k++ )
                {
                        // 40 hashes, 4 numbers per hash = 160 points per server
                        char ss[30];
                        sprintf( ss, "%s-%d", slist[i].ip, k );
                        unsigned char digest[16];
                        ketama_md5_digest( ss, digest );

                        // use successive 4-bytes from hash as numbers 
                        // for the points on the circle:
                        int h;
                        for( h = 0; h < 4; h++ )
                        {
                                continuum[cont].point = ( digest[3+h*4] << 24 )
                                      | ( digest[2+h*4] << 16 )
                                                              | ( digest[1+h*4] <<  8 )
                                                              |   digest[h*4];

                                memcpy( continuum[cont].ip, slist[i].ip, 22 );
                                cont++;
                        }
                }
        }
        free( slist );

        // sorts in ascending order of "point"
        qsort( (void*) &continuum, cont, sizeof( mcs ), (compfn)ketama_compare );

        // add data to shmmem
        shmid = shmget( key, MC_SHMSIZE, 0644 | IPC_CREAT );
        data = shmat( shmid, (void *)0, 0 );
        if ( data == (void *)(-1) )
        {
        strcpy( k_error, "Can't open shmmem for writing." );
                return 0;
        }

        time_t modtime = file_modtime( filename );
        int nump = cont;
        memcpy( data, &nump, sizeof( int ) );
        memcpy( data + 1, &modtime, sizeof( time_t ) );
        memcpy( data + 1 + sizeof( void* ), &continuum, sizeof( mcs ) * nump );

        // we detatch here because we will re-attach in read-only
        // mode to actually use it.
        if ( shmdt( data ) == -1 )
        strcpy( k_error, "Error detatching from shared memory!" );

        return 1;
}


int
ketama_roll( ketama_continuum* contptr, char* filename )
{
	strcpy( k_error, "" );

	key_t key;
   	int shmid;
   	int *data;
	//int sem_set_id;

   	key = ftok( filename, 'R' );
   	if ( key == -1 ) {
        sprintf( k_error, "Invalid filename specified: %s", filename );
                return 0;
	}

	*contptr = malloc( sizeof( continuum ) );
	(*contptr)->numpoints = 0;
	(*contptr)->array = 0;
	(*contptr)->modtime = 0;

    time_t modtime = file_modtime( filename );
    time_t* fmodtime = 0;

	while ( !fmodtime || modtime != *fmodtime ) {
		shmid = shmget( key, MC_SHMSIZE, 0 ); 			// read only attempt.
		data = shmat( shmid, (void *)0, SHM_RDONLY );

		if ( data == (void *)(-1) || (*contptr)->modtime != 0 ) {
			if ( !ketama_create_continuum( key, filename)) 
                return 0;

			shmid = shmget( key, MC_SHMSIZE, 0 ); // read only attempt.
			data = shmat( shmid, (void *)0, SHM_RDONLY );
		}

		if ( data == (void *)(-1) ) {
			strcpy( k_error, "Failed miserably to get pointer to shmemdata!" );
           	return 0;
		}

		(*contptr)->numpoints = *data;
		(*contptr)->modtime = ++data;
		(*contptr)->array = (mcs *)data + sizeof( mcs * );
        fmodtime = (time_t*)( (*contptr)->modtime );
	}

	return 1;
}


void
ketama_smoke( ketama_continuum contptr )
{
        free( contptr );
}


void
ketama_print_continuum( ketama_continuum cont )
{
        int a;
        printf( "Numpoints in continuum: %d\n", cont->numpoints );

        if ( cont->array == 0 )
        {
                printf( "Continuum empty\n" );
        }
        else
        {
                //mcs (*mcsarr)[cont->numpoints] = cont->array;
                mcs *mcsarr = cont->array;
                for( a = 0; a < cont->numpoints; a++ )
                {
                        //printf( "%s (%u)\n", (*mcsarr)[a].ip, (*mcsarr)[a].point );
                        fprintf(stderr, "%s (%u) srvid=%d\n", mcsarr[a].ip, mcsarr[a].point,mcsarr[a].srvid);
                }
        }
}


int
ketama_compare( mcs *a, mcs *b )
{
        if ( a->point < b->point )
                return -1;

        if ( a->point > b->point )
                return 1;

        return 0;
}


char*
ketama_error()
{
    return k_error;
}

//#ifdef __cplusplus } #endif


/* 
 * zxh modified for changed server information list.
 * before prepare must get server list information.
 */
int
ketama_create_prepare(ketama_continuum kcptr, serverinfo **slist,
				unsigned int numservers, unsigned long memory)
{
	// continuum will hold one mcs for each point on the circle:
   	mcs continuum[numservers * 160]; 
   	int i, k, cont = 0;

	if (!slist || !numservers)
		return FAIL;

	//fprintf(stderr, "numservers=%d\n", numservers);
	memset(continuum, '\0', sizeof(mcs)*numservers*160);

    for( i = 0; i < total_ktm_numservers; i++ ) {
		if (NULL == slist[i]) {
			//fprintf(stderr, "server : %d is NULL\n", i);
			continue;
		}

		float pct = (float)slist[i]->weight / (float)memory; 	//占整个权重的百分比
        int hpct = floorf( pct * 100.0 );						//整数权重值
        int ks = floorf( pct * 40.0 * (float)numservers );

		if (verbose_mode)
			fprintf(stderr, "Server no. %d: %s (mem: %ld = %d%% or %d of %d)\n", i, slist[i]->ip,
				slist[i]->weight, hpct, ks, numservers * 40 );

		for( k = 0; k < ks; k++ ) {
			// 40 hashes, 4 numbers per hash = 160 points per server
            char ss[30];
            sprintf( ss, "%s-%d", slist[i]->ip, k );
            unsigned char digest[16];
            ketama_md5_digest( ss, digest );

            // use successive 4-bytes from hash as numbers 
            // for the points on the circle:
            int h;
            for(h = 0; h < 4; h++) {
            	continuum[cont].point = ( digest[3+h*4] << 24 ) | ( digest[2+h*4] << 16 )
                                                              	| ( digest[1+h*4] <<  8 )
                                                              	|   digest[h*4];
				memset(continuum[cont].ip, '\0', IP_LEN);	
            	memcpy(continuum[cont].ip, slist[i]->ip, 22);
                continuum[cont].srvid = slist[i]->idx;		//srvid is the index in the matrixs server list.
                cont++;
            }
		}
	}

	// sorts in ascending order of "point"
    qsort( (void*) &continuum, cont, sizeof( mcs ), (compfn)ketama_compare );

	int nump = cont;
	mcs *mptr = NULL;	

	kcptr->nsrv = numservers;
	if (NULL == kcptr->array) {
		kcptr->array = (mcs *)calloc(cont, sizeof(mcs));
	//} else if (kcptr->numpoints < cont) {
	} else {
		free(kcptr->array);
		mptr = (mcs *)calloc(cont, sizeof(mcs));
		if (mptr)
			kcptr->array = mptr;
	}

	kcptr->numpoints = cont;
	if (kcptr->array)
		memcpy(kcptr->array, continuum, sizeof(mcs)*cont);
	else
		return FAIL;

	return OK;
}

/* 
 * zxh added
 * reset ketama algorithm.
 */
int ketama_reset(ketama_continuum *contptr,serverinfo **slist, 
				unsigned int numservers, unsigned long memory)
{
    if ( numservers < 1 ) {
		if (verbose_mode)
			fprintf(stderr, "number of server < 1!\n");
        return ERROR;
	}

	if (NULL == *contptr) {
		*contptr = malloc( sizeof(continuum) );
		(*contptr)->numpoints = 0;
		(*contptr)->array = 0;
		(*contptr)->modtime = 0;
	}

	return ketama_create_prepare(*contptr, slist, numservers, memory);
}


/*
 * delete an ip from slist.
 */
int delete_server_node(serverinfo **slist, unsigned int *numservers, 
			unsigned long *memory, char *del_ip)
{
	int i;
	unsigned long del_mem;

	if (!slist || !*numservers || !del_ip)		
		return FAIL;

	//for (i = 0; i < *numservers; i++) {
	for (i = 0; i < total_ktm_numservers; i++) {
		if (slist[i] == NULL)
			continue;

		if (!strncmp(slist[i]->ip, del_ip, IP_LEN)) {
			if (verbose_mode)
				fprintf(stderr, "delete matched!slistip=[%s],del_ip=[%s]\n", slist[i]->ip, del_ip);
			del_mem = slist[i]->weight;	
			/*
			for (j = i; j < *numservers-1; j++) {
				slist[j] = slist[j+1];
			}
			*/
			// here can do it better
			*(slist+i) = NULL; //just set NULL
			*memory -= del_mem;
			*numservers -= 1;
		}
	}
	return OK;
}


/*
 * add an ip for ketama server list.
 */
int add_server_node(serverinfo **slist, unsigned int *numservers, 
			unsigned long *memory, serverinfo *sri)
{
	int i;
	unsigned long add_mem;

	//if (!slist || !*numservers || !sri)		
	if (!slist || !sri)		 {
		return FAIL;
	}

	for (i = 0; i < total_ktm_numservers; i++) {
		if (NULL != *(slist+i)) {
			continue;
		}
		if (verbose_mode)
			fprintf(stderr, "find empty seat in ktmserver list: %d  add server=[%s]\n", i, sri->ip);

		*(slist+i) = sri;
		add_mem = sri->weight;
		*memory += add_mem;
		*numservers += 1;
		break;
	}

	if (i > total_ktm_numservers) {
		fprintf(stderr, "add server to server list error!\n");
		return FALSE;
	}

	return OK;
}

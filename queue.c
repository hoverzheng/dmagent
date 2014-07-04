/*
 * circle queue
 * hover added, 2014/12/12.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cqueue.h"
#include "common.h"
#include "log.h"


extern int verbose_mode;

queue_t *create_queue(int maxlen)
{
	queue_t *q;   
	
	q = (queue_t *)malloc(sizeof(queue_t));
	if (NULL == q)
		return NULL;
	//q->base = (elem_type *)malloc(sizeof(elem_type)*maxlen);
	q->base = (elem_type *)calloc(maxlen, sizeof(elem_type));
	if (NULL == q->base) {
		if (q) free(q);
		return NULL;
	}
	q->maxlen = maxlen;
	q->front = 0;
	q->rear = 0;
	q->size = 0;
	return q; 
}
 

int is_full(queue_t *q)
{
	if (q->size >= q->maxlen)
		return 1;
	else
		return 0;
}
 

int enqueue(queue_t *q, elem_type val)
{
	if (q->size >= q->maxlen) {
		if(verbose_mode) fprintf(stderr, "error: queue full!\n");
		dmlog("error, dead server queue if full.q.size=%d,q->maxlen=%d\n", q->size, q->maxlen);
		return ERROR;
	}
	q->base[q->rear] = val; 
	q->rear = (q->rear+1)%q->maxlen; //roll
	q->size += 1;
	return OK;
}


int dequeue(queue_t *q, elem_type *val)
{
	if (q->size <= 0) {
		if(verbose_mode) fprintf(stderr, "error: empty queue!\n");
		*val = 0;
		return ERROR;
	}
	*val = q->base[q->front];
	q->base[q->front] = 0; //clear
	q->front = (q->front+1)%q->maxlen;
	q->size -= 1;
	return OK;
}
 
 

#if 0
// unit test
#define MAXLEN 5
int main(void)
{
	char a[MAXLEN] = {'\0'};
	queue_t *q;
	char *p;
	char v;

	fgets(a, MAXLEN, stdin);
	a[strlen(a)-1] = '\0';

	printf("a=[%s]\n", a);
	q = create_queue(MAXLEN-3);
	if (!q) {
	 printf("create stack error!\n");
	 return 1;
	}

	printf("%d\n", q->size);
	p = a; 
	while ('\0' != *p) {
	 enqueue(q, *p);
	 p++;
	}
	 
	printf("\n-----dequeue size=[%d]-----\n", q->size);
	while (OK == dequeue(q, &v)) {
	   printf("%c\n", v);
	}
	printf("%d\n", q->size);

	printf("\nend\n");
	return 0;
}
#endif

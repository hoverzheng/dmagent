#ifndef _CQUEUE_H_INCLUDED_
#define _CQUEUE_H_INCLUDED_

#include "ketama.h"

typedef serverinfo * elem_type;

struct queue {
	elem_type *base;
	int front;
	int rear;
	int maxlen;
	int size;
};
typedef struct queue queue_t;


queue_t *create_queue(int maxlen);
int is_full(queue_t *q);
int enqueue(queue_t *q, elem_type val);
int dequeue(queue_t *q, elem_type *val);


#endif

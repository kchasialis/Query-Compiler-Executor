
#ifndef SET
#define SET

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include "alloc_free.h"
#include "dbg.h"

typedef struct node {
    char* value;
    struct node* next;
} node;


typedef struct queue{
    ssize_t size;
    node* first;
    node* last;
} queue;

queue* new_queue();
char* pop(queue *);
void push(queue *, char*);
int q_size(queue *);
void delete_queue(queue *q);

#endif
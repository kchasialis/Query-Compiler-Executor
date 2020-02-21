#include "set.h"

//function to initialize a queue
queue* new_queue() {
    queue* new_q = MALLOC(queue, 1);
    check_mem(new_q);
    
    new_q->first = NULL;
    new_q->last = NULL;
    new_q->size = 0;

    return new_q;

    error:
        return NULL;
}


//function to delete an char of the queue
void delete_char(char* i) {
    FREE(i);
    return;
}

//function to insert a new set in the queue
void push(queue* q, char* new_set) {
    if (new_set == NULL) return;

    node* new_n = MALLOC(node, 1);
    check_mem(new_n);

    new_n->value = new_set;
    new_n->next = NULL;

    if (q->size == 0) {
        q->first = new_n;
        q->last = new_n;
        q->size++;
    }
    else {
        q->last->next = new_n;
        q->last = new_n;
        q->size++;
    }

    error:
        return;
}

//function to pop a set of the queue
char* pop(queue *q){
    if (q->size == 0) goto error;
    char* temp_set;
    if (q->size == 1) {
        temp_set = q->first->value;

        free(q->first);
        q->first = NULL;
        q->last = NULL;
        q->size = 0;
    } else {
        temp_set = q->first->value;
        node* temp_node = q->first;
        
        q->first = temp_node->next;
        q->size--;
        free(temp_node);
    }

    return temp_set;


    error:
        return NULL;

}

//function to return the size of the queue
int q_size(queue* q) {
     return q->size;
}

//function to delete the queue
void delete_queue(queue *q) {
    FREE(q);
    return;
}
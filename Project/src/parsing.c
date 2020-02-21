#include "parsing.h"
#include "stretchy_buffer.h"

static void parse_relations(char *string, query *q) {
    //find how many relations appear according to spaces
    size_t spaces = 0;
    for (size_t i = 0; string[i] != '\0'; i++) {
        if (string[i] == ' ') {
            spaces++;
        }
    }
    
    uint32_t* relations = MALLOC(uint32_t, spaces + 1);

    //parse 
    char current_relation[16];
    char* ptr = string;
    int advance, i = 0;
    while (sscanf(ptr, "%15[^ ]%n", current_relation, &advance) == 1) {
        ptr += advance;
        relations[i++] = (int) strtol(current_relation, NULL, 10);
        if (*ptr != ' ') break;
        ptr++;
    }
    
    q->relations = relations;
    q->relations_size = spaces + 1;
}

static void parse_predicates(char *string, query *q) {
   
    size_t ampersands = 0;
    for (size_t i = 0; string[i] != '\0'; i++) {
        if (string[i] == '&') {
            ampersands++;
        }
    } 

    predicate *predicates = MALLOC(predicate, ampersands + 1);
    
    char current_predicate[128];

    uint32_t v1,v2,v3,v4;
    char op;
    char *ptr = string;
    int advance, i = 0;
    while (sscanf(ptr, "%127[^&]%n", current_predicate, &advance) == 1) {
        ptr += advance;

        if (sscanf(current_predicate, "%d.%d%c%d.%d", &v1, &v2, &op, &v3, &v4) == 5){
            relation_column first;
            first.relation = v1;
            first.column = v2;

            relation_column *second = MALLOC(relation_column, 1);
            second->relation = v3;
            second->column = v4;

            if (v1 == v3) {
                /*Self join*/
                predicates[i].type = 1;
                printf("self join\n");
                predicates[i].operator = SELF_JOIN;
            }
            predicates[i].type = 0;
            predicates[i].first = first;          
            predicates[i++].second = second;

        } else if (sscanf(current_predicate, "%u.%u%c%u", &v1, &v2, &op, &v3) == 4) {
            relation_column first;   
            first.relation = v1;
            first.column = v2;

            uint64_t *second = MALLOC(uint64_t, 1);
            *second = v3;

            predicates[i].type = 1;
            predicates[i].first = first;            
            predicates[i].second = second;
            if (op == '<') {
                predicates[i++].operator = L;
            }
            else if (op == '>') {
                predicates[i++].operator = G;
            }
            else if (op == '=') {
                predicates[i++].operator = EQ;
            }
        }

        if (*ptr != '&') {
            break;
        }
        ptr++;
    }

    q->predicates = predicates;
    q->predicates_size = ampersands + 1;
    
}

static void parse_select(char* string, query *q) {
    //find how many relations appear according to spaces
    size_t spaces = 0;
    for (size_t i = 0; string[i] != '\0'; i++) {
        if (string[i] == ' ') {
            spaces++;
        }
    }

    relation_column* selects = MALLOC(relation_column, spaces + 1);

    char temp_select[128];
    int relation, column;
    char *ptr = string;
    int advance, i = 0;
    while (sscanf(ptr, "%127[^ ]%n", temp_select, &advance) == 1) {
        ptr += advance;
        sscanf(temp_select, "%d.%d", &relation, &column);
        selects[i].relation = relation;
        selects[i++].column = column;
        if (*ptr != ' ') break;
        ptr++;
    }

    q->selects = selects;
    q->select_size = spaces + 1;
}

query* parser() {
    
    query *query_list = NULL;

    char* line_ptr = NULL;
    size_t n = 0;
    int characters;

    while ((characters = getline(&line_ptr, &n, stdin)) != -1) {
        
        if (line_ptr[0] == 'F') {
            break;
        }
        char relations[128], predicates[128], select[128];

        sscanf(line_ptr, "%[0-9 ]%*[|]%[0-9.=<>&]%*[|]%[0-9. ]", relations, predicates, select);

        query new_query = {0};
        parse_relations(relations, &new_query);
        
        parse_predicates(predicates, &new_query);
        
        parse_select(select,  &new_query);
        
        buf_push(query_list, new_query);
    }

    FREE(line_ptr);
    return query_list;

}
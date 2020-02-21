#include "utilities.h"
#include "stretchy_buffer.h"

#define N 30001891

//function to build a histogram
void build_histogram(relation *rel, histogram *hist, uint8_t wanted_byte, int start, int size) {
    //start is the index that starts the bucket and size is the number of tuples that has
    //intialize the histogram
    for (size_t i = 0 ; i < 256 ; i++) {
        hist->array[i] = 0;
    }
    //take the wanted byte of the tuples that belong to the bucket and make the histogram
    for (ssize_t i = start ; i < start + size ; i++) {
        uint32_t byte = get_byte(rel->tuples[i].key, wanted_byte);
        hist->array[byte]++; 
    }
}

//function to build psum
void build_psum(histogram *hist, histogram *psum) {
    //initialize the psum
    for (size_t i =  0 ; i < 256 ; i++) {
        psum->array[i] = -1;
    }

    size_t offset = 0;
    for (ssize_t i = 0;  i < 256 ; i++) {
        if (hist->array[i] != 0) { //every cell is not 0 there is in the histogram , so put it in the psum
            psum->array[i] = offset; 
            offset +=  hist->array[i]; //find the offset with the help of histogram
        }
    }

}

//function to build the reordered array
relation* build_reordered_array(relation* reorder_rel , relation *prev_rel, 
                                histogram* histo , histogram* psum, 
                                uint8_t wanted_byte, int start, int size) {
   
    histogram temp = *histo;

    
    for (ssize_t i = start ; i < start + size ; i++) {
        uint8_t byte = get_byte(prev_rel->tuples[i].key, wanted_byte);

        size_t index = start +  psum->array[byte] + (histo->array[byte] - temp.array[byte]);

        temp.array[byte]--;    

        reorder_rel->tuples[index].key = prev_rel->tuples[i].key;
        reorder_rel->tuples[index].payload = prev_rel->tuples[i].payload;
    }

    return reorder_rel;
}


//function to allocate the memory for the reordered array
relation* allocate_reordered_array(relation* rel) {
    
    relation *r = NULL;
    r = MALLOC(relation, 1);
    check_mem(r);

    r->num_tuples = rel->num_tuples;
    r->tuples = MALLOC(tuple, rel->num_tuples);
    check_mem(r->tuples);

    return r;

    error:
        return NULL;
}

//function to free the allocated memory of the reordered array
void free_reordered_array(relation* r) {
    FREE(r->tuples);
    FREE(r);
}

static inline void fill_data(metadata *m_data, uint64_t *mapped_file) {
    
    m_data->tuples = *mapped_file++;
    m_data->columns = *mapped_file++;
    //printf("tuples = %lu, columns = %lu\n", m_data->tuples, m_data->columns);
    m_data->data = MALLOC(relation *, m_data->columns);

    for (size_t j = 0 ; j < m_data->columns ; j++) {
        relation *rel = MALLOC(relation, 1);
        rel->num_tuples = m_data->tuples;
        rel->tuples = MALLOC(tuple, rel->num_tuples);
        m_data->data[j] = rel;
        uint64_t max_value = 0;
        uint64_t min_value = 0;
        for (size_t i = 0 ; i < m_data->tuples ; i++) {
            rel->tuples[i].key = *mapped_file++;
            rel->tuples[i].payload = i;
            
            if (i == 0) {
                max_value = rel->tuples[i].key;
                min_value = rel->tuples[i].key;
            }
            else {
                if (rel->tuples[i].key > max_value) {
                    max_value = rel->tuples[i].key;
                }
                if (rel->tuples[i].key < min_value) {
                    min_value = rel->tuples[i].key;
                }
            } 
        }
        //printf("rel = %p, column = %d, %lu %lu \n", rel, j, max_value, min_value);
        bool is_smaller = false;
        if (max_value - min_value + 1 <= N) {
            is_smaller = true;
        }
        size_t array_size = is_smaller ? max_value - min_value + 1 : N;
        bool *distinct = CALLOC(array_size, sizeof(bool), bool);
        for (size_t i = 0 ; i < rel->num_tuples ; i++) {
            uint64_t key = rel->tuples[i].key;
            if (is_smaller) {
                distinct[key - min_value] = true;
            } 
            else {
                distinct[(key - min_value) % array_size] = true;
            }
        }
        
        uint32_t distinct_values = 0;
        for (size_t i  = 0 ; i < rel->num_tuples ; i++) {
            distinct_values++;
        
        }
        rel->stats = MALLOC(statistics, 1);
        rel->stats->max_value = max_value;
        rel->stats->min_value = min_value;
        rel->stats->distinct_values = distinct_values;
        rel->stats->array = distinct;
        rel->stats->array_size = array_size;
        rel->stats->approx_elements = rel->num_tuples;
    }
}

static void update_non_eq(uint64_t k1, uint64_t k2, relation *rel) {
    
    if (k1 < rel->stats->min_value) {
        k1 = rel->stats->min_value;
    }
    if (k2 > rel->stats->max_value) {
        k2 = rel->stats->max_value;
    }

    if (rel->stats->max_value - rel->stats->min_value > 0) {
        rel->stats->distinct_values = (rel->stats->distinct_values * (k2 - k1)) / (rel->stats->max_value - rel->stats->min_value);
        rel->stats->approx_elements = (rel->stats->approx_elements * (k2 - k1)) / (rel->stats->max_value - rel->stats->min_value);
    }
    
    rel->stats->min_value = k1;
    rel->stats->max_value = k2;
}

static long my_pow(size_t base, size_t power) {

    long result = 1;
    for (size_t i = 0 ; i < power ; i++) {
        result *= base;
    }
    return result;
}

static void update_other_columns(ssize_t column, ssize_t approx_elements, metadata *md, relation *rel) {
    for (ssize_t i = 0 ; i < md->columns ; i++) {
        if (i != column) {
            relation *current_rel = md->data[i];
            size_t approx_tmp = rel->stats->approx_elements;
            size_t distinct_tmp = rel->stats->distinct_values;
            if (approx_elements != 0 && distinct_tmp != 0) {
                long val = my_pow(1 - rel->stats->approx_elements / approx_elements, approx_tmp / distinct_tmp);
                current_rel->stats->distinct_values *= (1 - val);
            }
            current_rel->stats->approx_elements = rel->stats->approx_elements;
        }
    }
}

void update_statistics(query *qry, metadata *metadata_arr) {

    for (ssize_t i = 0 ; i < qry->predicates_size ; i++) {
        predicate current = qry->predicates[i];
        if (current.type == 1) {
            /*If its a filter*/
            
            uint32_t row = qry->relations[current.first.relation];
            uint32_t column = current.first.column;
            metadata md = metadata_arr[row];
            relation *rel = md.data[column];


            bool found = false;
            
            size_t approx_elements = rel->stats->approx_elements;

            if (current.operator == EQ) {
                uint64_t number = *(uint64_t *) current.second;
                            
                if ( (number - rel->stats->min_value >= 0) && (number - rel->stats->min_value < rel->stats->max_value) && rel->stats->array[number - rel->stats->min_value]) {
                    rel->stats->approx_elements = approx_elements / (rel->stats->distinct_values+1);
                    rel->stats->distinct_values = 1;
                    rel->stats->array_size = 1;
                    FREE(rel->stats->array);
                    rel->stats->array = MALLOC(bool, 1);
                    rel->stats->array[0] = true;
                }
                else {
                    rel->stats->approx_elements = 0;
                    rel->stats->distinct_values = 0;
                }
                rel->stats->max_value = number;
                rel->stats->min_value = number;
            }
            else if (current.operator != SELF_JOIN) {
                /*Check if we have something like R.A > 5000 & R.A < 8000*/
                for (ssize_t j = i + 1 ; j < qry->predicates_size ; j++) {
                    predicate tmp = qry->predicates[j];
                    if (tmp.type == 0) {
                        break;
                    }
                    else if (current.first.relation == tmp.first.relation && current.first.column == tmp.first.column) {
                        uint64_t k1 = *(uint64_t *) current.second;
                        uint64_t k2 = *(uint64_t *) tmp.second;

                        if ((current.operator == LEQ && tmp.operator == GEQ) || (current.operator == L && tmp.operator == G)) {
                            /*If its tmp >= k2, current <= k1, make it tmp <= k2, current >= k1*/
                            uint32_t temp = k1;
                            k1 = k2;
                            k2 = temp;
                        }
                        
                        update_non_eq(k1, k2, rel);    
                        
                        found = true;
                        break;
                    }
                }
                if (!found && (current.operator == LEQ || current.operator == L)) {
                    /*We have only filter of type R.A < 5000, set k1 to be min*/
                    uint64_t k1 = rel->stats->min_value;
                    uint64_t k2 = *(uint64_t *) current.second;

                    update_non_eq(k1, k2, rel);
                }
                else if (!found && (current.operator == GEQ || current.operator == G)) {
                    uint64_t k1 = *(uint64_t *) current.second;
                    uint64_t k2 = rel->stats->max_value;
                    //printf("%lu %lu \n", k1 , k2);
                    update_non_eq(k1, k2, rel);
                }
            }
            else if (current.operator == SELF_JOIN) {
                relation_column rel_col = *(relation_column *) current.second;
                
                relation *relB = md.data[rel_col.column];
                uint64_t max_valA = rel->stats->max_value;
                uint64_t max_valB = relB->stats->max_value;
                uint64_t min_valA = rel->stats->min_value;
                uint64_t min_valB = relB->stats->min_value;
                uint64_t distinct_values = rel->stats->distinct_values;
                
                uint64_t max_val = max_valA > max_valB ? max_valA : max_valB;
                uint64_t min_val = min_valA < min_valB ? min_valA : min_valB;
                size_t n = max_val - min_val + 1;

                rel->stats->approx_elements = relB->stats->approx_elements = approx_elements / n;
                long val = 0;
                if (approx_elements != 0 && distinct_values != 0) {
                    val = my_pow(1 - rel->stats->approx_elements / approx_elements, approx_elements / distinct_values);
                }   
                rel->stats->distinct_values = distinct_values * (1 - val);  
                relB->stats->distinct_values = distinct_values * (1 - val);

                for (ssize_t j = 0 ; j < md.columns ; j++) {
                    if (j != column && j != rel_col.column) {
                        uint64_t approx_tmp = md.data[j]->stats->approx_elements;
                        uint64_t distinct_tmp = md.data[j]->stats->distinct_values;
                        if (distinct_tmp != 0 && approx_elements != 0) {
                            val = my_pow(1 - rel->stats->approx_elements / approx_elements, approx_tmp / distinct_tmp);
                            md.data[j]->stats->distinct_values *= (1 - val);
                        }
                        md.data[j]->stats->approx_elements = rel->stats->approx_elements;
                    }
                }
            }
            update_other_columns(column, approx_elements, &md, rel);
       }
   }
}

metadata* read_relations() {

    char *linptr = NULL;
    size_t n = 0;

    metadata *metadata_arr = NULL;

    while (getline(&linptr, &n, stdin) != -1) {
        if (!strncmp(linptr, "Done\n", strlen("Done\n")) || !strncmp(linptr,"done\n", strlen("done\n"))) {
            break;
        }
        
        linptr[strlen(linptr) - 1] = '\0';
        int fd = open(linptr, O_RDONLY, S_IRUSR | S_IWUSR);
        check(fd != -1, "open failed");

        struct stat sb;

        check(fstat(fd, &sb) != -1, "fstat failed");

        uint64_t *mapped_file = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        check(mapped_file != MAP_FAILED, "mmap failed");

        metadata m_data;

        fill_data(&m_data, mapped_file);

        buf_push(metadata_arr, m_data);

        munmap(mapped_file, sb.st_size);
        close(fd);
    }

    FREE(linptr);
    return metadata_arr;

    error:
        FREE(linptr);
        return NULL;
}

exists_info relation_exists(mid_result **mid_results_array, uint64_t relation, uint64_t predicate_id) {

    exists_info exists;
    exists.index = -1;
    for (ssize_t i = buf_len(mid_results_array) - 1 ; i >= 0 ; i--) {
        if (mid_results_array[i] != NULL) {
            for (ssize_t j = 0 ; j < buf_len(mid_results_array[i]) ; j++) {
                if (mid_results_array[i][j].relation == relation && mid_results_array[i][j].predicate_id == predicate_id) {
                    exists.index = j;
                    exists.mid_result = i;
                    return exists;
                }
            }
        }
    }

    return exists;
}

ssize_t relation_exists_current(mid_result *mid_results, uint64_t relation, uint64_t predicate_id) {

    ssize_t found = -1;
    for (ssize_t i = 0 ; i < buf_len(mid_results) ; i++) {
        if (mid_results[i].relation == relation && mid_results[i].predicate_id == predicate_id) {
            found = i;
            break;
        } 
    }

    return found;
}


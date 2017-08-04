#include "prefetcher2.h"
#include "time.h"

//CAREFUL: This implementation assumes sizeof(void*) == sizeof(long), it won't work if that's no the case
//This is used whenever using GPOINTER_TO_SIZE or GSIZE_TO_POINTER as gsize is defined as unsigned long


int debug;
static csr_t * volatile graph;

static pthread_t prefetch_thread;
static volatile unsigned long prefetch_thread_state = 0;


static prefetch_pipe_t ** volatile pipes; //An array of pointer to pipes to let threads talk to the prefetcher
static volatile unsigned int thread_count; //number of threads using this prefetcher
static int last_thread; //The max id of any thread

static LRU_ULL_t * lru; //A pointer to the most recently used element structure in the LRU linked list
//lru->next is the least recently used pointer
static GHashTable * page_LRU_addr;   //Hash table for the pages of nodes in the top_heap page_addr->lru_element
static GHashTable * heap_pos;   //Position of nodes in the heap, leading bit is 0 if on top, and 1 if on bottom


static GHashTable *  fadvise_hash;
pthread_mutex_t shared_hash;

static heap_element_t * top_heap;
static uint top_heap_size;

static GArray* bottom_heap;

static uint pages_count; //The number of pages in the hash
//We slightly overestimate this by counting twice a page
//which is both the last page of a multipage vertice, and
//the first page of another vertice

static uint fadvise_count;  //Used as a timestamp

static uint last_processed_thread; //Used to process them in round tobin



static void *prefetcher(void *arg){
  while(prefetch_thread_state != TERMINATE){
    iter();
  }
  return NULL; //is this necessary? why not just void?
}

void split_ull(){
  unsigned char half = lru->nbr_elements / 2;
  unsigned char i;
  LRU_ULL_t * lru_next = malloc(sizeof(LRU_ULL_t));

  lru_next->next = lru->next;
  lru->next = lru_next;


  lru_next->timestamp = lru->timestamp;

  for(i=0; i+half <lru->nbr_elements; i++){
    lru_next->elements[i] = lru->elements[i+half];
    g_hash_table_insert(page_LRU_addr, GSIZE_TO_POINTER(lru_next->elements[i].page_addr),(void*) &(lru_next->elements[i]));
  }

  lru_next->nbr_elements = lru->nbr_elements - half;
  lru->nbr_elements = half;
  //Finally, we leave the pointer on the second half to ensure lru->next is the least recently used element
  lru = lru_next;
}
void merge_ull(){
  unsigned char count;
  LRU_ULL_t* next = lru->next;
  if(lru->nbr_elements + next->nbr_elements < ULL_CHUNK){
    for(count = 0; count < next->nbr_elements; count++){
      lru->elements[lru->nbr_elements + count] = next->elements[count];
      g_hash_table_insert(page_LRU_addr, GSIZE_TO_POINTER(lru->elements[lru->nbr_elements + count].page_addr),(void*) &(lru->elements[lru->nbr_elements + count]));
    }
    lru->nbr_elements += next->nbr_elements;
    lru->next = next->next;
    free(next);
  }
}
void lru_add_page(ulong page_addr, ulong nbr_pages){
  LRU_element_t * le;
  le = (LRU_element_t *) g_hash_table_lookup(page_LRU_addr, GSIZE_TO_POINTER(page_addr));
  if(le != NULL){
#ifndef NDEBUG
    assert(nbr_pages == 1 || le->nbr_pages == 1); //There can only be one node at the end of the page
#endif
    if(nbr_pages > 1){
      le->nbr_pages = nbr_pages;
      pages_count += nbr_pages - 1;
      fadvise_wn(le);
    }
    le->nbr_nodes++;
  }else{
    //inserting an entry in the curren LRU chunk
    if(lru->nbr_elements == ULL_CHUNK){
      clean_ull(lru);
      if(lru->nbr_elements == ULL_CHUNK){
          split_ull();
      }
    }
    lru->elements[lru->nbr_elements].page_addr = page_addr;
    lru->elements[lru->nbr_elements].nbr_pages = nbr_pages;
    lru->elements[lru->nbr_elements].nbr_nodes = 1;
    fadvise_wn(&lru->elements[lru->nbr_elements]);
    g_hash_table_insert(page_LRU_addr, GSIZE_TO_POINTER(page_addr),(void*) &(lru->elements[lru->nbr_elements]));
    lru->nbr_elements++;
    pages_count += nbr_pages;
  }
}
void lru_add(ulong node){
  ulong page_addr, nbr_pages;
  NODE_PAGE(node, page_addr, nbr_pages);
  lru_add_page(page_addr, nbr_pages);

  //Then we put the aux in the LRU
  if(graph->index_aux != NULL){
    NODE_PAGE_AUX(node, page_addr, nbr_pages);
    lru_add_page(page_addr,nbr_pages);
  }
}
void lru_remove_page(ulong page_addr, ulong nbr_pages){
  LRU_element_t * le;
  le = (LRU_element_t *) g_hash_table_lookup(page_LRU_addr, GSIZE_TO_POINTER(page_addr));
#ifndef NDEBUG
  assert(le != NULL);
#endif
  le->nbr_nodes--;
  if(nbr_pages != 1){
    le->nbr_pages = 1;
    pages_count -= nbr_pages - 1;
  }
  if(le->nbr_nodes == 0){
    //We won't need the element soon
    //fadvise_dn(le);
    //Delete the element by turning it to 0, the iteration will reorganise things
    le->page_addr = ULONG_MAX;
    g_hash_table_remove(page_LRU_addr, GSIZE_TO_POINTER(page_addr));
    pages_count--;
  }
}
void lru_remove(ulong node){
  ulong page_addr, nbr_pages;
  NODE_PAGE(node, page_addr, nbr_pages);
  lru_remove_page(page_addr, nbr_pages);

  if(graph->index_aux != NULL){
    NODE_PAGE_AUX(node, page_addr, nbr_pages);
    lru_remove_page(page_addr,nbr_pages);
  }
}



void init_prefetcher(csr_t* g){
  graph = g;

#ifndef NDEBUG
  assert(sizeof(void*) == sizeof(long));
  debug = 0;
#endif
  page_LRU_addr = g_hash_table_new(g_direct_hash, g_direct_equal);
  heap_pos = g_hash_table_new(g_direct_hash, g_direct_equal);
  fadvise_hash = g_hash_table_new(g_direct_hash, g_direct_equal);
  bottom_heap = g_array_sized_new(FALSE,FALSE,sizeof(heap_element_t),2000000);

  lru = malloc(sizeof(LRU_ULL_t));
  lru->next = lru;
  lru->nbr_elements = 0;
  lru->timestamp = 0;
  //printf("lru initialised: %p\n", lru);

  pages_count = 0;
  fadvise_count = 0;
  top_heap_size = 0;

  top_heap = malloc(sizeof(heap_element_t) * MAX_PAGE_COUNT* (ASSUME_PAGE_SIZE * 8 / graph->bits_per_edge) + 1);
  //The +1 is to allow first adding an element to the heap and then checking if the page_count is too high
  //We make the assumption that all nodes have at least one edge
  pipes = calloc(MAX_THREAD_COUNT, sizeof(prefetch_pipe_t *));  //Allocate the array and sets all bits to 0
  last_thread = -1;
  last_processed_thread = 0;
  thread_count = 0;
  //pthread_mutex_init(&shared_hash, NULL);
}
void write_pipe(unsigned int thread, unsigned long node, int priority){
  uint next = (pipes[thread]->write_index == PIPE_SIZE -1) ? 0 : pipes[thread]->write_index + 1;
  uint a = 0;
  while(next == pipes[thread]->read_index){ //Wait until the pipe is not full
    a++;
  }
  pipes[thread]->pipe_array[pipes[thread]->write_index].node = node;
  pipes[thread]->pipe_array[pipes[thread]->write_index].priority = priority;
  pipes[thread]->write_index = next;
}
unsigned int register_thread(){
  prefetch_pipe_t * tmp;
  int count = 0;
  if(thread_count == MAX_THREAD_COUNT){
    perror("Using more threads than MAX_THREAD_COUNT");
    return -1;
  }
  while(pipes[count] != 0){
    count++;
  }

#ifndef NDEBUG
  assert(count <= last_thread + 1 && count != last_thread);
#endif
  tmp = malloc(sizeof(prefetch_pipe_t));
  tmp->read_index = 0;
  tmp->write_index = 0;
  pipes[count] = tmp;
  last_thread = count > last_thread ? count : last_thread;
  thread_count++;
  return count;
}
void deregister_thread(unsigned int id){
  //We let the prefetcher main thread do the bookeeping to avoid concurrency issues
  pipes[id]->write_index = -1;
}

unsigned int extra_pages(heap_element_t * he){
  ulong page_addr, page_addr_aux;
  uint nbr_pages, nbr_pages_aux;
  NODE_PAGE(he->node, page_addr, nbr_pages);
  if(g_hash_table_contains(page_LRU_addr, GSIZE_TO_POINTER(page_addr))){
    nbr_pages--;
  }
  if(graph->index_aux != NULL){
    NODE_PAGE_AUX(he->node, page_addr_aux, nbr_pages_aux);
    if(g_hash_table_contains(page_LRU_addr, GSIZE_TO_POINTER(page_addr_aux))){
      nbr_pages_aux--;
    }
    nbr_pages += nbr_pages_aux;
  }
  return nbr_pages;
}

void free_thread(unsigned int id){
  free(pipes[id]);
  pipes[id] = 0;
  if(id == last_thread){
    last_thread--;
    while(last_thread >= 0 && pipes[last_thread] == 0){
      last_thread--;
    }

#ifndef NDEBUG
    assert(last_thread == -1 || thread_count > 0);
#endif
  }
  thread_count--;
}
void check_heaps(){
  ulong addr,node;
  unsigned long i;
  for(i = 0; top_heap_size != 0 && i< (top_heap_size-1) / 2; i++){
      assert(top_heap[i].priority <= top_heap[2*i +1].priority);
      if(2*i + 2 < top_heap_size){
          assert(top_heap[i].priority <= top_heap[2*i + 2].priority);
      }
  }
  for(i = 0; bottom_heap->len != 0 && i< (bottom_heap->len-1) / 2; i++){
      assert(g_array_index(bottom_heap, heap_element_t, i).priority >= g_array_index(bottom_heap, heap_element_t, 2*i+1).priority);
      if(2*i + 2 < bottom_heap->len){
          assert(g_array_index(bottom_heap, heap_element_t, i).priority >= g_array_index(bottom_heap, heap_element_t, 2*i +2).priority);
      }
  }
  assert(top_heap_size == 0 ||
                 top_heap_size != 0 && bottom_heap->len == 0||
                 top_heap[0].priority >= g_array_index(bottom_heap, heap_element_t, 0).priority);
  for(i = 0; i< top_heap_size; i++){
      addr = GPOINTER_TO_SIZE(g_hash_table_lookup(heap_pos, GSIZE_TO_POINTER(top_heap[i].node)));
      assert(i == addr);
  }
  for(i = 0; i< bottom_heap->len; i++){
      addr = GPOINTER_TO_SIZE(g_hash_table_lookup(heap_pos, GSIZE_TO_POINTER(g_array_index(bottom_heap, heap_element_t, i).node)));
      assert(TEST_MSB(addr));
      addr = UNSET_MSB(addr);
      assert(i == addr);
  }


  GHashTableIter iter;
  gpointer key, value;
  g_hash_table_iter_init (&iter, heap_pos);
  while (g_hash_table_iter_next (&iter, &key, &value))
    {
      addr = GPOINTER_TO_SIZE(value);
      node = GPOINTER_TO_SIZE(key);
      if(TEST_MSB(addr)){
          assert(node == g_array_index(bottom_heap, heap_element_t, UNSET_MSB(addr)).node);
      }else{
          assert(node == top_heap[addr].node);
      }
    }
}
void check_lru(){
  assert(pages_count == lru_page_count());
  assert(pages_count <= MAX_PAGE_COUNT);
  assert(bottom_heap->len == 0 || pages_count + extra_pages(& g_array_index(bottom_heap, heap_element_t, 0)) >= MAX_PAGE_COUNT);
  LRU_ULL_t* tmp = lru;
  do{
    int i;
    for(i = 0; i< (int) tmp->nbr_elements; i++){
      if(tmp->elements[i].page_addr != ULONG_MAX){
        assert((LRU_element_t *) g_hash_table_lookup(page_LRU_addr, GSIZE_TO_POINTER(tmp->elements[i].page_addr)) == &tmp->elements[i]);
      }
    }
    tmp = tmp->next;
  }while(tmp != lru);
  GHashTableIter iter;
  gpointer key, value;
  g_hash_table_iter_init (&iter, page_LRU_addr);
  while (g_hash_table_iter_next (&iter, &key, &value))
    {
      assert(GPOINTER_TO_SIZE(key) == ((LRU_element_t *) value)->page_addr);
    }
}

char heap_test(heap_element_t * he){ //We test whether a heap element shoud be on the top or bottom heap
  char bottom = 1;
  if(top_heap_size == 0 && MAX_PAGE_COUNT != 0){
    bottom = 0;
  }else if(he->priority > top_heap[0].priority){
    bottom = 0;
  }else if(bottom_heap->len == 0 || he->priority >= g_array_index(bottom_heap, heap_element_t, 0).priority){
    if(extra_pages(he)+ pages_count <= MAX_PAGE_COUNT){
      bottom = 0;
    }
  }
  return bottom;
}
void process_pipe(unsigned int thread){
  uint index;
  ulong addr;
  gpointer ptr;
  heap_element_t he = pipes[thread]->pipe_array[pipes[thread]->read_index];
#ifndef NDEBUG
  //printf("Processing pipe instruction: Node:%lu, Priority:%i\n",he.node,he.priority);

#endif

  pipes[thread]->read_index = (pipes[thread]->read_index == PIPE_SIZE-1) ? 0 : pipes[thread]->read_index + 1;
  ptr = g_hash_table_lookup(heap_pos, GSIZE_TO_POINTER(he.node));
  if(ptr != NULL || (top_heap[0].node == he.node && top_heap_size > 0)){  //index 0 would have a NULL pointer
    addr = GPOINTER_TO_SIZE(ptr);
    index = UNSET_MSB(addr);   //remove msb and get rid of the upper bytes if int is smaller than long
    if(TEST_MSB(addr)){ //The node is in the bottom heap
      if(he.priority == INT_MIN){ //instruction to remove a node
        bottom_remove(index);
        balance_heaps();
      }else if(heap_test(&he)){ //The node should remain in the bottom heap
        bottom_change_priority(index, he.priority);
        balance_heaps();
      }else{  //The node should go to the top heap
        bottom_remove(index);
        top_insert(&he);
        balance_heaps();
      }
    }else{  //The node is in the top heap
      if(he.priority == INT_MIN){ //instruction to remove a node
        top_remove(index);
        balance_heaps();
      }else if(he.priority <= g_array_index(bottom_heap, heap_element_t, 0).priority){ //The node should go to the bottom heap
        top_remove(index);
        bottom_insert(&he);
        balance_heaps();
      }else{  //The node should remain in the top heap
        top_change_priority(index, he.priority);
      }
    }
  }else{
    if(he.priority != INT_MIN){
      if(heap_test(&he)){
        bottom_insert(&he);
      }else{
        top_insert(&he);
        balance_heaps();
      }
    }
  }
#ifndef NDEBUG
  check_heaps();
  check_lru();
#endif
//  if(he.node == 822266){
//      exit(EXIT_FAILURE);
//  }
}
void balance_heaps(){
  if(pages_count <= MAX_PAGE_COUNT){
      uint extra = extra_pages(& g_array_index(bottom_heap, heap_element_t, 0));
#ifndef NDEBUG
      uint old_pages_count = pages_count;
#endif
      while(bottom_heap->len != 0 && (extra + pages_count <= MAX_PAGE_COUNT)){ //We add new nodes from the bottom heap if possible
        heap_element_t popped = g_array_index(bottom_heap, heap_element_t, 0);
        bottom_pop();
        top_insert(&popped);
#ifndef NDEBUG
        assert(old_pages_count + extra == pages_count);
        old_pages_count = pages_count;
#endif
        extra = extra_pages(& g_array_index(bottom_heap, heap_element_t, 0));
      }
  }else{
    while(pages_count > MAX_PAGE_COUNT && top_heap_size > 1){
      heap_element_t popped = top_heap[0];
      top_pop();
      bottom_insert(&popped);
    }
  }
}
void top_insert(heap_element_t * he){
  top_heap[top_heap_size] = *he;
  bubble_up_top(top_heap_size);
  top_heap_size++;
  lru_add(he->node);
  //printf("Added %lu to top_heap\n", he->node);
}
void bottom_insert(heap_element_t * he){
  g_array_append_val(bottom_heap, *he);
  bubble_up_bottom(bottom_heap->len - 1);
  //printf("Added %lu to bottom_heap\n", he->node);
}
void print_prefetcher(){
  int i,j;
  printf("top heap:\n");
  for(i = 0; i < top_heap_size; i++){
    printf("[%lu, %i]\n", top_heap[i].node, top_heap[i].priority);
  }
  printf("bottom heap:\n");
  for(i = 0; i < bottom_heap->len; i++){
    printf("[%lu, %i]\n", g_array_index(bottom_heap, heap_element_t, i).node, g_array_index(bottom_heap, heap_element_t, i).priority);
  }
  for(i=0; i < MAX_THREAD_COUNT; i++){
    if(pipes[i] != 0){
      printf("Pipe %i:\n", i);
      for(j=pipes[i]->read_index; j != pipes[i]->write_index; j = j == PIPE_SIZE-1 ? 0: j+1){
        printf("[%lu, %i]\n",pipes[i]->pipe_array[j].node, pipes[i]->pipe_array[j].priority);
      }
    }
  }
}
int lru_page_count(){
  LRU_ULL_t* tmp = lru;
  int count = 0;
  do{
    int i;
    for(i = 0; i< (int) tmp->nbr_elements; i++){
      if(tmp->elements[i].page_addr != ULONG_MAX){
        count += tmp->elements[i].nbr_pages;
      }
    }
    tmp = tmp->next;
  }while(tmp != lru);
  return count;
}

void top_pop(){
  top_remove(0);
}
void bottom_pop(){
  bottom_remove(0);
}
void top_change_priority(uint index, int priority){
  if(top_heap[index].priority > priority){
    top_heap[index].priority = priority;
    bubble_up_top(index);
  }else{
    top_heap[index].priority = priority;
    bubble_down_top(index);
  }
}
void bottom_change_priority(uint index, int priority){
  if(g_array_index(bottom_heap, heap_element_t, index).priority < priority){
    g_array_index(bottom_heap, heap_element_t, index).priority = priority;
    bubble_up_bottom(index);
  }else{
    g_array_index(bottom_heap, heap_element_t, index).priority = priority;
    bubble_down_bottom(index);
  }
}

void top_remove(uint index){
  //printf("removed %lu from top_heap\n", top_heap[index].node);
  g_hash_table_remove(heap_pos, GSIZE_TO_POINTER(top_heap[index].node));
  lru_remove(top_heap[index].node);
  if(index != top_heap_size-1){
    int priority = top_heap[top_heap_size-1].priority;
    top_heap[index].node = top_heap[top_heap_size-1].node;
    top_heap_size--;
    top_change_priority(index,priority);
  }else{
    top_heap_size--;
  }
}
void bottom_remove(uint index){
  //printf("removed %lu from bottom_heap\n", g_array_index(bottom_heap, heap_element_t, index).node);
  g_hash_table_remove(heap_pos, GSIZE_TO_POINTER(g_array_index(bottom_heap, heap_element_t, index).node));
  if(index != bottom_heap->len -1){
    int priority = g_array_index(bottom_heap, heap_element_t, bottom_heap->len -1).priority;
    g_array_index(bottom_heap, heap_element_t, index).node = g_array_index(bottom_heap, heap_element_t, bottom_heap->len -1).node;
    g_array_remove_index(bottom_heap, bottom_heap->len -1);
    bottom_change_priority(index,priority);
  }else{
    g_array_remove_index(bottom_heap, bottom_heap->len -1);
  }
}

void bubble_up_bottom(uint pos){
  ulong addr;
  uint parent_pos = (pos - 1) / 2;
  heap_element_t swap;
  while(pos != 0 &&
        g_array_index(bottom_heap, heap_element_t, parent_pos).priority <
        g_array_index(bottom_heap, heap_element_t, pos).priority ){
    swap = g_array_index(bottom_heap, heap_element_t, parent_pos);
    g_array_index(bottom_heap, heap_element_t, parent_pos) = g_array_index(bottom_heap, heap_element_t, pos);
    g_array_index(bottom_heap, heap_element_t, pos) = swap;
    addr = pos;
    addr = SET_MSB(addr);
    g_hash_table_insert(heap_pos,
                        GSIZE_TO_POINTER(g_array_index(bottom_heap, heap_element_t, pos).node),
                        GSIZE_TO_POINTER(addr));
    pos = parent_pos;
    parent_pos = (pos - 1) / 2;
  }
  addr = pos;
  addr = SET_MSB(addr);
  g_hash_table_insert(heap_pos,
                      GSIZE_TO_POINTER(g_array_index(bottom_heap, heap_element_t, pos).node),
                      GSIZE_TO_POINTER(addr));
}
void bubble_down_bottom(uint pos){
  ulong addr;
  uint child1_pos = pos * 2 + 1;
  uint child2_pos = child1_pos + 1;
  heap_element_t swap;
  while(child2_pos < bottom_heap->len &&
        (g_array_index(bottom_heap, heap_element_t, pos).priority <
        g_array_index(bottom_heap, heap_element_t, child1_pos).priority ||
        g_array_index(bottom_heap, heap_element_t, pos).priority <
        g_array_index(bottom_heap, heap_element_t, child2_pos).priority) ){

    if(g_array_index(bottom_heap, heap_element_t, child1_pos).priority >
       g_array_index(bottom_heap, heap_element_t, child2_pos).priority){
      //Child1 goes on top
      swap = g_array_index(bottom_heap, heap_element_t, child1_pos);
      g_array_index(bottom_heap, heap_element_t, child1_pos) = g_array_index(bottom_heap, heap_element_t, pos);
      g_array_index(bottom_heap, heap_element_t, pos) = swap;
      addr = pos;
      addr = SET_MSB(addr);
      g_hash_table_insert(heap_pos,
                          GSIZE_TO_POINTER(g_array_index(bottom_heap, heap_element_t, pos).node),
                          GSIZE_TO_POINTER(addr));
      pos = child1_pos;
      child1_pos = pos * 2 + 1;
      child2_pos = child1_pos + 1;
    }else{
      swap = g_array_index(bottom_heap, heap_element_t, child2_pos);
      g_array_index(bottom_heap, heap_element_t, child2_pos) = g_array_index(bottom_heap, heap_element_t, pos);
      g_array_index(bottom_heap, heap_element_t, pos) = swap;
      addr = pos;
      addr = SET_MSB(addr);
      g_hash_table_insert(heap_pos,
                          GSIZE_TO_POINTER(g_array_index(bottom_heap, heap_element_t, pos).node),
                          GSIZE_TO_POINTER(addr));
      pos = child2_pos;
      child1_pos = pos * 2 + 1;
      child2_pos = child1_pos + 1;
    }
  }
  if(child2_pos == bottom_heap->len &&
     (g_array_index(bottom_heap, heap_element_t, pos).priority <
      g_array_index(bottom_heap, heap_element_t, child1_pos).priority)){

    swap = g_array_index(bottom_heap, heap_element_t, child1_pos);
    g_array_index(bottom_heap, heap_element_t, child1_pos) = g_array_index(bottom_heap, heap_element_t, pos);
    g_array_index(bottom_heap, heap_element_t, pos) = swap;
    addr = pos;
    addr = SET_MSB(addr);
    g_hash_table_insert(heap_pos,
                        GSIZE_TO_POINTER(g_array_index(bottom_heap, heap_element_t, pos).node),
                        GSIZE_TO_POINTER(addr));
    pos = child1_pos;
  }
  addr = pos;
  addr = SET_MSB(addr);
  g_hash_table_insert(heap_pos,
                      GSIZE_TO_POINTER(g_array_index(bottom_heap, heap_element_t, pos).node),
                      GSIZE_TO_POINTER(addr));
}
void bubble_up_top(uint pos){
  ulong addr;
  uint parent_pos = (pos-1) / 2;
  heap_element_t swap;
  while(pos != 0 &&
        (top_heap[parent_pos].priority > top_heap[pos].priority)){
    swap = top_heap[parent_pos];
    top_heap[parent_pos] = top_heap[pos];
    top_heap[pos] = swap;
    addr = pos;
    g_hash_table_insert(heap_pos,
                        GSIZE_TO_POINTER(top_heap[pos].node),
                        GSIZE_TO_POINTER(addr));
    pos = parent_pos;
    parent_pos = (pos-1) / 2;
  }
  addr = pos;
  g_hash_table_insert(heap_pos,
                      GSIZE_TO_POINTER(top_heap[pos].node),
                      GSIZE_TO_POINTER(addr));
}
void bubble_down_top(uint pos){
  ulong addr;
  uint child1_pos = pos * 2 +1;
  uint child2_pos = child1_pos + 1;
  heap_element_t swap;
  while(child2_pos < top_heap_size &&
        (top_heap[pos].priority >
        top_heap[child1_pos].priority ||
        top_heap[pos].priority >
        top_heap[child2_pos].priority) ){

    if(top_heap[child1_pos].priority <
       top_heap[child2_pos].priority){
      //Child1 goes on top
      swap = top_heap[child1_pos];
      top_heap[child1_pos] = top_heap[pos];
      top_heap[pos] = swap;
      addr = pos;
      g_hash_table_insert(heap_pos,
                          GSIZE_TO_POINTER(top_heap[pos].node),
                          GSIZE_TO_POINTER(addr));
      pos = child1_pos;
      child1_pos = pos * 2 + 1;
      child2_pos = child1_pos + 1;
    }else{
      swap = top_heap[child2_pos];
      top_heap[child2_pos] = top_heap[pos];
      top_heap[pos] = swap;
      addr = pos;
      g_hash_table_insert(heap_pos,
                          GSIZE_TO_POINTER(top_heap[pos].node),
                          GSIZE_TO_POINTER(addr));
      pos = child2_pos;
      child1_pos = pos * 2 + 1;
      child2_pos = child1_pos + 1;
    }
  }
  if(child2_pos == top_heap_size &&
     (top_heap[pos].priority >
      top_heap[child1_pos].priority)){

    swap = top_heap[child1_pos];
    top_heap[child1_pos] = top_heap[pos];
    top_heap[pos] = swap;
    addr = pos;
    g_hash_table_insert(heap_pos,
                        GSIZE_TO_POINTER(top_heap[pos].node),
                        GSIZE_TO_POINTER(addr));
    pos = child1_pos;
  }
  addr = pos;
  g_hash_table_insert(heap_pos,
                      GSIZE_TO_POINTER(top_heap[pos].node),
                      GSIZE_TO_POINTER(addr));
}

void next_ull(){
  unsigned char count;
  LRU_ULL_t * next;
  lru = lru->next;
  next = lru->next;
  clean_ull(lru);
  if(lru != next){
    clean_ull(next);
    while(lru->nbr_elements + next->nbr_elements < ULL_CHUNK / 2 && lru != next){
      merge_ull();
      next = lru->next;
      clean_ull(next);
    }
  }
}

void clean_ull(LRU_ULL_t * ull){  //Removes the entries that have been deleted
  unsigned char count;
  while(ull->nbr_elements > 0 && ull->elements[ull->nbr_elements-1].page_addr == ULONG_MAX){
    ull->nbr_elements--;
  } //delete the empty elements at the end
  for(count = 0; count < ull->nbr_elements; count++){
    if(ull->elements[count].page_addr == ULONG_MAX){
      ull->elements[count] = ull->elements[ull->nbr_elements - 1];
      ull->nbr_elements--;
      g_hash_table_insert(page_LRU_addr, GSIZE_TO_POINTER(ull->elements[count].page_addr),(void*) &(ull->elements[count]));
      while(ull->nbr_elements > count &&
             ull->elements[ull->nbr_elements-1].page_addr == ULONG_MAX){
        ull->nbr_elements--;
      }
    }
  }
}
void iter(){
  //First we find a thread that has work to do
  unsigned char found_thread = 0;
  unsigned int next_thread;
  if(thread_count == 0){
    //Do nothing, just leave found_thread as 0
  }else if(thread_count == 1){
    next_thread = last_processed_thread;
    if(pipes[next_thread]->write_index == -1){
      free_thread(next_thread);
    }else if(pipes[next_thread]->write_index != pipes[next_thread]->read_index){
      found_thread = 1; //Then we process that one and only thread
    }
  }else{  //multiple threads
#ifndef NDEBUG
    assert(last_thread != 0);
#endif
    next_thread = (last_processed_thread + 1) % last_thread;
    do{
      if(pipes[next_thread] == 0
      || pipes[next_thread]->write_index == pipes[next_thread]-> read_index)
      {
        next_thread = (next_thread +1) % last_thread;
      }else if(pipes[next_thread]->write_index == -1){
        free_thread(next_thread);
        next_thread = (next_thread +1) % last_thread;
      }else{
        found_thread = 1;
        last_processed_thread = next_thread;
      }
    }while(found_thread == 0 && next_thread != (last_processed_thread +1 ) % last_thread);
  }
  if(found_thread == 0 /*|| (fadvise_count - (lru->next)->timestamp) >= MAX_FADVISE_CYCLE*/){
    unsigned char count;
    next_ull();
    lru->timestamp = fadvise_count;
    for(count = 0; count < lru->nbr_elements; count++){
      fadvise_wn(&lru->elements[count]);
    }
  }else{
    process_pipe(next_thread);
  }
}
void fadvise_wn(LRU_element_t* e){  //Calls fadvise will need
  unsigned long retval;
  if(TEST_MSB(e->page_addr)){
    retval= posix_fadvise(graph->fd_aux,
       UNSET_MSB(e->page_addr) * ASSUME_PAGE_SIZE,
       e->nbr_pages * ASSUME_PAGE_SIZE,
       POSIX_FADV_WILLNEED);
    //printf("%i, %lu, %u \n", graph->fd_aux, UNSET_MSB(e->page_addr) * ASSUME_PAGE_SIZE, e->nbr_pages * ASSUME_PAGE_SIZE);
    /*pthread_mutex_lock (&shared_hash);
    if(g_hash_table_lookup(fadvise_hash, GSIZE_TO_POINTER(UNSET_MSB(e->page_addr) * ASSUME_PAGE_SIZE)) == NULL){
        g_hash_table_insert(fadvise_hash,
                              GSIZE_TO_POINTER(UNSET_MSB(e->page_addr) * ASSUME_PAGE_SIZE),
                              GSIZE_TO_POINTER(clock()));
    }
    pthread_mutex_unlock (&shared_hash);
 */
  }else{
    retval= posix_fadvise(graph->fd_calist,
       e->page_addr * ASSUME_PAGE_SIZE,
       e->nbr_pages * ASSUME_PAGE_SIZE,
       POSIX_FADV_WILLNEED);
    pthread_mutex_lock (&shared_hash);
    if(g_hash_table_lookup(fadvise_hash, GSIZE_TO_POINTER(e->page_addr * ASSUME_PAGE_SIZE)) == NULL){
        g_hash_table_insert(fadvise_hash,
                              GSIZE_TO_POINTER(e->page_addr * ASSUME_PAGE_SIZE),
                              GSIZE_TO_POINTER(clock()));
    }
    pthread_mutex_unlock (&shared_hash);
  }
  if(retval) {
    perror("Prefetch failed: ");
    fprintf(stderr, "Get %ld %u\n", e->page_addr, e->nbr_pages);
    exit(-1);
  }else{
    fadvise_count += e->nbr_pages;
  }
}
void fadvise_dn(LRU_element_t* e){  //Calls fadvise dont need
  unsigned long retval;
  if(TEST_MSB(e->page_addr)){
    retval= posix_fadvise(graph->fd_aux,
       UNSET_MSB(e->page_addr) * ASSUME_PAGE_SIZE,
       e->nbr_pages * ASSUME_PAGE_SIZE,
       POSIX_FADV_DONTNEED);
  }else{
    retval= posix_fadvise(graph->fd_calist,
       e->page_addr * ASSUME_PAGE_SIZE,
       e->nbr_pages * ASSUME_PAGE_SIZE,
       POSIX_FADV_DONTNEED);
  }
  if(retval) {
    perror("Prefetch failed: ");
    fprintf(stderr, "Get %ld %u\n", e->page_addr, e->nbr_pages);
    exit(-1);
  }else{
    fadvise_count += e->nbr_pages;
  }
}
char fadvise_test(unsigned long page){
  gpointer ptr;
  pthread_mutex_lock (&shared_hash);
  ptr = g_hash_table_lookup(fadvise_hash, GSIZE_TO_POINTER(page));
  /*
  printf("Contents: \n");
  GHashTableIter iter;
  gpointer key, value;
  g_hash_table_iter_init (&iter, fadvise_hash);
  int a = 0;
  while (g_hash_table_iter_next (&iter, &key, &value))
    {
      printf("  %lu\n",GPOINTER_TO_SIZE(key));
    }
  */
  pthread_mutex_unlock (&shared_hash);
  if(ptr == NULL && (pipes[0]->pipe_array[0].node != pipes[0]->pipe_array[1].node)){
    printf("Not called yet on page %lu\n",page);
    return 1;
  }else{
  //  printf("Called %lu ago on page %lu \n", ((ulong) clock()) - GPOINTER_TO_SIZE(ptr), page);
    return 0;
  }
}
void launch_prefetch_thread(){
  int launch_code;
  assert(prefetch_thread_state == 0);
  assert(sizeof(void*) == sizeof(long));
  prefetch_thread_state = RUNNING;
  launch_code = pthread_create(&prefetch_thread,
			       NULL, prefetcher, NULL);
  if(launch_code) {
    perror("Failed to launch prefetch thread");
  }
}
void terminate_prefetch_thread()
{
  assert(prefetch_thread_state == RUNNING);
  prefetch_thread_state = TERMINATE;
  pthread_join(prefetch_thread, NULL);
  prefetch_thread_state = 0;
}
void free_lru(){
  LRU_ULL_t * next;
  LRU_ULL_t * end;
  LRU_ULL_t * tmp;
  next = lru->next;
  end = lru;

  //printf("lru value: %p\n", lru);
  free(lru);
  while(next != end){
    tmp = next->next;
    free(next);
    next = tmp;
  }
}

void destroy_prefetcher(){
  int i;
  assert(prefetch_thread_state == 0);
  free_lru();
  free(top_heap);
  g_hash_table_destroy(page_LRU_addr);
  g_hash_table_destroy(heap_pos);
  g_array_free(bottom_heap, TRUE);
  for(i=0; i<last_thread; i++){
    if(pipes[i] != 0){
      free(pipes[i]);
    }
  }
  free(pipes);
}


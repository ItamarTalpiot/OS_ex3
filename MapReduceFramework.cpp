#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <map>
#include <algorithm>
#include "Barrier/Barrier.h"

struct ThreadContext;

typedef struct ThreadContext{
    IntermediateVec intermediate_vec;
    OutputVec* output_vec;
    const InputVec* input_vec;
    std::atomic<int>* num_intermediate_elements;
    std::atomic<int>* num_output_elements;
    std::atomic<int>* curr_input_index;
    Barrier* barrier;
    MapReduceClient* client;
    int thread_id;
    int input_size;
    std::map<int, ThreadContext*> threads_context_map;
    std::vector<IntermediateVec*>* vectors_after_shuffle;
    std::atomic<int>* num_of_vectors_in_shuffle;
    std::atomic<int>* num_of_shuffled_elements;
    std::atomic<int>* reduce_running_index;
    pthread_mutex_t* mutex_on_reduce_stage;
    JobState *job_state;
    std::atomic<uint64_t>* atomic_counter;
} ThreadContext;


typedef struct{
    JobState *job_state;
    pthread_t *threads;
    int num_of_threads;
    const InputVec* inputVec;
    std::map<int, ThreadContext*> threads_context_map;
    OutputVec* output_vec;
    std::atomic<int>* curr_input_index;
    std::atomic<int>* num_intermediate_elements;
    std::atomic<int>* num_output_elements;
    std::atomic<int>* reduce_running_index;
    std::atomic<uint64_t>* atomic_counter;
} JobData;


void print_library_error(std::string str){
  std::cout << "system error: " << str << std::endl;
}

void emit2 (K2* key, V2* value, void* context){
  ThreadContext* tc = (ThreadContext *) context;
  IntermediatePair pair = IntermediatePair(key, value);
  tc->intermediate_vec.push_back (pair);
  (*(tc->num_intermediate_elements))++;
}


void emit3 (K3* key, V3* value, void* context){
  ThreadContext * tc = (ThreadContext*) context;
  OutputPair pair = OutputPair(key, value);
  tc->output_vec->push_back (pair);
  (*(tc->num_output_elements))++;
}

void getJobState(JobHandle job, JobState* state){
  JobData* jb = (JobData*) job;
  state->percentage = jb->job_state->percentage;
  state->stage = jb->job_state->stage;
}

typedef struct thread_args{
    MapReduceClient *client;
    JobData *job_data;
    int thread_id;
    int input_size;
} thread_args;

bool compare_intermediate_pair(const IntermediatePair& pair1, const IntermediatePair& pair2){
  return *(pair1.first) < *(pair2.first);
}

void sort_stage(void* context)
{
  ThreadContext *tc = (ThreadContext *) context;
  std::sort (tc->intermediate_vec.begin (), tc->intermediate_vec.end (), compare_intermediate_pair);
}

bool is_all_empty(ThreadContext* tc)
{
  for (auto it: tc->threads_context_map)
    {
      if (!it.second->intermediate_vec.empty ())
        {
          return false;
        }
    }
  return true;
}

K2* get_max_key(ThreadContext* tc){
  K2* max_key = nullptr;
  for(auto it: tc->threads_context_map){
    if (!it.second->intermediate_vec.empty()){
      K2* cur_key = it.second->intermediate_vec.at(0).first;
      if(max_key == nullptr){
        max_key = cur_key;
      }
      else{
        if(*max_key < *cur_key){
          max_key = cur_key;
        }
      }
    }
  }
  return max_key;
}

void shuffle(void* context){
  ThreadContext* tc = (ThreadContext*) context;
  std::vector<IntermediateVec*> shuffle_vec = std::vector<IntermediateVec*>();
  while(!is_all_empty(tc)){
    K2* max_key = get_max_key (tc);
    IntermediateVec new_vec = IntermediateVec();
    for(auto it: tc->threads_context_map){
      while(!it.second->intermediate_vec.empty() &&
          !(*(it.second->intermediate_vec.back().first) < *max_key)
          && !((*max_key) < *(it.second->intermediate_vec.back().first))){
        new_vec.push_back (it.second->intermediate_vec.back());
        it.second->intermediate_vec.pop_back();
          (*(tc->num_of_shuffled_elements))++; //up the index

          *tc->atomic_counter = (static_cast<uint64_t>(SHUFFLE_STAGE) & 3) |
                                            (static_cast<uint64_t>(tc->num_of_shuffled_elements->load()) << 2) |
                                            (static_cast<uint64_t>(tc->num_intermediate_elements->load()) << 33);

          uint64_t counter = tc->atomic_counter->load();
          uint32_t processedKeys = (counter >> 2) & 0x7FFFFFFF;
          uint32_t totalKeys = (counter >> 33) & 0x7FFFFFFF;

          std::cout << "Shuffle processed: " << processedKeys << " total: " << totalKeys << std::endl;
          tc->job_state->percentage = 100 *(processedKeys) / (totalKeys);


      }
    }
    shuffle_vec.push_back (&new_vec);
    (*(tc->num_of_vectors_in_shuffle))++;
  }
  tc->vectors_after_shuffle = &shuffle_vec;
}


void print_after_map_vector(void* context){
  ThreadContext* tc = (ThreadContext*)context;
  for (size_t i = 0; i < tc->intermediate_vec.size(); ++i) {
      std::cout << (tc->intermediate_vec[i].first) << " ";
    }
  std::cout << std::endl;
}

void print_input_vector(InputVec vec)
{
    std::cout << "input vec:" << std::endl;
    for (InputPair pair : vec) {
        std::cout << "Key: " << pair.first << ", Value: " << pair.second << std::endl;
    }
}

void print_iter_vector(IntermediateVec vec)
{
    std::cout << "iter vec:" << std::endl;
    for (IntermediatePair pair : vec) {
        std::cout << "Key: " << pair.first << ", Value: " << pair.second << std::endl;
    }
}


void* thread_run(void* arguments)
{
    ThreadContext * thread_context = (ThreadContext*) arguments;
    MapReduceClient* client = thread_context->client;
    int thread_id = thread_context->thread_id;
    std::atomic<int>* curr_index = thread_context->curr_input_index;
    int old_value;
    int input_size = thread_context->input_size;
    thread_context->job_state->stage = MAP_STAGE;

    *thread_context->atomic_counter = (static_cast<uint64_t>(MAP_STAGE) & 3) |
                                      (static_cast<uint64_t>(0) << 2) |
                                      (static_cast<uint64_t>(input_size) << 33);

    std::cout << "running thread" << thread_id << std::endl;

    while((curr_index->load() < input_size) && ((old_value = (*curr_index)++) < input_size))
    {
        std::cout << "old" << old_value << std::endl;
        InputPair pair = (*thread_context->input_vec)[old_value];
        client->map(pair.first, pair.second, (void*)thread_context);
        std::cout << old_value << std::endl;

        *thread_context->atomic_counter = (static_cast<uint64_t>(MAP_STAGE) & 3) |
                                          (static_cast<uint64_t>(curr_index->load()) << 2) |
                                          (static_cast<uint64_t>(input_size) << 33);

        uint64_t counter = thread_context->atomic_counter->load();
        uint32_t processedKeys = (counter >> 2) & 0x7FFFFFFF;
        uint32_t totalKeys = (counter >> 33) & 0x7FFFFFFF;

        std::cout << "processed: " << processedKeys << " total: " << totalKeys << std::endl;
        thread_context->job_state->percentage = 100 *(processedKeys) / (totalKeys);
    }

    std::cout << "thread" << thread_id << "finished mapping" << std::endl;

    print_iter_vector(thread_context->intermediate_vec);

    sort_stage((void*)thread_context);

    std::cout << "enetring  barrier" << thread_id << std::endl;
    thread_context->barrier->barrier();
    std::cout << "enetring shuffle after barrier" << thread_id << std::endl;

    // changing to shuffle
    uint64_t counter = thread_context->atomic_counter->load();
    *thread_context->atomic_counter = (static_cast<uint64_t>(SHUFFLE_STAGE) & 3) |
                                      (static_cast<uint64_t>(0) << 2) |
                                      (static_cast<uint64_t>(thread_context->num_intermediate_elements->load()) << 33);
    thread_context->job_state->stage = SHUFFLE_STAGE;
    thread_context->job_state->percentage = 0;

  //shuffle if thread_id_is_0
    if(thread_context->thread_id == 0){
        shuffle ((void*)thread_context);
        std::cout << "0 finished shuffle" << thread_id << std::endl;
        thread_context->job_state->percentage = 100;
    }

    thread_context->barrier->barrier();

    std::cout << "starting reduce stage after barrier" << thread_id << std::endl;
    thread_context->job_state->stage = REDUCE_STAGE;
    thread_context->job_state->percentage = 0;

    while (!thread_context->vectors_after_shuffle->empty()){
        pthread_mutex_lock (thread_context->mutex_on_reduce_stage);
        if (!thread_context->vectors_after_shuffle->empty()){
            thread_context->
          client->reduce ((*(thread_context->vectors_after_shuffle)).at(0), (void*)thread_context);
            *(thread_context->reduce_running_index)+= thread_context->vectors_after_shuffle->size();
            (*(thread_context->vectors_after_shuffle)).erase((*(thread_context->vectors_after_shuffle)).begin());
            thread_context->job_state->percentage = thread_context->reduce_running_index->load() / thread_context->num_intermediate_elements->load();
        }
        pthread_mutex_unlock (thread_context->mutex_on_reduce_stage);
    }

    std::cout << "finished all " << thread_id << std::endl;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    pthread_t* threads = new pthread_t[multiThreadLevel];
//    pthread_t threads[multiThreadLevel];
    JobState* j_state = (JobState*) malloc(sizeof(JobState));
    if (j_state == NULL) {
        print_library_error ("Failed to allocate memory for JobState");
        exit(1);
    }
    j_state->stage = UNDEFINED_STAGE;
    j_state->percentage = 0.0f;

    // Allocate and initialize JobData
    JobData* job_data = (JobData*) malloc(sizeof(JobData));
    if (job_data == NULL) {
        print_library_error ("Failed to allocate memory for JobData");
        delete[](threads);
        free(j_state);
        exit(1);
    }

    job_data->job_state = j_state;
    job_data->threads = threads;
    job_data->num_of_threads = multiThreadLevel;
    job_data->inputVec = &inputVec;
    job_data->threads_context_map = std::map<int, ThreadContext*>();
    job_data->output_vec = &outputVec;
    job_data->curr_input_index = new std::atomic<int>(0);
    job_data->num_intermediate_elements = new std::atomic<int>(0);
    job_data->num_output_elements = new std::atomic<int>(0);
    job_data->reduce_running_index = new std::atomic<int>(0);
    job_data->atomic_counter = new std::atomic<uint64_t>(0);

    print_input_vector(inputVec);

    Barrier* barrier = new Barrier(multiThreadLevel);
    pthread_mutex_t* mutex_on_reduce_stage;

    int inputSize = inputVec.size();
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        ThreadContext * threadContext = (ThreadContext*) malloc(sizeof(ThreadContext));
        threadContext->threads_context_map = job_data->threads_context_map;
        threadContext->num_output_elements = job_data->num_output_elements;
        threadContext->curr_input_index = job_data->curr_input_index;
        threadContext->num_intermediate_elements = job_data->num_intermediate_elements;
        threadContext->reduce_running_index = job_data->reduce_running_index;
        threadContext->output_vec = &outputVec;
        threadContext->intermediate_vec = IntermediateVec ();
        threadContext->input_vec = &inputVec;
        threadContext->barrier = barrier;
        threadContext->thread_id = i;
        threadContext->input_size = inputSize;
        threadContext->client = const_cast<MapReduceClient*>(&client);
        threadContext->job_state = j_state;
        threadContext->atomic_counter = job_data->atomic_counter;
        threadContext->mutex_on_reduce_stage = mutex_on_reduce_stage;
        std::cout << "allocated thread" << i << std::endl;

        job_data->threads_context_map[i] = threadContext;

        //start_index, end_index = get_partition(size, thread_id)
        if (1 || threadContext->thread_id < inputSize)  //TODO: in free check if thread before free
        {
            pthread_create(threads+i, NULL, thread_run, (void*)threadContext);
        }
    }


    return (void*) job_data;
}


void waitForJob(JobHandle job)
{
    JobData* job_data = (JobData*) job;


    for (int i = 0; i < job_data->num_of_threads; ++i) {
        pthread_join(job_data->threads[i], NULL);
    }

    // TODO: change state to ended
    closeJobHandle(job);
}

void closeJobHandle(JobHandle job)
{
    JobData * job_data = (JobData*) job;
    if (job_data->job_state->stage != REDUCE_STAGE || job_data->job_state->percentage != 100)
    {
        waitForJob(job);
    }

    if (job)
    {
        if (job_data->threads)
            delete[] job_data->threads;
        if (job_data->job_state)
            free(job_data->job_state);
        for (int i=0; i < job_data->num_of_threads; i++)
        {
            ThreadContext * curr_context = job_data->threads_context_map[i];

            if (curr_context)
            {
                if (curr_context->barrier)
                    delete curr_context->barrier;
            }
        }

        free(job_data);
    }
}


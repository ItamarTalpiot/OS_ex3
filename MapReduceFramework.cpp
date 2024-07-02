#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <map>
#include <algorithm> // for std::sort
#include "Barrier/Barrier.h"


typedef struct{
    IntermediateVec intermediate_vec;
    OutputVec& output_vec;
    InputVec& input_vec;
    std::atomic<int>* num_intermediate_elements;
    std::atomic<int>* num_output_elements;
    std::atomic<int>* curr_input_index;
    Barrier* barrier;
    MapReduceClient* client;
    int thread_id;
    int input_size;
} ThreadContext;


typedef struct{
    JobState *job_state;
    pthread_t *threads;
    int num_of_threads;
    InputVec& inputVec;
    std::map<int, ThreadContext*> threads_context_map;
    OutputVec& output_vec;
    std::atomic<int>* curr_input_index;
    std::atomic<int>* num_intermediate_elements;
    std::atomic<int>* num_output_elements;
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
  tc->output_vec.push_back (pair);
  (*(tc->num_output_elements))++;
}

void getJobState(JobHandle job, JobState* state){
  JobData* jb = (JobData*) job;
  jb->job_state->percentage = state->percentage;
  jb->job_state->stage = state->stage;
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

void sort_stage( void* context){
  ThreadContext* tc = (ThreadContext*) context;
  std::sort(tc->intermediate_vec.begin(), tc->intermediate_vec.end(), compare_intermediate_pair);
}

void print_after_map_vector(void* context){
  ThreadContext* tc = (ThreadContext*)context;
  for (size_t i = 0; i < tc->intermediate_vec.size(); ++i) {
      std::cout << (tc->intermediate_vec[i].first) << " ";
    }
  std::cout << std::endl;
}

void* thread_run(void* arguments)
{
    ThreadContext * thread_context = (ThreadContext*) arguments;
    MapReduceClient* client = thread_context->client;
    int thread_id = thread_context->thread_id;
    std::atomic<int>* curr_index = thread_context->curr_input_index;
    int old_value;

    while((old_value = (*curr_index)++) < thread_context->input_size)
    {
        InputPair pair = thread_context->input_vec[old_value];
        client->map(pair.first, pair.second, (void*)thread_context);
    }

    sort_stage((void*)thread_context);

    thread_context->barrier->barrier();

  //shuffle if thread_id_is_0

    //block

    //reduce

    //end
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
    job_data->inputVec = inputVec;
    job_data->threads_context_map = std::map<int, ThreadContext*>();
    job_data->output_vec = outputVec;
    *(job_data->curr_input_index) = 0;
    *(job_data->num_intermediate_elements) = 0;
    *(job_data->num_output_elements) = 0;

    Barrier* barrier = new Barrier(multiThreadLevel);

    int inputSize = inputVec.size();
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        ThreadContext * threadContext = (ThreadContext*) malloc(sizeof(ThreadContext));
        threadContext->num_output_elements = job_data->num_output_elements;
        threadContext->num_intermediate_elements = job_data->num_intermediate_elements;
        threadContext->output_vec = outputVec;
        threadContext->intermediate_vec = IntermediateVec ();
        threadContext->input_vec = inputVec;
        threadContext->barrier = barrier;
        threadContext->thread_id = i;
        threadContext->input_size = inputSize;
        threadContext->client = const_cast<MapReduceClient*>(&client);

        job_data->threads_context_map[i] = threadContext;

        //start_index, end_index = get_partition(size, thread_id)
        if (threadContext->thread_id < inputSize)  //TODO: in free check if thread before free
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


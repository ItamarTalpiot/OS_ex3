#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>
#include <map>


typedef struct{
    IntermediateVec intermediate_vec;
    OutputVec& output_vec;
    std::atomic<int>* num_intermediate_elements;
    std::atomic<int>* num_output_elements;

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
  std::cout << str << std::endl;
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

void thread_run(void* arguments)
{
    thread_args* t_args = (thread_args*) arguments;
    MapReduceClient* client = t_args->client;
    JobData* job_data = t_args->job_data;
    int thread_id = t_args->thread_id;
    ThreadContext * thread_context = job_data->threads_context_map[thread_id];
    std::atomic<int>* curr_index = job_data->curr_input_index;
    int old_value;

    while((old_value = (*curr_index)++) < t_args->input_size)
    {
        InputPair pair = job_data->inputVec[old_value];
        client->map(pair.first, pair.second, (void*)thread_context);

    }

    //block

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
    JobState* j_state = (JobState*) malloc(sizeof(JobState));
    if (j_state == NULL) {
        std::cout << "Failed to allocate memory for JobState";  //TODO: error handle
        exit(1);
    }
    j_state->stage = UNDEFINED_STAGE;
    j_state->percentage = 0.0f;


    // Allocate and initialize JobData
    JobData* job_data = (JobData*) malloc(sizeof(JobData));
    if (job_data == NULL) {
        perror("Failed to allocate memory for JobData"); //TODO: error handle
        free(threads);
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

    int inputSize = inputVec.size();
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        thread_args* t_args = (thread_args*) malloc(sizeof(thread_args));
        t_args->client = const_cast<MapReduceClient*>(&client);
        t_args->job_data = job_data;
        t_args->thread_id = i;
        t_args->input_size = job_data->inputVec.size();

        ThreadContext * threadContext = (ThreadContext*) malloc(sizeof(ThreadContext));
        threadContext->num_output_elements = job_data->num_output_elements;
        threadContext->num_intermediate_elements = job_data->num_intermediate_elements;
        threadContext->output_vec = outputVec;
        threadContext->intermediate_vec = IntermediateVec ();

        t_args->job_data->threads_context_map[i] = threadContext;

        //start_index, end_index = get_partition(size, thread_id)
        if (t_args->thread_id < multiThreadLevel)  //TODO: in free check if thread before free
        {
            pthread_create(threads + i, NULL, thread_run, (void*)t_args);
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

int main(int argc, char** argv){
  int num_of_threads = 8;
  int size = 3;
  for(int i = 0; i < num_of_threads; i++){
    std::pair<int, int> pair = get_partition(i, size, num_of_threads);
    int start = pair.first;
    int end = pair.second;
    std::cout << "id: " << i << " got: start: " << start << " end: " << end << std::endl;
  }
}

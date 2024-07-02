#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <iostream>

typedef struct{
    JobState *job_state;
    pthread_t *threads;
    int num_of_threads;
    InputVec inputVec;
    IntermediateVec intermediate_vec;
    OutputVec output_vec;
    std::atomic<int>* num_intermediate_elements;
    std::atomic<int>* num_output_elements;
} JobData;

void emit2 (K2* key, V2* value, void* context){
  JobData* jb = (JobData*) context;
  IntermediatePair pair = IntermediatePair(key, value);
  jb->intermediate_vec.push_back (pair);
  (*(jb->num_intermediate_elements))++;
}


void emit3 (K3* key, V3* value, void* context){
  JobData* jb = (JobData*) context;
  OutputPair pair = OutputPair(key, value);
  jb->output_vec.push_back (pair);
  (*(jb->num_output_elements))++;
}

void getJobState(JobHandle job, JobState* state){
  JobData* jb = (JobData*) job;
  jb->job_state->percentage = state->percentage;
  jb->job_state->stage = state->stage;
}

typedef struct thread_args{
    MapReduceClient *client;
    JobData *context;
    int thread_id;
    int start_index;
    int end_index;
} thread_args;

void thread_run(void* arguments)
{
    thread_args* t_args = (thread_args*) arguments;
    MapReduceClient* client = t_args->client;
    JobData* context = t_args->context;
    int thread_id = t_args->thread_id;

    //map
    for (int i=t_args->start_index; i < t_args->end_index; i++)
    {
        K1 k = context->
        client->map()

    }

    //block

    //shuffle if thread_id_is_0

    //block

    //reduce

    //end
}

// when gets called, return the right partition to the thread id ([0, num_of_threads - 1])
std::pair<int, int> get_partition(int thread_id, int size, int num_of_threads){
  int base_size = size / num_of_threads;
  int remainder = size % num_of_threads;
  int start = thread_id * base_size + std::min(thread_id, remainder);
  int end = start + base_size;
  if (thread_id < remainder) {
      end++;
  }
  return std::make_pair(start, end);
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
        perror("Failed to allocate memory for JobData");
        free(threads);
        free(j_state);
        return EXIT_FAILURE;
    }
    job_data->job_state = j_state;
    job_data->threads = threads;

    int inputSize = inputVec.size();
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        thread_args* t_args = (thread_args*) malloc(sizeof(thread_args));
        t_args->client = const_cast<MapReduceClient*>(&client);
        t_args->context = job_data;
        t_args->thread_id = i;
        //start_index, end_index = get_partition(size, thread_id)
        if (t_args->thread_id < multiThreadLevel)  //TODO: in free check if thread before free
        {
            pthread_create(threads + i, NULL, thread_run, (void*)t_args);
        }
    }


    return (void*) job_data ;
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

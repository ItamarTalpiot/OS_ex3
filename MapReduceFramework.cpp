#include "MapReduceFramework.h"
#include <pthread.h>


typedef struct{
    JobState *job_state;
    pthread_t *threads;
    int num_of_threads;
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

    // map


void getJobState(JobHandle job, JobState* state){
  JobData* jb = (JobData*) job;
  jb->job_state->percentage = state->percentage;
  jb->job_state->stage = state->stage;
}

typedef struct thread_args{
    const MapReduceClient &client;
    JobData *context;
    int thread_id;
    int start_index;
    int end_index;
} thread_args;

void thread_run(void* arguments)
{
    thread_args* args = (thread_args*) arguments;

    //block


    //shuffle if thread_id_is_0

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
        t_args->client = client;
        t_args->context = job_data;
        t_args->thread_id = i;


        pthread_create(threads + i, NULL, thread_run, contexts + i);
    }


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
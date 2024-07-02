#include "MapReduceFramework.h"
#include <pthread.h>


typedef struct{
    JobState *job_state;
    pthread_t threads[];
} JobData;

void emit2 (K2* key, V2* value, void* context){


}


void getJobState(JobHandle job, JobState* state){
  JobData* jb = (JobData*) job;
  jb->job_state->percentage = state->percentage;
  jb->job_state->stage = state->stage;
}






JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    pthread_t* threads = new pthread_t[multiThreadLevel];

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(threads + i, NULL, client.map, contexts + i);
    }

    for (int i = 0; i < MT_LEVEL; ++i) {
        pthread_join(threads[i], NULL);
    }
}

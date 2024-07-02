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







#!/usr/bin/env python3

# from typing import List
import uvicorn
from fastapi import FastAPI, Body #, Query
from pydantic import BaseModel, Schema
from starlette.responses import Response
from starlette.requests import Request
import logging
import argparse
import time
import asyncio
import numpy as np

from da_circo_const import Job, State
#from da_circo_scheduler import JobManager
from da_circo_k8s_scheduler import JobManager

app = FastAPI(title = "Da Circo API ")

@app.get("/jobs")
async def get_jobs():
    """ Handler for the GET /jobs request.
    
    Returns the list of all active and terminated jobs
    with their state.
    """    
    return mngr.getAllJobs()

@app.get("/jobs/{jobId}")
async def get_jobs(jobId: int) -> str:
    """ Handler for the GET /jobs/{jobID} request.
    
    Returns information about job #jobId

    :return: the description of the job

    :param jobId: the ID of the job
    """    
    return mngr.getJob(str(jobId))

@app.get("/jobs/{jobId}/state")
async def get_job_state(jobId: int) -> State:
    """ Handler for the GET /jobs/{jobID}/state request.
    
    Returns information the stat of job #jobId

    :return: the state of the job

    :param jobId: the ID of the job
    """    
    return mngr.getStateJob(str(jobId))

@app.get("/stats")
async def get_stats():
    """ Handler for the GET /stats request.

    Returns some statistics about all jobs completed
    """
    ret = {}
    counter = 0
    counter_error = 0
    counter_completed = 0
    list_duration = list()
    for j in mngr.transcode_requests:
        counter = counter + 1
        task = mngr.transcode_requests[j]
        if task.state == State.ERROR:
            counter_error = counter_error + 1
        elif task.state == State.COMPLETED:
            counter_completed += 1
            list_duration.append(task.duration)

    if not counter_completed:
        percent = 0
        aver = 0
    else:
        percent = np.percentile(np.array(list_duration), np.array(95))
        aver = np.average(np.array(list_duration))

    ret["completed ratio"]=float(counter_completed/counter)
    ret["duration 95th percentile"] = percent
    ret["average"] = aver

    return ret




@app.post("/jobs", status_code = 201)
async def create_job(response: Response,
               request: Request,
               job: Job = Body(
                   ...,
                   example={
                       "id_video": "bbb_0.mp4",
                       "bitrate": 7000,
                       "speed": "ultrafast",
                       },
                   )
               ):
    """ Handler for the POST /jobs/{"id_video", "bitrate", "speed"} request. 
    
    Ask Da Circo Job Manager to create a new job to handle this request.  
    Returns the Id of the created job. 

    :param request: the received request
    :param response: the answer sent in response of the request 
    :param job: the parameters of the job to create 
    """    
    

    # get an ID and return to client
    id_job = mngr.getID()
    logger.debug("got id_job %s" %id_job)
    resp = ["http:/"]
    resp.append(request.headers['host'])
    resp.append(id_job)
    response.headers["Location"] = "/".join(resp)

    # create the task
    mngr.newJob(id_job, 
                job.id_video, 
                job.bitrate, 
                job.speed)

    return id_job

if __name__ == "__main__":
    ### Command line arguments parsing
    parser = argparse.ArgumentParser(description='The DaCirco API')
    parser.add_argument('-p', '--port',
                    dest='port',
                    help='(default:  \'%(default)s\') port on host to send requests to',
                    required=False,
                    type=int,
                    default='9000')
    parser.add_argument('-d', '--debug',
                    dest='debugFlag',
                    help='Raise the log level to debug',
                    action="store_true",
                    default=False)
    parser.add_argument('-c', '--cloud',
                    dest='cloud',
                    help='(default:  \'%(default)s\') name of the cloud description in the cloud.yaml file',
                    required=False,
                    default='openstack')
    parser.add_argument('-k', '--keypair',
                    dest='keypair',
                    help='(default:  \'%(default)s\') name of the keypair to use (as declared on the OpenStack controller)',
                    required=False,
                    default='os-tp-key')
    parser.add_argument('-i', '--key-file',
                    dest='key_file',
                    help='(default:  \'%(default)s\') path of the local private keyfile ',
                    required=False,
                    default='./os-tp-key')
    args = parser.parse_args()

    ### Log level configuration
    logging.basicConfig(format='%(message)s', level=logging.WARNING)
    logger = logging.getLogger(__name__)

    if args.debugFlag == True:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO

    modules = [__name__, 'da_circo_scheduler', 'da_circo_vm_manager', 'da_circo_k8s_manager']
    for m in modules:
        logging.getLogger(m).setLevel(logLevel)

    # Instantiate the Job Manager
    mngr = JobManager()

    # Start the REST API (Web App)
    uvicorn.run(app, port=args.port, log_level="info")

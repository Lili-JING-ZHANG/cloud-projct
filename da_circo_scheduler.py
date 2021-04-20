#! /usr/bin/python3
# coding: utf-8

import logging
logger = logging.getLogger(__name__)
import argparse
from typing import Union, Dict
import da_circo_vm_manager as vm_manager
import time
import openstack
import asyncio
import threading
from da_circo_const import State

state = State

class JobManager:
    """ The Scheduler for daCirco.

    It is instantiated when da_circo_API is started. It then \
    reacts to call from the API when new jobs are requested. It \
    deals with the jobs.

    :param cloud: The name of the cloud description in the cloud.yaml file
    :param keypair: The name of the keypair to use \
                    (as declared on the OpenStack controller)
    :param key_file: The path of the local private keyfile 
    """

    def __init__(self, cloud: str = 'openstack', 
                 keypair: str = 'os-tp-key', 
                 key_file :str = './os-tp-key'):
        """ 
        :param cloud: The name of the cloud description in the cloud.yaml file
        :param keypair: The name of the keypair to use \
                        (as declared on the OpenStack controller)
        :param key_file: The path of the local private keyfile 
        """
        self.cloud = cloud
        self.keypair = keypair
        self.key_file = key_file

        # dictionary with jobID and transcoding request objects
        self.transcode_requests = {}

        # ================================================
        # ================================================
        #
        #
        # put here everything you need to deal with jobs
        # and the created virtual machines
        #
        #
        # ================================================


    def getID(self) -> str:
        """ Create an unique ID for a new transcoding request.

        :return: the unique job ID
        """
        now = str(time.time())
        jobID = "".join(now.split("."))
        # logger.info("New JobID created: %s" %str(self.jobID))

        return jobID

    def newJob(self, id_job: str, id_video: str, bitrate: int, speed: str):
        """  Create a new job.

        :param id_job: the id of the job (got with getID())
        :param id_video: the name of the video that should be transcoded
        :param bitrate: in [500, 8000]. the target bit-rate
        :param speed: in {"ultrafast", "fast"}. the encoding setting
        """

        # create an object for the transcoding request
        new_task = Task(self,
                        id_job,
                        id_video,
                        bitrate,
                        speed,
                        cloud = self.cloud, 
                        keypair = self.keypair, 
                        key_file = self.key_file,)
        logger.info("%s task created" %id_job)


        # save in the dictionary
        self.transcode_requests[id_job] = new_task

        # =================================================
        # =================================================
        #
        # here you should implement what you plan to do with the
        # task that was just created.
        # Tips: "new_task.start()" executes the
        # function new_task.run() as a Thread.
        #
        # ================================================
        # ================================================


    def getJob(self, id_job: str) -> str:
        """ The getter for a single job description.

        :return: the description of the task

        :param id_job: the unique identifier of the job
        """

        if id_job in self.transcode_requests.keys():
            return {"job": str(self.transcode_requests[id_job])}
        else:
            return {}

    def getAllJobs(self) -> Dict[str, Dict[bool, float]]:
        """ The getter for the list of all jobs.

        :return: a dictionnary indexed by JobIDs giving the state and \
                 duration for each job
        """
        ret = {}
        for j in self.transcode_requests:
            d = {}
            task = self.transcode_requests[j]
            d['state'] = task.state
            if task.state == state.COMPLETED:
               d['duration'] = task.duration 
            ret[j] = d

        return ret

    def getStateJob(self, id_job: str) -> State:
        """ The getter for the state of the Task id_job.

        :return: the state of the job

        :param id_job: the unique identifier of the job
        """

        if id_job in self.transcode_requests.keys():
            return {"state": self.transcode_requests[id_job].state}
        else:
            return {}





class Task(threading.Thread):
    """ The class that deal with the trancoding requests.

    :param job_manager: the calling Job Manager
    :param id_job: the id of the job
    :param movie_name: the name of the movie to be transcoded
    :param bitrate: in [500, 8000] the target bit-rate
    :param encoding_quality: in {"ultrafast", "fast"}
    :param cloud: The name of the cloud description in the cloud.yaml file
    :param keypair: The name of the keypair to use \
                    (as declared on the OpenStack controller)
    :param key_file: The path of the local private keyfile 
    """

    def __init__(self,
                 job_manager: JobManager,
                 id_job: str,
                 movie_name: str,
                 bitrate: int,
                 encoding_quality: str,
                 cloud: str = 'openstack', 
                 keypair: str = 'os-tp-key',
                 key_file: str = './os-tp-key',
                 *args,
                 **kwargs):
        """
        :param job_manager: the calling Job Manager
        :param id_job: the id of the job
        :param movie_name: the name of the movie to be transcoded
        :param bitrate: in [500, 8000] the target bit-rate
        :param encoding_quality: in {"ultrafast", "fast"}
        :param cloud: The name of the cloud description in the cloud.yaml file
        :param keypair: The name of the keypair to use \
                        (as declared on the OpenStack controller)
        :param key_file: The path of the local private keyfile 
        """
        # Call parent constructor (the one of Thread)
        super(Task, self).__init__(*args, **kwargs)
        self.daemon = True
        # Enables to cleanly kill the calling program with Ctrl + C
        # (see: https://stackoverflow.com/questions/1635080/terminate-a-multi-thread-python-program)

        self.id_job = id_job
        self.name = movie_name
        self.bitrate = bitrate
        self.speed = encoding_quality
        self.cloud = cloud
        self.keypair = keypair
        self.key_file = key_file

        # to get statistics on the encoding time
        self.creation_time = time.time()
        self.duration = 0

        self.state = state.WAITING

        # the VM that will be in charge of transcoding
        self.vm = 0

        # ================================================
        # ================================================
        #
        #
        # put here everything you need to deal with the job
        # in particular the management of the VM
        #
        #
        # ================================================


    def __str__(self):
        sentence = ("task %s for movie %s at bitrate %d and speed %s" 
                                    %(self.id_job,
                                    self.name,
                                    self.bitrate,
                                    self.speed))
        return sentence


    def run(self) -> bool:
        """ Run the job.
        
        This method is automatically called by the :func:`Start()` \
        method inherited from :class:`Thread`).

        :return: False if an Error occurred during config, True otherwise
        """

        # create connection to OpenStack
        try:
            conn = openstack.connect(cloud=self.cloud)
        except:
            logger.warning("%s ERROR connecting to OpenStack -> skip job" %self.id_job)
            self.state = state.ERROR
            return False

        # ===================================================
        # ===================================================
        #
        # Here you launch the action of transcoding 
        # the video.
        # Tip for the first part of the TP:
        #   - create a VM,
        #   - wait for the VM to be ready (in particular answer to 
        #     ping and accept ssh),
        #   - copy the transcoding programs in the created VM,
        #   - configure necessary entries in /etc/hosts (for controller)
        #   - run the transcoding program with appropriate parameters, 
        #   - and, once it is done, kill the VM
        #
        # If needed, you can also enrich the exception 
        # management bellow
        #
        # ===================================================
        self.state = state.STARTED
        return True
        

    def is_object_in_store(self,
                           conn: openstack.connection.Connection,
                           obj: str, container: str):
        """ Test if an object is present in a given container of Swift.

        :return: True if the object is in Swift which \
                 would mean that the video has been \
                 transcoded. False otherwise.

        :param conn: the configuration to access OS
        :param obj: the object that is looked for
        :param container: the name of the container where to look
        """
        try:
            conn.object_store.get_object_metadata(
                obj,
                container=container)
            return True
        except openstack.exceptions.ResourceNotFound as e: 
            return False


### Start Application if directly called from command line
if __name__ == "__main__":
    
    ### Command line arguments parsing
    parser = argparse.ArgumentParser(description='The DaCirco Scheduler')
    parser.add_argument('-d', '--debug',
                        dest='debugFlag',
                        help='Raise the log level to debug',
                        action="store_true",
                        default=False)

    args = parser.parse_args()


    ### Log level configuration
    if args.debugFlag == True:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.WARNING
    logging.basicConfig(level=logLevel)


#!/usr/bin/env python3

# This module is inspired from the Kubernetes python client examples 
# at https://github.com/kubernetes-client/python/blob/master/examples/pod_exec.py#

import logging
logger = logging.getLogger(__name__)
from typing import Union, Dict

import time

from kubernetes import config, client
from kubernetes.client import Configuration
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.client import V1Job


class K8sManager():
    """  Class to manage a Kubernetes cluster.

    It includes methods to handle pods and jobs. 

    The connection to the k8s cluster is initiated based on  \
    information found in the local k8s configuration file \
    (eg ~/.kube/config). See Kubernetes documentation for more \
    information about confiuration files.
    """
    def __init__(self):
        """
        Constructor: initiate the connection to the k8s cluster based on \
        information found in the local k8s configuration file \
        (eg ~/.kube/config). See Kubernetes documentation for more \
        information about confiuration files.
        """
        
        self.core_api = None
        self.batch_api = None
        try:
            config.load_kube_config()
            # c = Configuration()
            # c.assert_hostname = False
            # Configuration.set_default(c)

            self.core_api = client.CoreV1Api()
            self.batch_api = client.BatchV1Api()

        except:
            logger.error("Did not manage to connect to k8s")
            raise

    #################################################################
    ### Methods to handle PODs
    #################################################################
    def pod_exists_in_namespace(self, 
                                pod_name: str, 
                                namespace: str = 'default') -> bool:
        """ Checks if a pod with the given name already exists in the \
            cluster

        :return: True if the pod exists, False otherwise

        :param pod_name: name of the pod to look for
        :param namespace: namespace where to look for the pod
        """
        resp = None
        try:
            resp = self.core_api.read_namespaced_pod(name=pod_name,
                                                    namespace=namespace)
        except :
            pass

        return True if resp else False

    def execute_container_in_pod(self,
                            manifest: Dict,
                            container_name: str, 
                            pod_name: str = None, 
                            namespace: str = 'default'):
        """ Creates a new pod based on the manifest specification, \
            starts the execution of the specified container and delete \
            the pod when finished.

        :param manifest: dictionary decribing the pod as in the yaml \
                         syntax
        :param container_name: name of the container to execute. \
                               This container should be described in \
                               the pod manifest
        :param pod_name: if specified, this parameter overrides the \
                         name of the pod to create
        :param namespace: namespace where to create the pod in
        """

        # Geting the Pod Name from the manifest or from the optional parameter
        if pod_name:
            manifest['metadata']['name'] = pod_name
        else: 
            pod_name = manifest['metadata']['name']
        

        # Create the Pod
        logger.debug("Creating the pod %s..." % pod_name)

        self.core_api.create_namespaced_pod(body=manifest,
                                           namespace=namespace)
        # Wait for pod creation finished
        while True:
            resp = self.core_api.read_namespaced_pod(name=pod_name,
                                                    namespace=namespace)
            if resp.status.phase != 'Pending':
                break
            time.sleep(1)
        logger.debug("\tPod %s created" % pod_name)


        # Wait for end of Pod execution 
        logger.debug("Pod %s executing, wating..." % pod_name)
        start_time = time.time()
        resp = stream(self.core_api.connect_get_namespaced_pod_attach,
                      name=pod_name,
                      namespace=namespace,
                      container=container_name,
                      stderr=True, stdin=False,
                      stdout=True, tty=False,
        )
        duration = time.time() - start_time
        logger.debug("\tExecution time: " + str(duration))
        logger.debug("\tPod output:\n" + resp)


        # Delete the Pod
        logger.debug("Pod %s Finished: Now deleting the pod..."  % pod_name)
        self.core_api.delete_namespaced_pod(name=pod_name,
                                            namespace=namespace)
        logger.debug("\tPod %s Deleted" % pod_name)


    def wait_for_pod_creation(self, pod_name: str, 
                              namespace: str = 'default',
                              timeout: int = 180, 
                              retry_interval: float = 1) -> bool:
        """ Wait for a pod to leave the 'Pending' state

        :return: True if pod was successfully created, \
                 False otherwise (timeout)
                    
        :param pod_name: name of the pod to wait for
        :param namespace: namespace of the pod
        :param timeout: The maximum waiting time (in seconds)
        :param retry_interval: The delay between successive tests \
                               (in seconds)
        """
        retry_interval = float(retry_interval)
        timeout = int(timeout)
        timeout_start = time.time()

        while time.time() < timeout_start + timeout:
            time.sleep(retry_interval)
            resp = self.core_api.read_namespaced_pod(name=pod_name,
                                                    namespace=namespace)
            if resp.status.phase != 'Pending':
                return True
        return False




    #################################################################
    ### Methods to handle JOBs
    #################################################################
    def execute_job(self,
                    manifest: Union[Dict, V1Job],
                    job_name: str = None, 
                    namespace: str = 'default'):
        """ Creates a new pod based on the manifest specification, \
        starts the execution of the specified container and delete the \
        pod when finished.

        :param manifest: dictionary decribing the pod as in the yaml \
                         syntax
        :param job_name: if specified, this parameter overrides the \
                         name of the pod to create
        :param namespace: namespace where to create the pod in
        """
        # Geting the Job Name from the manifest or from the optional parameter
        if job_name:
            manifest['metadata']['name'] = job_name
        else: 
            job_name = manifest['metadata']['name']

        # Create the job 
        self.batch_api.create_namespaced_job(
        #     body=job,
            body=manifest,
            namespace="default",
            )

        # Wait for end of Job execution 
        logger.debug("Jod %s executing, wating..." % job_name)
        start_time = time.time()
        self.wait_for_job_completion(job_name, namespace=namespace)
        duration = time.time() - start_time
        logger.debug("\tExecution time: " + str(duration))

        # Retrieve and display the Job execution trace
        res = self.get_job_output(job_name)
        logger.debug("\tJob output:\n" + res)


        # Delete the Job and associated Pods
        logger.debug("Deleting Job %s..." % job_name)
        self.delete_job(job_name)



    def wait_for_job_completion(self, 
                                job_name: str, 
                                namespace: str = 'default',
                                timeout: int = 180, 
                                retry_interval: float = 1) -> bool:
        """ Wait for a job to complete.

        :return: True if job successfully completed, \
                 False otherwise (timeout)

        :param job_name: name of the job to wait for
        :param namespace: namespace where to create th job
        :param timeout: The maximum waiting time (in seconds)
        :param retry_interval: The delay between successive tests \
                               (in seconds)
        """
        retry_interval = float(retry_interval)
        timeout = int(timeout)
        timeout_start = time.time()

        while time.time() < timeout_start + timeout:
            time.sleep(retry_interval)
            resp = self.batch_api.read_namespaced_job_status(
                                name=job_name,
                                namespace=namespace).status.active
            if resp == None:
                return True
        return False


    def get_job_output(self, 
                       job_name, 
                       container_name=None, 
                       namespace='default'):
        # Retrieve the pod name to connect to
        pods = self.core_api.list_namespaced_pod(namespace=namespace,
                                            # label_selector=job_name,
                                            timeout_seconds=10)
        pod_name=""
        for i in pods.items:
            pod_name = i.metadata.name
            # print (job_name, pod_name)
            if job_name in pod_name:
                break

        print(pod_name, job_name)

        # Connect to Pod
        resp=self.core_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=namespace,
                container=job_name)

        return resp



    def delete_job(self, job_name: str, namespace: str = 'default'):
        """ Delete a job and its pods.
    
        :param job_name: name of the job to delete
        :param namespace: namespace of the job
        """

        # Delete all dependant pods as well
        delete_opt = client.V1DeleteOptions(propagation_policy='Background')

        self.batch_api.delete_namespaced_job(name=job_name,
                                            namespace=namespace,
                                            body=delete_opt)





if __name__ == '__main__':
    import socket
    from da_circo_k8s_example import example_pod_template, example_job_template

    logging.basicConfig(format='%(message)s', level=logging.WARNING)
    logger.setLevel(logging.DEBUG)

    m = K8sManager()

    swift_add = socket.gethostbyname('controller')
    # Example for Pods
    pod_manifest = example_pod_template(swift_ip_add=swift_add)
    m.execute_container_in_pod(
            manifest=pod_manifest,
            container_name='transcode')

    # # Example for Jobs
    # job_manifest = example_job_template(swift_ip_add=swift_add)
    # m.execute_job(job_manifest)

    # # # m.transcode_task_in_pod(swift_ip_add='10.30.4.12')
    # # m.transcode_task_in_job(swift_ip_add='10.30.4.12')



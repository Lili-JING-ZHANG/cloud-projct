#!/usr/bin/env python3

import logging
logger = logging.getLogger(__name__)
from typing import List, Dict

import time
import socket

from da_circo_k8s_manager import K8sManager 


def example_pod_template(swift_ip_add: str, 
                         namespace: str='default') -> Dict:
    """ Returns the template of an example pod to transcode a movie.

    :return: A dictionary with the pob template

    :param swift_ip_add: IP address of the swift controller
    :param namespace: namespace where to create the pod
    """

    name = 'tst-transcode'

    pod_manifest = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': name
        },
        'spec': {
            'containers': [{
                'image': 'gitlab-devops.cloud.rennes.enst-bretagne.fr:4567/pcloud/shared/transcode',
                'name': 'transcode',

                "command": [
                    'python3',
                    "transcode.py", 
                    "-x", "1234", 
                    "-i", "bbb_1.mp4", 
                    "-b", "1111",
                    "-p", "ultrafast", 
                    "-d"
                ],

                'env':[
                    {'name': 'OS_PROJECT_DOMAIN_NAME',
                        'value': 'Default'},
                    {'name': 'OS_USER_DOMAIN_NAME',
                        'value': 'Default'},
                    {'name': 'OS_PROJECT_NAME',
                        'value': 'demo'},
                    {'name': 'OS_USERNAME',
                        'value': 'demo'},
                    {'name': 'OS_PASSWORD',
                        'value': 'usr'},
                    {'name': 'OS_AUTH_URL',
                        'value': 'http://controller:5000/v3'},
                    {'name': 'OS_IDENTITY_API_VERSION',
                        'value': '3'},
                ],

                    'resources':{
                        'limits':{
                            'cpu': '4',
                            'memory': '2Gi',
                        },
                    },
                }],

            'hostAliases':[
                {
                    'hostnames': ['controller'],
                    'ip': swift_ip_add,
                }
            ],

            'restartPolicy': 'Never',
        }
    }
    
    return pod_manifest



#################################################################
### Methods to handle JOBs
#################################################################
def example_job_template(swift_ip_add: str,
                         namespace: str = 'default') -> Dict:
    """ Returns the template of an example job to transcode a movie.

    :return: A dictionary with the job template

    :param swift_ip_add: IP address of the swift controller
    :param namespace: namespace where to create the pod
    """

    job_name = "tst-job-" + str(int(time.time()))

    job_manifest = {
        'apiVersion': 'batch/v1',
        'kind': 'Job',
        'metadata': {
            'name': job_name
        },
        'spec': {
            'backoff_limit': 0,
            'parallelism': 1,
        
        'template': {
            'spec': {
                'restartPolicy': 'Never',

                'containers': [{
                    'image': 'gitlab-devops.cloud.rennes.enst-bretagne.fr:4567/pcloud/shared/transcode',
                    'name': job_name,
    
                    'command': [
                        'python3',
                        'transcode.py',
                        '-x', '1234',
                        '-i', 'bbb_1.mp4',
                        '-b', '1111',
                        '-p', 'ultrafast',
                        '-d'
                    ],

                    'env': [{'name': 'OS_PROJECT_DOMAIN_NAME',
                                'value': 'Default'},
                            {'name': 'OS_USER_DOMAIN_NAME',
                                'value': 'Default'},
                            {'name': 'OS_PROJECT_NAME',
                                'value': 'demo'},
                            {'name': 'OS_USERNAME',
                                'value': 'demo'},
                            {'name': 'OS_PASSWORD',
                                'value': 'usr'},
                            {'name': 'OS_AUTH_URL',
                                'value': 'http://controller:5000/v3'},
                            {'name': 'OS_IDENTITY_API_VERSION',
                                'value': '3'}
                    ],

                    'resources': {
                        'limits': {
                            'cpu': '4',
                            'memory': '2Gi'
                        },
                        'requests': None
                    },


                }],

                'hostAliases': [{
                    'hostnames': ['controller'],
                    'ip': swift_ip_add
                }],
            }
            }
        }
    }
    return job_manifest

### Bellow (in comment) is an alternative syntax returning a
### kubernetes.client.V1Job object  
#        # Configure Job template container
#        container = client.V1Container(
#             name=job_name,
#             image='gitlab-devops.cloud.rennes.enst-bretagne.fr:4567/pcloud/shared/transcode',
#
#             command=[
#                 'python3',
#                 "transcode.py", 
#                 "-x", "1234", 
#                 "-i", "bbb_1.mp4", 
#                 "-b", "1111",
#                 "-p", "ultrafast", 
#                 "-d"
#             ],
#
#             env=[
#                 client.V1EnvVar(name='OS_PROJECT_DOMAIN_NAME',
#                                 value='Default'),
#                 client.V1EnvVar(name='OS_USER_DOMAIN_NAME',
#                                 value='Default'),
#                 client.V1EnvVar(name='OS_PROJECT_NAME',
#                                 value='demo'),
#                 client.V1EnvVar(name='OS_USERNAME',
#                                 value='demo'),
#                 client.V1EnvVar(name='OS_PASSWORD',
#                                 value='usr'),
#                 client.V1EnvVar(name='OS_AUTH_URL',
#                                 value='http://controller:5000/v3'),
#                 client.V1EnvVar(name='OS_IDENTITY_API_VERSION',
#                                 value='3'),
#             ],
#
#             resources=client.V1ResourceRequirements(
#                     limits={
#                         'cpu': '4',
#                         'memory': '2Gi',
#                     },
#                     requests=None
#                 ),
#         )
#
#         # Create and configurate a spec section
#         template = client.V1PodTemplateSpec(
#
#             spec=client.V1PodSpec(
#                 restart_policy="Never",
#                 containers=[container],
#                 host_aliases=[
#                     client.V1HostAlias(
#                         hostnames=['controller'],
#                         ip='10.30.4.12'),
#                 ],
#             )
#         )
#
#         # Create the specification of deployment
#         spec = client.V1JobSpec(
#             template=template,
#             backoff_limit=0,
#             parallelism=1,
#            )
#
#         # Instantiate the job object
#         job = client.V1Job(
#             api_version="batch/v1",
#             kind="Job",
#             metadata=client.V1ObjectMeta(name=job_name),
#             spec=spec)




if __name__ == '__main__':

    logging.basicConfig(format='%(message)s', level=logging.WARNING)
    logger.setLevel(logging.DEBUG)
    logging.getLogger('da_circo_k8s_manager').setLevel(logging.DEBUG)

    swift_add = socket.gethostbyname('controller')

    m = K8sManager()

    # Example for Pods
    pod_manifest = example_pod_template(swift_ip_add=swift_add)
    m.execute_container_in_pod(
            manifest=pod_manifest,
            container_name='transcode')

    # # Example for Jobs
    # job_manifest = example_job_template(swift_ip_add=swift_add)
    # m.execute_job(job_manifest)
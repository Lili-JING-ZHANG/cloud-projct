#!/usr/bin/env python3

import logging
logger = logging.getLogger(__name__)
from typing import List, Dict
import os
import subprocess
import time
import paramiko
from scp import SCPClient, SCPException
import openstack
import keyring







def testPing(hostname: str) -> bool:
    """ Test if hostname answers to ping.
    
    :return: True if ping successful, False otherwise

    :param hostname: the hostname
    """
    FNULL = open(os.devnull, 'w')
    retcode = subprocess.call( ("ping -c 1 -W 1 " + hostname).split(),
                                stdout=FNULL, stderr=subprocess.STDOUT)
    if retcode == 0:
        return True
    else:
        return False

def wait_for_ssh_to_be_ready(host: str, port: int = 22,
                             timeout: int = 180,
                             retry_interval: int =1 ) -> bool:
    """ Try to connect via ssh to the host/port until it works.

    :return: True if SSH eventually successful, False otherwise \
             (e.g. due to timeout)

    :param host: the hostname to test SSH on
    :param port: TCP port of the the SSH service
    :param timeout: maximum waiting time (in seconds)
    :param retry_interval: delay between successive tests (in seconds)
    """
    client = paramiko.client.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    retry_interval = float(retry_interval)
    timeout = int(timeout)
    timeout_start = time.time()
    while time.time() < timeout_start + timeout:
        time.sleep(retry_interval)
        try:
            client.connect(host, int(port), allow_agent=False,
                           look_for_keys=False)
        except paramiko.ssh_exception.SSHException as e:
            return True
        except paramiko.ssh_exception.NoValidConnectionsError as e:
            continue
    return False
    

def wait_for_ping(host: str, 
                  timeout: int = 180, retry_interval: int = 1) -> bool:
    """ Try to ping the host/port until it works.

    :return: True if ping eventually successful, False otherwise \
             (e.g. due to timeout)

    :param host: the hostname to wait for
    :param timeout: maximum waiting time (in seconds)
    :param retry_interval: delay between successive tests (in seconds)
    """
    retry_interval = float(retry_interval)
    timeout = int(timeout)
    timeout_start = time.time()

    while time.time() < timeout_start + timeout:
        time.sleep(retry_interval)
        if testPing(host):
            return True
    return False



class VirtualMachine():
    """ A class to deal with Virtual Machines.

    First, instantiate the VM parameters in the constructor. Then call
    the :func:`create()` method to actually trigger the VM creation on
    the cluster.  

    :param connection: The OpenStack connection object 
    :param name: the name of the VM 
    :param image: the OpenStack object representing the VM image 
    :param flavor: the OpenStack object representing the VM flavor 
    :param keypair: the name of the keypair to use 
    :param key_file: the files for the keys 
    :param network: the OpenStack object representing the network of 
        the VM 
    :param security_groups: list of security groups for the VM \
        under the form of a list of dictionaries of \
        {'name': 'Security group name'} 
    :param floatingIpNetwork: The IP address in case the VM has a floating one 
    """
    def __init__(self,
                 connection: openstack.connection.Connection, 
                 name: str,
                 image: openstack.image.v2.image.Image, 
                 flavor: openstack.compute.v2.flavor.Flavor,
                 network: openstack.network.v2.network.Network,
                 keypair: str = None,
                 key_file: str =None,
                 security_groups: List[Dict[str, str]] = [{'name': 'default'}], 
                 floatingIpNetwork: str = None                 
                 ):
        """
        :param connection: The OpenStack connection object
        :param name: the name of the VM
        :param image: the OpenStack object representing the VM image 
        :param flavor: the OpenStack object representing the VM flavor
        :param keypair: the name of the keypair to use
        :param key_file: the files for the keys
        :param network: the OpenStack object representing the network of the VM
        :param security_groups: list of security groups for the VM \
            under the form of a list of dictionaries of \
            {'name': 'Security group name'} 
        :param floatingIPNetwork: The IP address in case you have a floating one 
        """

        self.conn = connection
        self.name = name
        self.image = image
        self.flavor = flavor
        self.keypair = keypair
        self.key_file = key_file
        if keypair and not key_file:
             self.key_file = keypair
        self.network = network
        self.security_groups = security_groups
        self.floatingIpNetwork = floatingIpNetwork
        self.server = None # the instance
        self.fip = None # the Floating IP
        self.ip = None # the IP address of the VM (internal or Floating if exists)


    def create(self):
        """ Create the VM and wait the IP address to be available.
        """
        if not self.conn:
            return

        if self.server:
            logger.warning("Warning: VM Already initialized! => Aborting")
            return

        logger.info("%s VM configuration..." % self.name)

        self.create_security_groups_if_not_existing()
        self.create_keypair_if_not_existing()

        img = self.image
        flv = self.flavor
        net = self.network

        # Create and start the instance
        self.server = self.conn.compute.create_server(
            name=self.name,
            image_id=img.id,
            flavor_id=flv.id,
            networks=[{"uuid": net.id}],
            security_groups=self.security_groups,
            key_name=self.keypair,
            )

        logger.debug("%s ... wait for server..." % self.name)

        # Wait for the instance to be created to get its IP 
        self.conn.compute.wait_for_server(self.server)
        self.ip = self.server.addresses[self.network.name][0]['addr']
        logger.debug("%s IP address found %s" %(self.name, self.ip))

        # if requested: attach a floating IP     
        if self.floatingIpNetwork:
            self.conn.compute.wait_for_server(self.server)
            extnet = self.conn.network.find_network(self.floatingIpNetwork)
            self.fip = self.conn.network.create_ip(floating_network_id=extnet.id)
            self.conn.compute.add_floating_ip_to_server(
                self.server.id,
                self.fip.floating_ip_address)
            self.ip = self.fip.floating_ip_address



    def create_security_groups_if_not_existing(self):
        group_name = 'enable_ICMP'
        if self.conn.network.find_security_group(group_name)==None:
            group = self.conn.network.create_security_group(name=group_name)

            icmp_rule = self.conn.network.create_security_group_rule(
                security_group_id=group.id,
                direction='ingress',
                remote_ip_prefix='0.0.0.0/0',
                protocol='icmp',)


        group_name = 'enable_SSH'
        if self.conn.network.find_security_group(group_name)==None:
            group = self.conn.network.create_security_group(name=group_name)

            icmp_rule = self.conn.network.create_security_group_rule(
                security_group_id=group.id,
                direction='ingress',
                remote_ip_prefix='0.0.0.0/0',
                protocol='tcp',
                port_range_max='22',
                port_range_min='22',)

    def create_keypair_if_not_existing(self):
        key_name = self.keypair
        key_file = self.key_file
        pub_key_file = self.key_file + '.pub'
        c = self.conn.compute

        # If keypair exists on the controller AND we have the private key file locally => OK
        if c.find_keypair(key_name) \
        and os.path.isfile(key_file): 
            return

        # If we have the private and public key files locally
        # BUT the keypair doesn't exist on the controller => upload the public key
        if (os.path.isfile(key_file) and os.path.isfile(pub_key_file)) \
        and (c.find_keypair(key_name)==None):
            # Here we should
            #  - Upload the public key to the OpenStack Controller
            #  - and return
            # BUT there is no way to upload and existing key file in openstack SDK
            # So we just do noting and let the next section create a new key      
            pass

        # If keypair doesn't exist on the controller OR we don't have the private key file:
        if (c.find_keypair(key_name)==None) or \
        not os.path.isfile(key_file):
            # delete previous on the server (doesn't trigger any error if not existing)
            c.delete_keypair(key_name, ignore_missing=True)
            key = c.create_keypair(name=key_name) # Create e new one
            with open(key_file, 'w') as f:
                f.write(key.private_key)
            with open(pub_key_file, 'w') as f:
                f.write(key.public_key)
            return

    def ssh(self, username: str, key_filename: str, command: str,
            blocking: bool = False):
        """ Open a SSH connection on the VM and run the requested command.

        :return stdout, stderr: the piped output of the command
        :rtype: subprocess pipes

        :param username: the login for the SSH connection
        :param key_filename: path to the file of the SSH key
        :param command: the command to be executed
        :param blocking: if True, the method doesn't return before the \
            end of the execution of the command on the remote host.
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.ip, username=username, key_filename=key_filename)
        stdin, stdout, stderr = ssh.exec_command(command)
        if blocking:
            out = stdout.readlines() 
            err = stderr.readlines()
            return out, err
        return stdout, stderr

    def scp(self, username: str, key_filename: str, 
            src_file: str, dst_dir: str):
        """ Copy a file to a distant server using SCP.

        :return stdout, stderr: the piped output of the command
        :rtype: subprocess pipes

        :param username: the login for the SSH connection
        :param key_filename: path to the file of the SSH key
        :param src_file: path of the file to be copied on the local machine
        :param dst_dir: target folder where on the destination machine
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        ssh.connect(self.ip, 
                    username=username, 
                    key_filename=key_filename)
        
        scp = SCPClient(ssh.get_transport())
        scp.put(src_file,
                recursive=True,
                remote_path=dst_dir)
        scp.close()


    def suspend(self):
        self.conn.compute.suspend_server(self.server)

    def resume(self):
        self.conn.compute.resume_server(self.server)

    def stop(self):
        self.conn.compute.stop_server(self.server)

    def start(self):
        self.conn.compute.start_server(self.server)

    def reboot(self, reboot_type='HARD'):
        self.conn.compute.reboot_server(self.server, reboot_type=reboot_type)

    def delete(self):
        """ Delete the VM
        """
        logger.info("%s VM is deleted" %self.name)
        if self.server:
            self.conn.compute.delete_server(self.server)
            self.server = None
        if self.fip:
            self.conn.network.delete_ip(self.fip)
            self.fip = None
        self.ip = None

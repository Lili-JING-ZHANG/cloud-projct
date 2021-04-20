#! /usr/bin/python3
# coding: utf-8

import logging
import argparse
import csv
import asyncio
import time
import requests
import scenario_generator
import time
import sys
import signal
import openstack
import numpy as  np

### Command line arguments parsing
parser = argparse.ArgumentParser(description='The DaCirco Scenario Player')
parser.add_argument('-f', 
                    dest='scenario_file',
                    help='an input scenario file',
                    required=False,
                    type=str,)
parser.add_argument('-n', 
                    dest='nb_instances',
                    help='if no scenario file, create a new one with n calls',
                    required=False,
                    type=int,)
parser.add_argument('-d', '--debug',
                    dest='debugFlag',
                    help='Raise the log level to debug',
                    action="store_true",
                    default=False)
parser.add_argument('-t', '--target',
                    dest='target',
                    help='(default:  \'%(default)s\') host address to send requests to',
                    required=False,
                    default='127.0.0.1')
parser.add_argument('-p', '--port',
                    dest='port',
                    help='(default:  \'%(default)s\') port on host to send requests to',
                    required=False,
                    type=int,
                    default='9000')

args = parser.parse_args()


### Log level configuration
if args.debugFlag == True:
    logLevel = logging.DEBUG
else:
    logLevel = logging.WARNING
logging.basicConfig(level=logLevel)

###
def signal_handler(sig, frame):
    """
    capture signal end
    """
    return None

### collect and process stats
def process_stats(scn):
    """
    compute the stats
    """

    conn = openstack.connect()
    counter = 0
    counter_error = 0
    counter_completed = 0
    list_duration = list()        
    for movie in scn.duration.keys():
        counter += 1
        try:
            metad = conn.object_store.get_object_metadata(
                movie,
                container='CompressedVideos')
            counter_completed += 1
            duration = float(metad['timestamp']) - scn.duration[movie]
            list_duration.append(duration)
        except openstack.exceptions.ResourceNotFound as e: 
            counter_error += 1
    
    # for obj in conn.object_store.objects("CompressedVideos"):
    #     if obj.name in scn.duration.keys():
    #         print("found %s" %obj.name)
    #         metad = conn.object_store.get_object_metadata(obj)
    #         scn.duration[obj.name].append(float(metad['timestamp']))

    
    # for movie  in scn.duration.keys():
    #     counter = counter + 1
    #     if len(scn.duration[movie]) == 1:
    #         print("movie %s has not been transcoded" %movie)
    #         counter_error += 1
    #     else:
    #         counter_completed += 1
            

    if not counter_completed:
        percent = 0
        aver = 0
    else:
        percent = np.percentile(np.array(list_duration), np.array(95))
        aver = np.average(np.array(list_duration))

    print("completed ratio: %s" %float(counter_completed/counter))
    print("duration 95th percentile: %s" %percent)
    print("average: %s" %aver)
        
### class Scenario
class Scenario:
    def __init__(self, host, port):
        self.scenario = []
        self.host = host
        self.port = port

        self.duration = dict()


    def load_from_csv(self, filepath):
        with open(filepath, 'r') as f:
            time = 0
            reader = csv.reader(f, delimiter=',', quotechar='"')
            for row in reader:
                if len(row)!=4 or row[0][0]=='#':
                    continue
                t = 0
                try:
                    t = float(row[0])
                except:
                    msg_tab = ["Skip on config file input due to invalid time"]
                    msg_tab.append(str(row[0]))
                    msg = ": ".join(msg_tab)
                    logging.debug(msg)
                    continue
                
                time += t
                movie, bitrate, preset = row[1:]

                self.scenario.append({
                    'ts': time, 
                    'movie': movie,
                    'bitrate': int(bitrate),
                    'preset': preset})

    def load_from_generator(self, nb_instances):
        new_scenario = scenario_generator.Scenario(nb_instances, "last_scenario.csv")

        time = 0
        for row in new_scenario.entries:
            t, movie, bitrate, preset = row
            time = time + t
            self.scenario.append({
                    'ts': time, 
                    'movie': movie,
                    'bitrate': int(bitrate),
                    'preset': preset})
        new_scenario.saveInFile()

    #@staticmethod
    async def request_job(self, delay, job):
        await asyncio.sleep(delay)
        print("Request Job: " + str(job))

        url  = 'http://' + self.host + ':' + str(self.port) + '/jobs'
        r=requests.post(url, json=job)
        job_id = r.json()
        self.duration[job_id] = time.time()

    async def play(self):
        #tasksList = []
        logging.debug("doing play")
        tasks = []
        for order in self.scenario:
            t = order['ts']
            logging.debug("running task planned at %f" %t)

            job = { 'id_video': order['movie'], 
                    'bitrate' : order['bitrate'],
                    'speed'   : order['preset'],
                  }

            # tasks.append(asyncio.create_task(
            #     self.request_job(t, job)))
            loop = asyncio.get_event_loop()
            tasks.append(loop.create_task(
                self.request_job(t, job)))

        for task in tasks:
            await task


    def __str__(self):
        s = ''
        for i in self.scenario:
            s += "At t= %d, transcode at %s k with %s speed\n" %(
                i['ts'], i['bitrate'], i['preset'])
        if len(s) != 0: # remove last '\n'
            s = s[:-1]
        return s



### Main function
def main():
    intro_msg = ["Start the App\n"]
    
    scn = Scenario(host=args.target, port=args.port)

    if (args.scenario_file == None and args.nb_instances==None):
        intro_msg.append("None")
    elif (args.scenario_file==None):
        intro_msg.append("create scenario")
        scn.load_from_generator(args.nb_instances)
    else:
        intro_msg.append("input file:")
        intro_msg.append(args.scenario_file)
        scn.load_from_csv(args.scenario_file)

    logging.debug(" ".join(intro_msg))
    logging.debug(scn)

    # asyncio.run(scn.play())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scn.play())
    loop.close()

    signal.signal(signal.SIGINT, signal_handler)
    # signal.pause()
    # process_stats(scn)
    sys.exit(0)

### Start Application if directly called from command line
if __name__ == "__main__":
    main()


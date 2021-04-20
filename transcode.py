#! /usr/bin/python3
# coding: utf-8

import argparse
from ffmpy import FFmpeg as ffmpeg
import os
import transcode_constants
import logging
import subprocess
import time
import openstack

class Movie:
    """ class for a movie and the operations on it.

    :param id_job: the id of the job (and also the name of the output)
    :param movie_title: the filename of the movie to transcode
    :param target_bitrate: target bit-rate in kbps for the output video
    :param preset: the speed of the encoding ("ultrafast" or "fast") 
    """

    def __init__(self, id_job: str, movie_title: str, 
                 target_bitrate: int, preset: str):
        """
        :param id_job: the id of the job (and also the name of the output)
        :param movie_title: the filename of the movie to transcode
        :param target_bitrate: target bit-rate in kbps for the output video
        :param preset: the speed of the encoding ("ultrafast" or "fast") 
        """

        self.id_job = id_job

        self.conn = openstack.connect(debug=False)
        if preset not in ["ultrafast", "fast"]:
            self.preset = "fast"
        else:
            self.preset = preset

        if (target_bitrate > transcode_constants.MAX_BITRATE or 
            target_bitrate < transcode_constants.MIN_BITRATE):
            self.bitrate = "".join([str(transcode_constants.MIN_BITRATE),'k'])
        else:
            self.bitrate = "".join([str(target_bitrate), 'k'])

        self.movie = self.get_movie(movie_title)

        # create the name of the output video
        directory = os.getcwd()
        folder_compressed = "CompressedVideos"
        if folder_compressed not in os.listdir():
            os.mkdir(folder_compressed)
        folder_output = os.path.join(directory, folder_compressed)
        output_tab = []
        output_tab.append(self.movie.split(".")[0])
        output_tab.append(self.preset)
        output_tab.append(self.bitrate)
        filename_output = "_".join(output_tab)
        filename_output = ".".join([filename_output,"mp4"])
        self.output = os.path.join(folder_output, filename_output)

        duration = transcode_constants.LENGTH

        # create the list of encoding parameters for the output video
        param_tab = []
        param_tab.append('-c:v libx265')
        param_tab.append(" ".join(["-preset", self.preset]))
        param_tab.append(" ".join(["-b:v", self.bitrate]))
        param_tab.append(" ".join(["-t", duration]))
        param_tab.append("-x265-params log-level=2")
        param_tab.append("-max_muxing_queue_size 1024") # see https://stackoverflow.com/questions/49686244/ffmpeg-too-many-packets-buffered-for-output-stream-01
        transcoding_param = " ".join(param_tab)

        # verbosity parameters
        global_tab = []
        global_tab.append("-hide_banner")
        global_tab.append("-v error")
        global_params = " ".join(global_tab)

        # create the ffmpeg object
        self.ff = ffmpeg(
            inputs = {self.movie: None},
            outputs = {self.output: transcoding_param},
            global_options= {global_params}
            )

    def get_movie(self, movie: str) -> str:
        """ Get the movie (if necessary).

        :retrun: the filemane of the movie

        :param movie: the filename of the movie
        """

        if movie not in os.listdir():
            logging.debug("Get from Swift")
            movie_data = self.conn.object_store.download_object(movie,
                                                           container="videos")
            movie_file = open(movie, "wb")
            movie_file.write(movie_data)
            logging.debug("Movie fetched")

        return movie


    def transcode(self):
        """ Run the transcoding operation.
        """

        logging.debug(f"start encoding %s at {time.strftime('%X')}" %(self.bitrate))
        log, error = self.ff.run(stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        outs = error.decode(encoding="utf-8")
        try:
            outputs = outs.split(" in ")
            time_transcode = outputs[1].split("s (")[0]
            logging.debug("encoding time was %s s" %time_transcode)
        except:
            logging.debug("something wrong in the output")
            logging.debug(outs)


    def clean(self):
        """ Remove the file of the movie once saved in Swift.
        """

        os.remove(self.output)
        logging.debug("%s is deleted from local hard disk" % self.output)

    def save(self):
        """ Save the transcoded movie in Swift.
        """

        logging.debug("%s is sent to Swift" % self.output)
        transcoded_file = open(self.output, "rb")
        transcoded_data = transcoded_file.read()
        video_name = self.id_job #os.path.basename(self.output)
        self.conn.object_store.upload_object(container="CompressedVideos",
                                             name=video_name,
                                             data=transcoded_data)
        
# =============================================

def check_bitrate(value: int) -> int:
    """ Check of the bit-rate in input is valid

    :return: the argument if it is valid

    :param value: the input argument
    :type  value: int
    """
    ivalue = int(value)

    if ivalue < transcode_constants.MIN_BITRATE:
        raise argparse.ArgumentTypeError("%s is too small" % value)
    elif ivalue > transcode_constants.MAX_BITRATE:
        raise argparse.ArgumentTypeError("%s is too high" % value)
    return ivalue

if __name__ == "__main__":

    ### Command line arguments parsing
    explain_data = []
    explain_data.append("launch a transcoding operation")
    explain_data.append("1- get the movie (if not already in folder)")
    explain_data.append("2- transcode with input parameters")
    explain_data.append("3- put transcoded video in swift")
    explain = " ".join(explain_data)

    parser = argparse.ArgumentParser(description=explain)

    parser.add_argument('-x', dest='id',
         type = str,
         help = "the job ID")
    parser.add_argument('-i', dest='movie',
         type = str,
         default = "bbb_0.mp4",
         help = "the name of the movie that should be transcoded")
    parser.add_argument('-b', dest='bitrate',
         type = check_bitrate,
         help = "the target bit-rate in kbps (int in [%d,%d])" % 
            (transcode_constants.MIN_BITRATE,
             transcode_constants.MAX_BITRATE))
    parser.add_argument('-p', dest='preset',
         type = str,
         default = "fast",
         choices = ["ultrafast", "fast"],
         help = "the speed of the encoding operation")
    parser.add_argument('-d', '--debug',
         dest='debugFlag',
         help='Raise the log level to debug',
         action="store_true",
         default=False)
   
    arg = parser.parse_args()

    ### Log level configuration
    if arg.debugFlag == True:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.WARNING
    # logging.basicConfig(level=logLevel)
    logging.basicConfig(level=logLevel, format='%(message)s')

    # disable keystoneauth, stevedore & urllib3 debug messages
    logging.getLogger('keystoneauth').setLevel(logging.INFO)
    logging.getLogger('stevedore').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)


    # create the movie object
    movie_obj = Movie(
        id_job = arg.id,
        movie_title = arg.movie,
        target_bitrate = arg.bitrate,
        preset = arg.preset)

    # actions
    movie_obj.transcode()

    movie_obj.save()

    movie_obj.clean()

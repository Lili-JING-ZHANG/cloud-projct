#!/usr/bin/env python3

from fastapi import Path, Body
from pydantic import BaseModel, Schema
from typing import List

class Job(BaseModel):
    """ Defines the type parameter of POST /jobs request.
    """    
    id_video: str = Schema (None,
                            title = "Resource Path of the Video",
                            max_length = 256)
    """ resource Path of the Video
    """

    bitrate: int = Schema (None,
                           title = "Bitrate of the Requested Video",
                           gt = 0,
                           description = "The bitrate must be in [500, 8000]")
    """ bitrate of the Requested Video. Must be in range [500, 8000]
    """
    speed: str = Schema (None,
                         title = "Encoding Speed",
                         description = "It can be ultrafast or fast") 
    """ encoding speed. Can be "ultrafast" or "fast"
    """    
  
class State:
    """  Possible states for a Job.
    """    
    WAITING = "Waiting"
    STARTED = "Started"
    COMPLETED = "Completed"
    ERROR = "Error"  

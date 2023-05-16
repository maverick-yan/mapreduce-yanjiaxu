import os 
import json
import settings
from multiprocessing import Process

class MapReduce(object):
    """
    the mapreduce model
    """

    def __init__(self, input_dir = settings.default_input_dir, 
                 output_dir = settings.default_input_dir,
                 n_mappers = settings.default_n_mappers, n_reducers = 
                 settings.default_n_reducers,
                 clean = True):
        
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.clean = clean

    def mapper(self, key, value):
        """
        to be implemented
        
        """
        pass

    def reducer(self, key, value_list):
        """
        to be implemented
        
        """
        pass
    

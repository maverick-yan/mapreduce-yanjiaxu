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
    
    def run_mapper(self, index):
        """_summary_

        Args:
            index (int): the index of the thread    
        """
        # read a key 
        # read a value 
        # get the result of the mapper
        # store the result to reducer
        pass
    
    def run_reducer(self, index):
        """reducer

        Args:
            index (int): index of the reducer thread
        """
        # load results from mapper
        # for each key, do reduce
        # store results
        
        pass
    
    def run(self, join=False):
        """
        where map and reduce operations take place
        
        
        """
        
        # initialize mappers list
        map_workers = []
        
        # initialize reducers list
        reduce_workers = []
        
        #map
        for thread_id in range(self.n_mappers):
            p = Process(target=self.run_mapper, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in map_workers]
        
        
        
        #reduce
        
        for thread_id in range(self.n_reducers):
            p = Process(target=self.run_reducer, args=(thread_id,))
            p.start()
            reduce_workers.append(p)
        [t.join() for t in reduce_workers]
        
        if join:
            self.join_outputs()
        

class FileHandler(object):
    """FileHandler Class
    """
    def __init__(self, input_file_path, output_dir):
    
        
        self.input_file_path = input_file_path
        self.output_dir = output_dir
        
    def split_file(self, number_of_splits):
            
        """split the files according to number of splits

        """
        pass
    
    def join_files(self, number_of_files, clean = None, sort = True, decreasing = True):
        """join all the files into a single output file.

        Args:
            number_of_files (_type_): 
            clean (_type_, optional): _description_. Defaults to None.
            sort (bool, optional): _description_. Defaults to True.
            decreasing (bool, optional): _description_. Defaults to True.
            
        :return output_join_list: a list of the outputs
        """

        pass
    
    def begin_file_split(self, split_index, index):
        """initialize a split file by opening and adding an index.
        :param split_index: the split index we are currently on, to be used for naming the file.
        :param index: the index given to the file.
        """
        file_split = open(settings.get_input_split_file(split_index-1), "w+")
        file_split.write(str(index) + "\n")
        return file_split
    
    def is_on_split_position(self, character, index, split_size, current_split):
        
        """Check if it is the right time to split.
        i.e: character is a space and the limit has been reached.
        :param character: the character we are currently on.
        :param index: the index we are currently on.
        :param split_size: the size of each single split.
        :param current_split: the split we are currently on.
        """
        return index>split_size*current_split+1 and character.isspace()
    
    def split_file(self, number_of_splits):
        """split a file into multiple files.
        note: this has not been optimized to avoid overhead.
        :param number_of_splits: the number of chunks to
        split the file into.
        """
        file_size = os.path.getsize(self.input_file_path)
        unit_size = file_size / number_of_splits + 1
        original_file = open(self.input_file_path, "r")
        file_content = original_file.read()
        original_file.close()
        (index, current_split_index) = (1, 1)
        current_split_unit = self.begin_file_split(current_split_index, index)
        for character in file_content:
            current_split_unit.write(character)
            if self.is_on_split_position(character, index, unit_size, current_split_index):
                current_split_unit.close()
                current_split_index += 1
                current_split_unit = self.begin_file_split(current_split_index,index)
            index += 1
        current_split_unit.close()

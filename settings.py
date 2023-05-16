# default dir for input files
default_input_dir = "input_files"

# default temporary map files
default_map_dir = "temp_map_files"

# default output files
default_output_dir = "output_files"

# default mapper & reducer threads

default_n_mappers = 10
default_n_reducers = 10

# return name of the input file to be split

def get_input_file(input_dir = None, extension = ".csv"):
    if not (input_dir is None):
        return input_dir+"/file" + extension
    return default_input_dir + "/file" + extension

# return the name of current split file corresponding to given index
def get_input_split_file(index, input_dir = None, extension = '.csv'):
    if not(input_dir is None):
        return input_dir+"/file_"+ str(index) + extension
    return default_input_dir + "/file_" + str(index) + extension


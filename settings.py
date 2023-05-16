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

# return  name of the temporary map file by given index
def get_temp_map_file(index, reducer, output_dir = None, extension = ".csv"):
    if not(output_dir is None):
        return output_dir + "/map_file_" + str(index)+"-" + str(reducer) + extension
    return default_output_dir + "/map_file_" + str(index) + "-" + str(reducer) + extension


# return the name of the output file given its corresponding index
def get_output_file(index, output_dir = None, extension = ".out"):
    if not(output_dir is None):
        return output_dir+"/reduce_file_"+ str(index) + extension
    return default_output_dir + "/reduce_file_" + str(index) + extension


# return the name of the output file
def get_output_join_file(output_dir = None, extension = ".out"):
    if not(output_dir is None):
        return output_dir +"/output" + extension
    return default_output_dir + "/output" + extension
    
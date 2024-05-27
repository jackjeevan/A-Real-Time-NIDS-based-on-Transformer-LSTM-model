import os

class FileExtractor():
    '''
    FileExtractor object extract the files present in the given Folder
    :param path: The path of the folder
    :type path: str
    '''

    def __init__(self, path):
        self.__path = path

    def getFiles(self):
        '''
        getFiles return the generator object for all the files in the directory
        :returns: file object generator
        '''

        for each in os.scandir(self.__path):
            if each.is_file():
                yield each
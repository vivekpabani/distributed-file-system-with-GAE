import os
import random
import string

#Program to create dataset of given size and number of files.

# File details as size-number of files
create_file_details = {1:100,10:100,100:100,1024:100,10240:10,102400:1}

# characters to choose for filename and file data
characters = list(string.ascii_uppercase) + list(string.ascii_lowercase) + list(string.digits)

writeDir = 'dataset'

#to create a directory
def create_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

#to create a file with given name and data
def create_file(fname, fdata):
    global writeDir
    
    writeFile = writeDir + '/' + fname
    
    file = open(writeFile,"w+")
    file.write(fdata)
    file.close()

#to create all the files given as a list of size-number of files
def create_all_files(file_d):
    global writeDir
    
    create_dir(writeDir)
    
    for key in file_d:
        value = int(file_d[key])
        
        for count in xrange(value):
            create_file(gen_filename(),gen_filedata(key))

#to generate a file name of 10 characters long
def gen_filename():
    return ''.join(random.choice(characters) for i in xrange(10))

#to generate file data as per given size, with 100 characters per line
def gen_filedata(size):
    datasize = size*1024
    loopsize = datasize/100
    remdata = datasize%100
    data = ''
    data = ''.join(''.join(random.choice(characters) for i in xrange(99)) +"\n" for j in xrange(loopsize))
    data = data + ''.join(random.choice(characters) for i in xrange(remdata))
    return data

#call to create all the files in the declared directory
create_all_files(create_file_details)
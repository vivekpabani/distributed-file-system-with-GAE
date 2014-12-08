import os
import urllib
import webapp2
import cgi
import cgitb; cgitb.enable()
import random
import threading
import Queue

from google.appengine.api import memcache

from google.appengine.ext import ndb
import cloudstorage as gcs
import urllib2

MULTITHREAD = 0     #To activate multithread.
MEM_ACTIVE = 1      #To activate and deactivate memcache.

bucket = '/vap-demo2.appspot.com'       #Defalut bucket to save the files.


#------- Mandatory Functions Start--------#

#function to insert the file to gcs bucket

def insert(key, value):
    global bucket
    global MEM_ACTIVE

    fname = bucket + '/' + key
    write_retry_params = gcs.RetryParams(backoff_factor=1.1)

    gcs_file = gcs.open(fname,
                        'w',
                        options={'x-goog-acl': 'public-read',
                        'x-goog-meta-foo': 'foo',
                        'x-goog-meta-bar': 'bar'},
                        retry_params=write_retry_params)
    gcs_file.write(value)
    gcs_file.close()
    stat = gcs.stat(fname)
    # inserts to memcache if size less than 100 KB
    if MEM_ACTIVE:
        if stat.st_size <= 102400:
            memcache.set(key,value)

    return stat.st_size



def listing():
    global bucket

    stored_files = []
    stats = gcs.listbucket(bucket)

    for stat in stats:
        stored_files.append(stat.filename)

    return stored_files

def check(key):
    global bucket

    found = 0

    value = memcache.get(key)
    if value is not None:
        found = 1
    else:
        fname = bucket + '/' + key
        stored_files = listing()
    
        for file in stored_files:
            if file == fname :
                found = 1
                break
    return found



def find(self, key):
    global bucket

    if check(key):
        value = memcache.get(key)
        if value is not None:
            self.response.headers['Content-Type'] = 'binary/octet-stream'
            self.response.headers['Content-Disposition'] = 'attachment;filename=%s' %str(key + "MEM")
            self.response.write(value)
        else:
            fname = bucket + '/' + key
            url = "https://storage.cloud.google.com" + fname
            url2 = str(url)
            self.redirect(url2)
        return 1
    else:
        return 0

def remove(key):
    global bucket

    fname = bucket + '/' + key
    
    if check(key):
        value = memcache.get(key)

        if value is not None:
            memcache.delete(key)

        gcs.delete(fname)
        
        return 1
    else:
        return 0



#------- Mandatory Functions End--------#


#------- Extra Functions Start----------#

def checkStorage(key):
    global bucket
    
    fname = bucket + '/' + key
    stored_files = listing()
    found = 0
    
    for file in stored_files:
        if file == fname :
            found = 1
            break

    return found


def checkCache(key):
    
    value = memcache.get(key)
    if value is not None:
        return 1
    else:
        return 0


def removeAllCache():
    
    return memcache.flush_all()


def gcs_delete(key):
    gcs.delete(key)


def removeAll():

    stored_files = listing()

    if not MULTITHREAD:
        for fname in stored_files:
            gcs.delete(fname)
    else:
        count = len(stored_files)
        numThreads = 4
        currentFile = 0
            
        while(count>0):
            if count>numThreads:
                count = count - numThreads
            else:
                numThreads = count
                count = 0
            
            threads = []
            for i in xrange(numThreads):
                thread1 = threading.Thread(target=gcs_delete, args = stored_files[currentFile])
                currentFile = currentFile+1
                threads.append(thread1)
                
            # Wait for all threads to complete
            for t in threads:
                t.start()
                
            for t in threads:
                t.join()

    memdone = memcache.flush_all()


def cacheSizeMB():
    
    cacheSize = int((memcache.get_stats()['bytes']))/1048576.0
    return cacheSize


def cacheSizeElem():
    
    return memcache.get_stats()['items']


def storageSizeMB():
    global bucket
    
    stored_files = listing()
    storageSize = 0
    for fname in stored_files:
        storageSize = storageSize + gcs.stat(fname).st_size
    
    return int(storageSize)/1048576.0


def storageSizeElem():
    return len(listing())


def findInFile(key, value):
    global bucket
    
    if check(key):
        fname = bucket + '/' + key
        gcs_file = gcs.open(fname)
        
        if value in gcs_file.read():
            return 1
        else:
            return 0
    else:
        return -1


def listingRegEx(value):

    stored_files = listing()
    matched_files = []
    
    for filename in stored_files:
        fname = filename.split('/')[2]
        if value in fname:
            matched_files.append(fname)

    return matched_files


#------- Extra Functions End----------#


#------- Workload Generator Functions Start----------#


def get_data(key, out_q):
    global bucket
    
    value = ''
    value = memcache.get(key)
    if value:
        pass
    else:
        fname = bucket + '/' + key
        gcs_file = gcs.open(fname)
        value = gcs_file.read()
        gcs_file.close()
    out_q.put(value)


def workloadGen(self):
    global bucket

    stored_files = listing()
    length = len(stored_files)

    count = 2 * length
    numThreads = 4
    currentFile = 0
    data = ''

    outputfile = 'outfile'
    self.response.headers['Content-Type'] = 'binary/octet-stream'
    self.response.headers['Content-Disposition'] = 'attachment;filename=%s' %str(outputfile)

    while(count>0):
        out_q = Queue.Queue()
        
        if count>numThreads:
            count = count - numThreads
        else:
            numThreads = count
            count = 0
        
        threads = []

        for i in xrange(numThreads):
            key = random.choice(stored_files)
            thread1 = threading.Thread(target=get_data, args = (key, out_q))
            threads.append(thread1)
        
        # Wait for all threads to complete
        for t in threads:
            t.start()

        for k in xrange(numThreads):
            data = data + out_q.get()

        for t in threads:
            t.join()

    self.response.write(data)




#------- Workload Generator Functions End----------#

# Main Handler class which loads forst when application is invoked. It provides with intreface to invoke all the functions described above.

class MainHandler(webapp2.RequestHandler):
    def get(self):

        self.response.out.write('<html><body>')

        self.response.out.write('<form action="/insert" method="post" enctype="multipart/form-data">')
        self.response.out.write('Upload multiple Files: <input type="file" name="insert_files" value = "Browse" multiple><br> <input type="submit" name="submit" value="Upload"> </form>')

        self.response.out.write('<form action="http://storage.googleapis.com/vap-demo2.appspot.com" method="post" enctype="multipart/form-data">')
        self.response.out.write('<input type="hidden" name="key" value="${filename}" />')
        self.response.out.write('<input type="hidden" name="success_action_status" value="201" />')

        self.response.out.write('Upload a large file:<input type="file" name="file">')
        self.response.out.write('<br><input type="submit" value="Upload"> </form>')


        self.response.out.write('<form action="/list" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="submit" name="submit" value="List All Files"> </form>')

        self.response.out.write('<form action="/check" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="text" name="check_file"> <input type="submit" name="submit" value="Check File"> </form>')

        self.response.out.write('<form action="/find" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="text" name="find_file"> <input type="submit" name="submit" value="Find File"> </form>')

        self.response.out.write('<form action="/remove" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="text" name="remove_file"> <input type="submit" name="submit" value="Remove File"> </form>')



        self.response.out.write('<form action="/checkstorage" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="text" name="check_storage_file"> <input type="submit" name="submit" value="Check Storage File"> </form>')

        self.response.out.write('<form action="/checkcache" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="text" name="check_cache_file"> <input type="submit" name="submit" value="Check Cache File"> </form>')

        self.response.out.write('<form action="/removeallcache" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="submit" name="submit" value="Remove All Cache"> </form>')

        self.response.out.write('<form action="/removeall" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="submit" name="submit" value="Remove All"> </form>')

        self.response.out.write('<form action="/cachesizemb" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="submit" name="submit" value="Cache Size MB"> </form>')

        self.response.out.write('<form action="/cachesizeelem" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="submit" name="submit" value="Cache Size Elements"> </form>')

        self.response.out.write('<form action="/storagesizemb" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="submit" name="submit" value="Storage Size MB"> </form>')
        
        self.response.out.write('<form action="/storagesizeelem" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="submit" name="submit" value="Storage Size Elements"> </form>')
        
        self.response.out.write('<form action="/findinfile" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> File Name : <input type="text" name="find_in_file"> &nbsp String to Find : <input type="text" name="find_value"> <input type="submit" name="submit" value="Find In File"> </form>')

        self.response.out.write('<form action="/listingregex" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br>String to Match : <input type="text" name="match_value"> <input type="submit" name="submit" value="List Matching Files"> </form>')

        self.response.out.write('<form action="/workload" method="POST" enctype="multipart/form-data">')
        self.response.out.write('<br> <input type="hidden" name="submit" value="Work Load"> </form>')


        self.response.out.write('</body></html>')


#------- Mandatory Functions Handler Start--------#

class InsertHandler(webapp2.RequestHandler):
    def post(self):
        
        filenames = self.request.POST.getall('insert_files')
        filenames_list = [filenames]
        count = 0
        keys = []
        values = []
        inserted_files = []
        if len(str(filenames_list[0])) > 5:             #checks if any file uploaded or not.
            for file_data in filenames :
                keys.append(file_data.filename)
                values.append(file_data.file.read())
                count = count+1

            numThreads = 4
            currentFile = 0

            while(count>0):
                if count>numThreads:
                    count = count - numThreads
                else:
                    numThreads = count
                    count = 0

                threads = []
                for i in xrange(numThreads):

                    thread1 = threading.Thread(target=insert, args = (keys[currentFile],values[currentFile]))       #thread invoked with filename and filedata to insert.
                    inserted_files.append(keys[currentFile])
                    currentFile = currentFile+1
                    threads.append(thread1)
    
                # Wait for all threads to complete
                for t in threads:
                    t.start()
    
                for t in threads:
                    t.join()

            for file in inserted_files:
                self.response.out.write('File : %s uploaded successfully.<br>' % file)

        else:
            self.response.out.write('No files are selected for upload.')



class ListHandler(webapp2.RequestHandler):
    def post(self):
        
        stored_files = listing()

        for file in stored_files:
            self.response.out.write('%s<br>' % file.split('/')[2])


class CheckHandler(webapp2.RequestHandler):
    def post(self):

        filename = self.request.get('check_file')

        if check(filename):
            self.response.out.write('%s file exists.<br>' % filename)
        else:
            self.response.out.write('%s file does not exist.<br>' % filename)


class FindHandler(webapp2.RequestHandler):
    def post(self):

        filename = self.request.get('find_file')
        found = find(self, filename)

        if not found:
            self.response.out.write('%s file does not exist.<br>' % filename)


class RemoveHandler(webapp2.RequestHandler):
    def post(self):
        
        filename = self.request.get('remove_file')
        removed = remove(filename)

        if removed:
            self.response.out.write('file %s deleted successfully.<br>' % filename)
        else:
            self.response.out.write('%s file does not exist.<br>' % filename)


#------- Mandatory Functions Handler End--------#


#------- Extra Functions Handler Start--------#


class CheckStorageHandler(webapp2.RequestHandler):
    def post(self):
        
        filename = self.request.get('check_storage_file')
        
        if checkStorage(filename):
            self.response.out.write('%s file exists in storage.<br>' % filename)
        else:
            self.response.out.write('%s file does not exist in storage.<br>' % filename)


class CheckCacheHandler(webapp2.RequestHandler):
    def post(self):
        
        filename = self.request.get('check_cache_file')
        
        if checkCache(filename):
            self.response.out.write('%s file exists in memcache.<br>' % filename)
        else:
            self.response.out.write('%s file does not exist in memcache.<br>' % filename)


class RemoveAllCacheHandler(webapp2.RequestHandler):
    def post(self):
        
        removed = removeAllCache()
        
        if removed:
            self.response.out.write('All files deleted from memcache successfully.<br>')
        else:
            self.response.out.write('There was some error while deleting the files from memcache.<br>')


class RemoveAllHandler(webapp2.RequestHandler):
    def post(self):
        
        removed = removeAll()
        self.response.out.write('All files deleted from storage and memcache successfully.<br>')


class CacheSizeMBHandler(webapp2.RequestHandler):
    def post(self):
        
        self.response.out.write('Total size of data in memcache : %s MB<br>' % cacheSizeMB() )


class CacheSizeElemHandler(webapp2.RequestHandler):
    def post(self):
        
        self.response.out.write('Total number of elements in memcache : %s <br>' % cacheSizeElem() )


class StorageSizeMBHandler(webapp2.RequestHandler):
    def post(self):
        
        self.response.out.write('Total size of data in storage : %s MB<br>' % storageSizeMB() )


class StorageSizeElemHandler(webapp2.RequestHandler):
    def post(self):
        
        self.response.out.write('Total number of elements in storage : %s <br>' % storageSizeElem() )


class FindInFileHandler(webapp2.RequestHandler):
    def post(self):
        
        filename = self.request.get('find_in_file')
        value = self.request.get('find_value')
        found = findInFile(filename,value)
        if found == 1:
            self.response.out.write('Text : "%s" found in given file.<br>' % value)
        elif found == 0:
            self.response.out.write('Text : "%s" not found in given file.<br>' % value)
        else:
            self.response.out.write('File : "%s" does not exist.<br>' % filename)



class ListingRegExHandler(webapp2.RequestHandler):
    def post(self):
        
        value = self.request.get('match_value')
        matched_files = listingRegEx(value)

        if len(matched_files)>0:
            self.response.out.write('String : "%s" matched with following files : <br>' % value)
            for filename in matched_files:
                self.response.out.write('%s<br>' % filename)
        else:
            self.response.out.write('No filenames found matching with string : %s .<br>' % value)


class WorkLoadHandler(webapp2.RequestHandler):
    def post(self):
        
        workloadGen(self)


#------- Extra Functions Handler End--------#


app = webapp2.WSGIApplication([('/', MainHandler),
                               ('/list', ListHandler),
                               ('/insert', InsertHandler),
                               ('/check', CheckHandler),
                               ('/remove', RemoveHandler),
                               ('/find', FindHandler),
                               ('/checkstorage', CheckStorageHandler),
                               ('/checkcache', CheckCacheHandler),
                               ('/removeallcache', RemoveAllCacheHandler),
                               ('/removeall', RemoveAllHandler),
                               ('/cachesizemb', CacheSizeMBHandler),
                               ('/cachesizeelem', CacheSizeElemHandler),
                               ('/storagesizemb', StorageSizeMBHandler),
                               ('/storagesizeelem', StorageSizeElemHandler),
                               ('/findinfile', FindInFileHandler),
                               ('/listingregex', ListingRegExHandler),
                               ('/workload', WorkLoadHandler),
                               ],
                              debug=True)
#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import xmlrpclib, pickle, sys, pprint, hashlib, math
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
#from StringIO import StringIO
md5 = hashlib.md5

server=xmlrpclib.ServerProxy('http://localhost:51234')

class Memory(LoggingMixIn, Operations):
    """Example memory filesystem. Supports only one level of files."""

    def __init__(self):
        #File Dictionary
        #self.files = {}
        now = time()
        attrs = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
            st_mtime=now, st_atime=now, st_nlink=2)
        file_dict = dict([['/', attrs]])
        pf=pickle.dumps(file_dict)
        server.put('1', Binary("files"), Binary(pf), 3000)
        #Data Dictionary
        self.data = defaultdict(str)
        pd=pickle.dumps(self.data)#string obj
        server.put('1',Binary("data"), Binary(pd), 3000)
        #fd 
        self.fd=0
        #pfd=pickle.dumps(self.fd)
        #server.put(Binary("fd"), Binary(pfd), 3000)
        fd=str(self.fd)
        pfd=pickle.dumps(fd)
        server.put('1',Binary("fd"), Binary(pfd), 3000)
        #self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
            #st_mtime=now, st_atime=now, st_nlink=2)

    def getRandomNode(self, path):   #hash function to return a random node id
        m = md5(path)
        m_hex = m.hexdigest()
        x = int(m_hex,16)
        x = str(x) 
        #x = x[0:32]   
        return x
        
    def chmod(self, path, mode):
        #self.files[path]['st_mode'] &= 0770000
        #get
        rv=server.get('1',Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        myFiles[path]['st_mode'] &= 0770000
        #self.files[path]['st_mode'] |= mode
        myFiles[path]['st_mode'] |= mode
        #set
        p=pickle.dumps(myFiles)
        server.put('1',Binary("files"), Binary(p), 3000)
        #endSet
        return 0

    def chown(self, path, uid, gid):
        #self.files[path]['st_uid'] = uid
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        myFiles[path]['st_uid'] = uid
        #self.files[path]['st_gid'] = gid
        myFiles[path]['st_gid'] = gid
        #set
        p=pickle.dumps(myFiles)
        server.put('1',Binary("files"), Binary(p), 3000)
        #endSet

    def create(self, path, mode):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        #self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
            #st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        myFiles[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
            st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        #get
        rv=server.get('1',Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        if path not in myFiles:
            raise FuseOSError(ENOENT)
        st = myFiles[path]
        return st

    def getxattr(self, path, name, position=0):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet        
        attrs = myFiles[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def getNumBlocksToTransfer(self, fileSize):
        fileSiz=int(fileSize)
        numBlocksToTransfer=fileSiz/1024
        if fileSiz%1024:
            numBlocksToTransfer+=1
            return numBlocksToTransfer
        else:
            return numBlocksToTransfer
    def getSizeofFile(self, path):    
        #get metadata
        rv=server.get('1', Binary("files")) #metadata is definitely there because it was initialized in init method
        data_str = rv["value"].data
        myFiles = pickle.loads(data_str) #unpickle
        #get
        print 'myFiles', myFiles
        print 'myFiles[path]', myFiles[path]
        print 'myFiles[path][st_size]', myFiles[path]['st_size']  
        fileSize=myFiles[path]['st_size']
        return fileSize  

    def listxattr(self, path):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        attrs = myFiles[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        myFiles[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        myFiles['/']['st_nlink'] += 1
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet   

    def open(self, path, flags):
        #get
        rv=server.get('1', Binary("fd"))
        data_str = rv['value'].data
        myFd = pickle.loads(data_str)
        #endGet
        self.fd = int(myFd)
        self.fd += 1
        #set
        p = pickle.dumps(self.fd)
        server.put('1', Binary('fd'), Binary(p), 3000)
        return self.fd
    #data function needs to go in here
    def read(self, path, size, offset, fh):
        print 'im inside read'
        myData = self.retreiveDataFromServer(path)
        print 'myData=', myData
        print type(myData)
        return myData[path][offset:offset + size]

    def readdir(self, path, fh):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        return ['.', '..'] + [x[1:] for x in myFiles if x != '/']
    #data function needs to go in here
    def readlink(self, path):
        node_id = self.getRandomNode(path)
        #get
        rv=server.get(node_id, Binary("data"))
        data_str = rv['value'].data
        Data = pickle.loads(data_str)
        #endGet
        return Data[path]

    def removexattr(self, path, name):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        attrs = myFiles[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet    

    def rename(self, old, new):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        myFiles[new] = myFiles.pop(old)
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet 
        
    def rmdir(self, path):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        myFiles.pop(path)
        myFiles['/']['st_nlink'] -= 1
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet 

   

    def sendDataToServer(self, path, Data, fileSize):
        print 'inside sendDataToServer method now:' 
        print 'Passed in file size=', fileSize
        numBlocksToTransfer=self.getNumBlocksToTransfer(fileSize)
        print 'i got numBlocksToTransfer=', numBlocksToTransfer
        n=numBlocksToTransfer
        node_id = self.getRandomNode(path)
        print node_id
        if n == 0: #when n is 0 tht means there is no file
            print "inside send Data when n=1"
            print 'beginning send Data when there was no dictionary'
            #set
            p=pickle.dumps(Data)
            server.put(node_id, Binary("data"), Binary(p), 3000)
            #endSet
            
            debug = server.get(node_id, Binary('data'))
            print 'what i just sent = ', debug
            also = debug['value'].data
            this = pickle.loads(also)
            print 'in english what i sent was = ', this
        else:
            if n==1:
                print "inside send Data when n=1"
                p=pickle.dumps(Data)
                server.put(node_id, Binary("data"), Binary(p), 3000)
            # i='1' #string that will be concatenated with new path names
            # d=0 #counter
            # for d in range(0,n): #loop to come up with new path names for each chunk that needs to be sent
            #             print 'in for loop(get), d = ', d
            # newpath = path + i
            # node_id = self.getRandomNode(newpath)
            # rv = server.get(node_id, Binary('data'))
            # data_str = rv['value'].data
            else:
                print 'file is over 1k'

            # returned_dict = pickle.loads(e)
            # value_field = returned_dict[newpath]
            #             data_dump = data_dump + value_field[:]
            # int_i = int(i)  #str to int, increment, then back
            # int_i = int_i + 1   #for concat w/pathname
            # i = str(int_i)

    def retreiveDataFromServer(self, path):
        print 'i am inside retrieve'
        node_id = self.getRandomNode(path)
        rv=server.get(node_id, Binary("data"))
        if not rv:
            print 'rv was zero so there was no file', rv
            #initilize dictionary 
            self.data = defaultdict(str)
            print 'self.data=', self.data
            pd=pickle.dumps(self.data)
            server.put(node_id, Binary("data"), Binary(pd), 3000) #no need for special put because default dic will never be over 1k
            #end init
            print 'i have finished init'
            #get
            rv=server.get(node_id, Binary("data")) #now bring back the dictionary we just sent over
            data_str = rv["value"].data
            myData = pickle.loads(data_str) #unpickle
           
            print 'brought back the initial dic and depickled it'
            #endGet
            print 'data from empty dictionary=', myData #Data is empty here
            print 'finished retrieve Data when there was no dictionary'
            print 'myData1=', myData
            print type(myData)
            return myData
        else: #a return value was present so we need to actually retreive some data (in this case there is no initilization required)    
            fileSize=self.getSizeofFile(path)
            print 'fileSize=', fileSize
            numBlocksToTransfer=self.getNumBlocksToTransfer(fileSize)
            n=numBlocksToTransfer
            print 'numBlocksToTransfer=', n 
            if n == 1: #if there is only 1 block to transfer
            #get
                data_str = rv["value"].data
                myData = pickle.loads(data_str) #unpickle
            #endGet
                print 'finished retreive data when a dictionary was present'
                print 'myData2=', myData
                print type(myData)
                return myData
            
            else:
                print 'this file is over 1k'  
                print 'you need to finish coding'            


    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        attrs = myFiles[path].setdefault('attrs', {})
        attrs[name] = value
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet 

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet
        myFiles[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
            st_size=len(source))
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet
        node_id = self.getRandomNode(path)
        #get
        rv=server.get(node_id, Binary("data"))
        data_str = rv['value'].data
        Data = pickle.loads(data_str)
        #endGet 
        Data[target] = source
        #set
        p=pickle.dumps(Data)
        server.put(node_id, Binary("data"), Binary(p), 3000)
        #endSet 
    #data function needs to go in here
    def truncate(self, path, length, fh=None):
        node_id = self.getRandomNode(path)
        #get
        rv=server.get(node_id, Binary("data"))
        data_str = rv['value'].data
        Data = pickle.loads(data_str)
        #endGet
        Data[path] = Data[path][:length]
        node_id = self.getRandomNode(path)
        #set
        p=pickle.dumps(Data)
        server.put(node_id, Binary("data"), Binary(p), 3000)
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet        
        myFiles[path]['st_size'] = length
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet

    def unlink(self, path):
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet        
        myFiles.pop(path)
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        #get
        rv=server.get('1', Binary("files"))
        data_str = rv['value'].data
        myFiles = pickle.loads(data_str)
        #endGet 
        myFiles[path]['st_atime'] = atime
        myFiles[path]['st_mtime'] = mtime
        #set
        p=pickle.dumps(myFiles)
        server.put('1', Binary("files"), Binary(p), 3000)
        #endSet
        
    def write(self, path, data, offset, fh):
        print 'starting write'
        print 'path=', path
        print 'data to write=', data
        node_id = self.getRandomNode(path)  #figure out node id which represents a server and is particular to a path
        print 'node_id in write function =', node_id #may not need this
        Data = self.retreiveDataFromServer(path) #retrieve any data that may already be saved with this path
        print 'i am back from retreiveDataFromServer method'
        #after retrieve some type of dictionary is present either empty or not
        print 'the Data= ', Data
        print type(Data)
        if not Data: #data is empty if the dictionary brought back was just the initializer in other words if this is a new file path
            print 'Data was empty'
            Data[path] = Data[path][:offset] + data
            print 'data=', data
            print 'Data=', Data
            print 'Data[path]=', Data[path]
            
            #get metadata
            rv=server.get('1', Binary("files")) #metadata is definitely there because it was initialized in init method
            data_str = rv["value"].data
            myFiles = pickle.loads(data_str) #unpickle
            #endGet 
            
            myFiles[path]['st_size'] = len(Data[path]) #update size of file
            a=myFiles[path]['st_size']#fileSize
            print 'a=', a
            print 'i finished if data was empty'
        
        else:  #data was present  
           
            Data[path] = Data[path][:offset] + data #concatinate data
            print 'Data[path]=', Data[path]
                    
            #get metadata
            rv=server.get('1', Binary("files")) #metadata is definitely there because it was initialized in init method
            data_str = rv["value"].data
            myFiles = pickle.loads(data_str) #unpickle
            #endGet 
            
            myFiles[path]['st_size'] = len(Data[path]) #update size of file
            a=myFiles[path]['st_size']#fileSize
            
        #set
        pickledData=pickle.dumps(Data) #pickle new data
        self.sendDataToServer(path, pickledData, a) #send back special this may be over 1k
        #endSet
            
        #set
        p=pickle.dumps(myFiles) #pickle new metadata
        server.put('1', Binary("files"), Binary(p), 3000) #send back (always use same server and file is small so no need for special put as with data)
        #endSet
        
        print'write complete'
        return len(data)


if __name__ == "__main__":
    if len(argv) != 2:
        print 'usage: %s <mountpoint>' % argv[0]
        exit(1)
    fuse = FUSE(Memory(), argv[1], foreground=True)

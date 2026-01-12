import pyarrow as pa
import pyarrow.fs as fs
import io as io


def open(path,arrow=None,mode='rb',mmap=False,encoding="utf-8"):
    if arrow:
        if mmap:
          return  pa.memory_map(path,mode=mode)
        else:
           return pa.OSFile(path,mode=mode)
    else:
       if mode =='r':
          binary_file = open(path,mode='rb')
          return io.TextIOWrapper(binary_file,encoding=encoding)
        
       elif mode =='rb':
            return open(path,mode='rb')
       else:
           raise ValueError(f"mode {mode} not supported or the path {path} is not a valid file path")




def find(data:memoryview ):
    i = 0
    for byte in data:
        i=+1 
        if chr( byte) =='\n':
            return i
            

    


def offset(data:bytes|str|memoryview,chunk):
    if isinstance(data,bytes):
        try:
           return (chunk - data.index(b'\n',chunk))
        except ValueError:
            return -1
    elif isinstance(data,str):
        try:
           return (chunk - data.index('\n',chunk))
        except ValueError:
            return -1
    elif isinstance(data,memoryview):
        try:
           return find(data[chunk:])
        except ValueError:
            return -1
    elif not data:
        raise ValueError("data is empty")
    else:
        raise ValueError("data must be bytes or str")
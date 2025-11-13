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






def offset(data:bytes|str,newline_readhead):
    if isinstance(data,bytes):
        try:
           return data.index(b'\n',newline_readhead)
        except ValueError:
            return -1
    elif isinstance(data,str):
        try:
           return data.index('\n',newline_readhead)
        except ValueError:
            return -1
    elif not data:
        return 0
    else:
        raise ValueError("data must be bytes or str")
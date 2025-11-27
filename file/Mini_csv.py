

import pyarrow as pa
import mmap as mm
import file as fi
import queue as q
import threading  as th
import concurrent.futures as cf
 

x = th.Condition()
y = th.Lock()

cf.ThreadPoolExecutor()

def file__in__chunks(path,chunk_size,newline_readhead,arrow=None,mode='rb',encoding="utf-8"):
    
    file_size = 0

    chunk_rightnow = 0
    leftover = b''
    leftover_prev = b''

    with fi.open(path,arrow=arrow,mode=mode,mmap=False,encoding=encoding) as file:
       done = False
       while not done:
               current_size = chunk_size + newline_readhead
               data = file.read(current_size)
           
               offset = fi.offset(data,chunk_size)
               if offset == -1:
                   raise ValueError("newline character not found in the chunk")
           
               elif offset == 0:
                   done =True 

               else:
                   chunk_rightnow = data[:offset+1+chunk_size]
                   leftover = data[chunk_size+offset+1:]
           
               def return_data(data=chunk_rightnow,leftover_prev=leftover_prev):
                 data = leftover_prev + data
                 return memoryview(data)
                    
               yield return_data 
       leftover_prev = leftover
      


def file__in__chunks_mmap(path,chunk_size,newline_readhead,arrow=None,mode='rb',mmap=True,encoding="utf-8"):
  with open(path,mode=mode) as file:
      file.seek(0,2)
      file_length = file.tell()
      file.seek(0)
      offset = 0
      leftover_prev = 0
      current_map = 0
      file_map = mm.mmap(file.fileno(),length = file_length,access=mm.ACCESS_READ)
      data = memoryview(file_map)
      while current_map < file_length:
           next_point =  min(file_length,(chunk_size + current_map + newline_readhead))
           data_chunk = data[current_map:next_point]
           if next_point < file_length:
              
               offset = fi.offset(data_chunk,(next_point-newline_readhead))
               if offset == -1:
                   raise ValueError("newline character not found in the chunk")
               else:
                  leftover = newline_readhead-(offset+1)
                  new_current = next_point - leftover
           else:
               new_current = file_length
           def return_data(data=data_chunk):
              
               return data[current_map:new_current]
           yield return_data
           current_map = new_current
          

                   
               
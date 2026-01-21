import concurrent.futures as futures
from typing import List

class Arraybyte_pool:
    def __init__(self):
        self.pool:List[bytearray] = []
        self.lock = futures.thread.Lock()
        self._index = 0


    def Rent(self):
          buffer:bytearray = None
          Allocate = False
          with self.lock:
                buffer = self.pool[self._index]
                self._index += 1
                Allocate = buffer is None
          if Allocate:
              buffer = bytearray()
          return buffer
    


    def Return(self, array:bytearray, clear:bool =True):
        if clear:
            array.clear()
        with self.lock:
            if self._index != 0:
                self._index -=1
                self.pool[self._index] = array
               
            

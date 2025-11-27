
import pandas as pd
import json as js
import Column as c
import logging as log
import numpy as np
from abc import ABC, abstractmethod
import Factory as f


class Analysis_Summary(ABC):

    data_types = ['int64','float64','int32','float32','int16','int8','float16']
    float_types = {
            'float16': np.finfo(np.float16),
            'float32': np.finfo(np.float32),
            'float64': np.finfo(np.float64)
        }

    def __init__(self, file_path, chunk_size:int):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.first = False
     
        self.Columns:dict[str,c.Column] = {}
        self.expanded_columns:list[str] = []
        self.col_dtypes:dict[str,str] = {}
        self.row_count = 0
        self.row_expanded = False




    def __getitem__(self, key:str):
        return self.Columns.get(key, None)
    
    @abstractmethod
    def run(file_path, column_names, compound_col_name, compound_col_data, replace_char, target_char, splitchar):
        pass
    
    @classmethod
    def start(cls,engine, file_path,chunksize, column_names:list[str], compound_col_name:list[str]=None, compound_col_data=None, replace_char=None, target_char=None, splitchar=None):
      analysis = f.EngineFactory.get_engine(engine, file_path, chunksize)
      try:
        
        analysis.run( column_names, compound_col_name, compound_col_data, replace_char, target_char, splitchar)
      except Exception as e:
        print(f"Error during analysis: {e}")

      return analysis
    

  
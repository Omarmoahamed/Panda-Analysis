
import pandas as pd
import json as js
import Column as c
import logging as log
import numpy as np

class Analysis_Summary:

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
    

    @classmethod
    def start(cls, file_path,chunksize, column_names:list[str], compound_col_name:list[str]=None, compound_col_data=None, replace_char=None, target_char=None, splitchar=None):
      analysis = cls(file_path, chunk_size=chunksize)
      try:
        
        analysis.optimize(column_names)
        analysis.__summarize_data(file_path, column_names, compound_col_name, compound_col_data, replace_char, target_char, splitchar)
      except Exception as e:
        print(f"Error during analysis: {e}")

      return analysis
    

    def optimize(self,column_names:list[str]):

        no_rows = 0
        if self.chunk_size>50000:
            no_rows = self.chunk_size*0.1
        else:
            no_rows = self.chunk_size

        data = pd.read_csv(self.file_path,nrows=no_rows)
        if not column_names in data.columns:
            raise ValueError("One or more specified columns do not exist in the data.")
        self.optimize_dtypes(column_names, data)

         
    def optimize_dtypes(self,column_names:list[str],data:pd.DataFrame):
        for col in column_names:
            
                if data[col].dtype in self.data_types:
                     min_val = data[col].min()
                     max_val = data[col].max()
                     if data[col].dtype == 'int64':
                       
                        if min_val >= 0:
                            if max_val < 255:
                                self.col_dtypes[col] = 'uint8'
                            elif max_val < 65535:
                                self.col_dtypes[col] = 'uint16'
                            elif max_val < 4294967295:
                                self.col_dtypes[col] = 'uint32'
                            else:
                                self.col_dtypes[col] = 'uint64'
                        else:
                            if min_val > -128 and max_val < 127:
                                self.col_dtypes[col] = 'int8'
                            elif min_val > -32768 and max_val < 32767:
                                self.col_dtypes[col] = 'int16'
                            elif min_val > -2147483648 and max_val < 2147483647:
                                self.col_dtypes[col] = 'int32'
                            else:
                                self.col_dtypes[col] = 'int64'
                     elif data[col].dtype == 'float64':
                                if max_val < self.float_types['float16'].max:
                                    self.col_dtypes[col] = 'float16'
                                elif max_val < self.float_types['float32'].max:
                                    self.col_dtypes[col] = 'float32'
                                else:
                                    self.col_dtypes[col] = 'float64'
                            

        
    def __summarize_data(self,file_path,column_names:list[str],compound_col_name:list[str]=None,compund_col_data =None,replace_char=None,target_char=None,splitchar=None):
       
        chunks = pd.read_csv(file_path, chunksize=self.chunk_size,usecols=column_names,dtype=self.col_dtypes)

        for chunk in chunks :
              for col in column_names:

                self.row_count += chunk.shape[0]
                count = len(chunk[col])
                if col in self.Columns:
                     self.Columns[col].col_count += count
                else:
                     self.Columns[col] = c.Column(count)
                self.calculate(chunk,col)
        self.Average_Calculation(column_names.extend(compound_col_name))
    

  
  
  
    def summarize_compound_data(self,chunk:pd.DataFrame,col_name:list[str],replace_char=None,target_char=None,splitchar=None):
        chunk = self.compound_expand(chunk,col_name,replace_char=replace_char,target_char=target_char,splitchar=splitchar)
        for col in self.expanded_columns:
            if(self.row_expanded):
                self.row_count += chunk.shape[0]
                count = len(chunk[col])
                if col in self.Columns:
                     self.Columns[col].col_count += count
                else:
                     self.Columns[col] = c.Column(count)
                self.calculate(chunk,col)
       
        
    
    def calculate(self,chunk:pd.DataFrame,col:str):
         if chunk[col].dtype in self.data_types:
                     current_col:c.Column = None
                     if col in self.Columns:
                      chunk[col].fillna(0,inplace=True)
                      current_col = self.Columns[col]
                     
                     if current_col:
                         current_col = self.Columns[col]
                         current_max = chunk[col].max()
                         current_col.col_max = max(current_col.col_max, current_max)
                         if current_col.avg is None:
                                current_col.avg = chunk[col].sum()
                         else:
                                current_col.avg += chunk[col].sum()
                     else:
                          log.error(f"Column {col} not found in Columns dictionary.")
                         
                     
                     


                    
    def Average_Calculation(self,cols:list[str]):
        for col in cols:
            if col in self.Columns:
                current_col:c.Column = self.Columns[col]
                current_col.col_avg = current_col.col_avg /current_col.col_count
            else:
                log.error(f"Column {col} not found in Columns dictionary.")

    

    def column_count(self,count,col:str):
            if col in self.col_count:
                self.col_count[col] += count
            else:
                self.col_count[col] = count



    def compound_expand(self,chunk:pd.DataFrame,cols_expand:list[str],replace_char=None,target_char = None,splitchar=None):
       if cols_expand: 
         if replace_char and target_char:
             
              for col in cols_expand:
                   replaced = chunk[col].str.replace(replace_char,target_char)
                   chunk = expand_column(self,chunk,col,replaced,splitchar=splitchar)

         else:
                for col in cols_expand:
                    chunk = expand_column(self,chunk,col,replaced=None,splitchar=splitchar)
         

       def expand_column(self,chunk:pd.DataFrame,col:str,replaced,splitchar = None):
        try:
            if chunk[col].values[0].startswith('{'):
             chunk[col] = replaced.apply(js.loads)
             expand = pd.json_normalize(chunk[col])
             if not self.rows_expanded:
               self.expanded_columns[:] = [col for col in expand.columns]
             chunk = pd.concat([chunk.drop(columns=[col]), expand], axis=1)
            else:
              chunk= chunk.assign(item=chunk[col].str.split(splitchar)).explode('item').drop(columns=[col]).reset_index(drop=True)
              chunk = chunk.rename(columns={'item':col})
              
        except Exception as e:
            print(f"Error expanding column : {e}")

       return chunk
        
         
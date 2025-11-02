
import pandas as pd
import json as js
import Column as c
import logging as log

class Analysis_Summary:
    def __init__(self, file_path, chunk_size:int):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.first = False
     
        self.Columns:dict[str,c.Column] = {}
        self.expanded_columns:list[str] = []
        self.row_count = 0
        self.row_expanded = False




    def __getitem__(self, key:str):
        return self.Columns.get(key, None)
    

    @classmethod
    def start(cls, file_path, column_names:list[str], compound_col_name:list[str]=None, compound_col_data=None, replace_char=None, target_char=None, splitchar=None):
        analysis = cls(file_path, chunk_size=5000)
        analysis.__summarize_data(file_path, column_names, compound_col_name, compound_col_data, replace_char, target_char, splitchar)
        return analysis


    def __summarize_data(self,file_path,column_names:list[str],compound_col_name:list[str]=None,compund_col_data =None,replace_char=None,target_char=None,splitchar=None):
       
        chunks = pd.read_csv(file_path, chunksize=self.chunk_size)

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
         if chunk[col].dtype in ['int64','float64']:
                     current_col:c.Column = None
                     if col in self.Columns:
                      chunk[col].fillna(0,inplace=True)
                      current_col = self.Columns[col]
                     
                     if current_col:
                         current_col = self.Columns[col]
                         current_max = chunk[col].max()
                         current_col.col_max = max(self.max[col], current_max)
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
              chunk= chunk.assign(item=chunk[col].str.split(', ')).explode('item').drop(columns=[col]).reset_index(drop=True)
              chunk = chunk.rename(columns={'item':col})
              
        except Exception as e:
            print(f"Error expanding column : {e}")

       return chunk
        
         
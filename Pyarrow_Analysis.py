import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.compute as pc
import file.Mini_csv as f
import Analysis_Summary as base
import concurrent.futures as futures
import threading as th
import csv as csv
import Factory as fa
import file.Multithreading as mth
import Column as c
import logging as log

@fa.EngineFactory.register_engine("pyarrow")
class Pyarrow_Analysis(base.Analysis_Summary):
    def __init__(self,file_path, chunk_size:int):
        super().__init__(file_path, chunk_size)

    

    def run(self, column_names, compound_col_name, compound_col_data, replace_char, target_char, splitchar):
         
         read_options = csv.ReadOptions(use_threads = True)
         parse_options = csv.ParseOptions(delimiter = ',')
         col_names = self.optimize(column_names=column_names)
         convert_options = csv.ConvertOptions(column_types=self.data_types,column_names=col_names,include_columns=column_names)

         def process(data):
              buffer = pa.BufferReader(pa.py_buffer(data()))
              chunk = pa_csv.read_csv(buffer,read_options=read_options,parse_options=parse_options,convert_options=convert_options)
              self.__summarize_data(chunk,column_names,compound_col_name,compound_col_data,replace_char,target_char,splitchar)
         
         pool = mth.get_pool_threads(4)

         chunk = f.file__in__chunks(self.file_path,32*1024*1024,128*1024)
 
         pool.map(process,chunk)


              
        
              



    

    def __summarize_data(self,chunk,column_names:list[str],compound_col_name:list[str]=None,compund_col_data =None,replace_char=None,target_char=None,splitchar=None):
       
        
        for col in column_names:
                self.row_count += chunk.num_rows
                count = len(chunk[col])
                if col in self.Columns:
                     self.Columns[col].col_count += count
                else:
                     self.Columns[col] = c.Column(count)
                self.calculate(chunk,col)
        self.Average_Calculation(column_names)
    
    def calculate(self,chunk:pa.table,col:str):
         if chunk[col].dtype in self.data_types:
                     current_col:c.Column = None
                     if col in self.Columns:
                      
                      current_col = self.Columns[col]
                     
                     if current_col:
                         fillna_col = chunk[col].fillna(0,inplace=True)
                         chunk =chunk.set_column(chunk.schema.get_field_index(col),col,fillna_col)
                         current_col = self.Columns[col]
                         current_max = pc.max( fillna_col).as_py()
                         current_col.col_max = max(current_col.col_max, current_max)
                         if current_col.avg is None:
                                current_col.avg = pc.sum(chunk[col]).as_py()
                         else:
                                current_col.avg += pc.sum(chunk[col]).as_py()
                     else:
                          log.error(f"Column {col} not found in Columns dictionary.")
                         
                     
    def Average_Calculation(self,cols:list[str]):
        for col in cols:
            if col in self.Columns:
                current_col:c.Column = self.Columns[col]
                current_col.col_avg = current_col.col_avg /current_col.col_count
            else:
                log.error(f"Column {col} not found in Columns dictionary.")

    def optimize(self,column_names:list[str])->list[str]:

        no_rows = 0
        if self.chunk_size>40000:
            no_rows = self.chunk_size*0.1
        else:
            no_rows = self.chunk_size

        data = pa_csv.read_csv(self.file_path,read_options=pa_csv.ReadOptions(limit=no_rows))
        if not column_names in data.columns:
            raise ValueError("One or more specified columns do not exist in the data.")
        self.optimize_dtypes(column_names, data)
        return data.column_names

         
    def optimize_dtypes(self,column_names:list[str],data:pa.table):
        for col in column_names:
            if('id' in col.lower()):
                continue
            else:
                if data[col].type in self.data_types:
                     min_val = pc.min( data[col]).as_py()
                     max_val = pc.max(data[col]).as_py()
                     if data[col].type == 'int64':
                       
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
                     elif data[col].type == 'float64':
                                if max_val < self.float_types['float16'].max:
                                    self.col_dtypes[col] = 'float16'
                                elif max_val < self.float_types['float32'].max:
                                    self.col_dtypes[col] = 'float32'
                                else:
                                    self.col_dtypes[col] = 'float64'
                else:
                          self._optimize_object_dtypes(col,data)


    def _optimize_object_dtypes(self,col:str,data:pa.table):
            if data[col].type == 'object':
                num_unique_values = pc.pc.count_distinct(data[col]).as_py()
                num_total_values = len(data[col])
                if num_unique_values / num_total_values < 0.5:
                    if num_unique_values < 127:
                        self.col_dtypes[col] = pa.dictionary(pa.int8(), pa.string())
                    elif num_unique_values < 32767:
                        self.col_dtypes[col] = pa.dictionary(pa.int16(), pa.string())
                    
                    
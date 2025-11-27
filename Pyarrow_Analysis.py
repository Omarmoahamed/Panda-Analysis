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
              pass
         
         pool = mth.get_pool_threads(4)

         chunk = f.file__in__chunks(self.file_path,32*1024*1024,128*1024)

         for ch in chunk:
              pool.map(process,chunk)


              
        
              



    



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
                if data[col].dtype in self.data_types:
                     min_val = pc.min( data[col])
                     max_val = pc.max(data[col])
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
                else:
                          self._optimize_object_dtypes(col,data)


    def _optimize_object_dtypes(self,col:str,data:pa.table):
            if data[col].dtype == 'object':
                num_unique_values = data[col].nunique()
                num_total_values = len(data[col])
                if num_unique_values / num_total_values < 0.5:
                    self.col_dtypes[col] = 'category'
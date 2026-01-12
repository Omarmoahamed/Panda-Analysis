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
import json as js
import Compound_Columns as cc

@fa.EngineFactory.register_engine("pyarrow")
class Pyarrow_Analysis(base.Analysis_Summary):
    def __init__(self,file_path, chunk_size:int):
        super().__init__(file_path, chunk_size)
        # Dictionary to store compound column objects
        # Key: tuple of column names, Value: compound_column object
        self.compound_columns: dict[tuple, cc.compound_column] = {}

    

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
        
        # Expand compound columns if provided (compound_col_data contains list of column names with combined data)
        expanded_chunk = chunk
        if compund_col_data and splitchar:
            # compound_col_data is a list of column names that contain combined data separated by delimiter
            if isinstance(compund_col_data, list):
                expanded_chunk = self.expand_compound_columns(chunk, compund_col_data, replace_char, target_char, splitchar)
            else:
                log.warning("compound_col_data should be a list of column names for expansion")
        
        # Analyze compound columns if provided (compound_col_name contains group of columns to analyze together)
        if compound_col_name:
            # compound_col_name is a list of column names to analyze together
            analyze_config = {
                'columns': compound_col_name,
                'group_by': []  # Can be extended if needed
            }
            self.summarize_compound_columns(expanded_chunk, analyze_config)
    
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
                num_unique_values = pc.count_distinct(data[col]).as_py()
                num_total_values = len(data[col])
                if num_unique_values / num_total_values < 0.5:
                    if num_unique_values < 127:
                        self.col_dtypes[col] = pa.dictionary(pa.int8(), pa.string())
                    elif num_unique_values < 32767:
                        self.col_dtypes[col] = pa.dictionary(pa.int16(), pa.string())
                    
    def expand_compound_columns(self, chunk: pa.Table, compound_col_data: list[str], 
                                replace_char=None, target_char=None, splitchar=None) -> pa.Table:
        """
        Expands compound columns that contain multiple data separated by delimiter or JSON type.
        This is separate from analysis - it only expands columns.
        
        Args:
            chunk: PyArrow table containing the data
            compound_col_data: List of column names that contain combined data (delimiter-separated or JSON)
            replace_char: Character to replace before splitting
            target_char: Character to replace with
            splitchar: Delimiter to split on (for delimiter-separated columns)
        
        Returns:
            Expanded PyArrow table with compound columns split into separate columns
        """
        if not compound_col_data:
            return chunk
        
        result_chunk = chunk
        
        for col_name in compound_col_data:
            if col_name not in result_chunk.column_names:
                log.warning(f"Compound column '{col_name}' not found in chunk")
                continue
            
            col_data = result_chunk[col_name]
            
            # Check if JSON type (starts with '{')
            try:
                first_val = col_data[0].as_py()
                if first_val and isinstance(first_val, str) and first_val.strip().startswith('{'):
                    # JSON type - expand using JSON parsing
                    result_chunk = self._expand_json_column(result_chunk, col_name, replace_char, target_char)
                elif splitchar:
                    # Delimiter-separated type
                    result_chunk = self._expand_delimiter_column(result_chunk, col_name, replace_char, target_char, splitchar)
            except Exception as e:
                log.warning(f"Could not determine column type for '{col_name}': {e}")
                if splitchar:
                    result_chunk = self._expand_delimiter_column(result_chunk, col_name, replace_char, target_char, splitchar)
        
        return result_chunk
    
    def _expand_json_column(self, chunk: pa.Table, col_name: str, replace_char=None, target_char=None) -> pa.Table:
        """Expands a JSON column into separate columns."""
        try:
            col_data = chunk[col_name]
            col_list = col_data.to_pylist()
            
            # Parse JSON and collect all keys
            all_keys = set()
            parsed_data = []
            
            for val in col_list:
                if val is None:
                    parsed_data.append({})
                    continue
                
                try:
                    # Handle string replacement if needed
                    json_str = str(val)
                    if replace_char and target_char:
                        json_str = json_str.replace(replace_char, target_char)
                    
                    parsed = js.loads(json_str)
                    if isinstance(parsed, dict):
                        parsed_data.append(parsed)
                        all_keys.update(parsed.keys())
                    else:
                        parsed_data.append({})
                except (js.JSONDecodeError, TypeError):
                    parsed_data.append({})
            
            # Create columns for each key
            new_columns_dict = {}
            for key in sorted(all_keys):
                col_name_new = f"{col_name}_{key}"
                col_values = [row.get(key, None) if isinstance(row, dict) else None for row in parsed_data]
                
                # Try to convert to appropriate type
                try:
                    # Try numeric conversion
                    numeric_values = []
                    is_numeric = True
                    for v in col_values:
                        if v is None:
                            numeric_values.append(None)
                        else:
                            try:
                                numeric_values.append(float(v))
                            except (ValueError, TypeError):
                                is_numeric = False
                                break
                    
                    if is_numeric and numeric_values:
                        # Check if all are integers
                        if all(x is None or (isinstance(x, float) and x.is_integer()) for x in numeric_values):
                            new_columns_dict[col_name_new] = pa.array(
                                [int(x) if x is not None else None for x in numeric_values],
                                type=pa.int64()
                            )
                        else:
                            new_columns_dict[col_name_new] = pa.array(numeric_values, type=pa.float64())
                    else:
                        new_columns_dict[col_name_new] = pa.array(
                            [str(v) if v is not None else None for v in col_values],
                            type=pa.string()
                        )
                except Exception:
                    new_columns_dict[col_name_new] = pa.array(
                        [str(v) if v is not None else None for v in col_values],
                        type=pa.string()
                    )
            
            # Remove original column and add new columns
            other_cols = {name: chunk[name] for name in chunk.column_names if name != col_name}
            result_chunk = pa.table({**other_cols, **new_columns_dict})
            
            # Update expanded_columns list
            new_col_names = list(new_columns_dict.keys())
            if not self.expanded_columns:
                self.expanded_columns = new_col_names
            else:
                self.expanded_columns.extend(new_col_names)
            
            return result_chunk
            
        except Exception as e:
            log.error(f"Error expanding JSON column '{col_name}': {e}")
            return chunk
    
    def _expand_delimiter_column(self, chunk: pa.Table, col_name: str, 
                                 replace_char=None, target_char=None, splitchar=None) -> pa.Table:
        """Expands a delimiter-separated column into separate columns."""
        try:
            col_data = chunk[col_name]
            
            # Handle string replacement if needed
            if replace_char and target_char:
                col_data = pc.replace_substring(col_data, replace_char, target_char)
            
            # Split the column by delimiter
            col_list = col_data.to_pylist()
            
            # Determine number of columns after splitting
            num_cols = 0
            for val in col_list:
                if val is not None and str(val).strip():
                    num_cols = len(str(val).split(splitchar))
                    break
            
            if num_cols == 0:
                return chunk
            
            # Split each row and create arrays for each new column
            split_arrays = [[] for _ in range(num_cols)]
            
            for val in col_list:
                if val is None:
                    split_values = [None] * num_cols
                else:
                    split_values = str(val).split(splitchar)
                    # Pad or truncate to match num_cols
                    if len(split_values) < num_cols:
                        split_values.extend([None] * (num_cols - len(split_values)))
                    elif len(split_values) > num_cols:
                        split_values = split_values[:num_cols]
                
                for i, split_val in enumerate(split_values):
                    split_arrays[i].append(split_val.strip() if split_val else None)
            
            # Create new columns and attempt type conversion
            new_columns_dict = {}
            for i in range(num_cols):
                col_name_new = f"{col_name}_{i}"
                col_data_list = split_arrays[i]
                
                # Try to convert to numeric types
                converted_data = []
                is_numeric = True
                is_float = False
                
                for item in col_data_list:
                    if item is None or item == '':
                        converted_data.append(None)
                    else:
                        try:
                            float_val = float(item)
                            converted_data.append(float_val)
                            is_float = True
                        except (ValueError, TypeError):
                            converted_data.append(item)
                            is_numeric = False
                
                # Create array with appropriate type
                if is_numeric and is_float:
                    if all(x is None or (isinstance(x, float) and x.is_integer()) for x in converted_data):
                        try:
                            new_columns_dict[col_name_new] = pa.array(
                                [int(x) if x is not None else None for x in converted_data],
                                type=pa.int64()
                            )
                        except (ValueError, TypeError):
                            new_columns_dict[col_name_new] = pa.array(converted_data, type=pa.float64())
                    else:
                        new_columns_dict[col_name_new] = pa.array(converted_data, type=pa.float64())
                else:
                    new_columns_dict[col_name_new] = pa.array(col_data_list, type=pa.string())
            
            # Remove original compound column and add split columns
            other_cols = {name: chunk[name] for name in chunk.column_names if name != col_name}
            result_chunk = pa.table({**other_cols, **new_columns_dict})
            
            # Update expanded_columns list
            new_col_names = list(new_columns_dict.keys())
            if not self.expanded_columns:
                self.expanded_columns = new_col_names
            else:
                self.expanded_columns.extend(new_col_names)
            
            return result_chunk
            
        except Exception as e:
            log.error(f"Error expanding delimiter column '{col_name}': {e}")
            return chunk
    
    def summarize_compound_columns(self, chunk: pa.Table, analyze_config: dict):
        """
        Summarizes compound columns (group of columns) to find max/min, counts, and unique values.
        This is separate from expansion - it analyzes a group of columns together.
        
        Args:
            chunk: PyArrow table containing the data (should already be expanded if needed)
            analyze_config: Dictionary specifying which columns to analyze together
                Expected format: {
                    'columns': ['product', 'category', 'price', 'quantity'],  # List of columns to analyze
                    'group_by': ['category']  # Optional: columns to group by for counting
                }
        """
        try:
            if not analyze_config or 'columns' not in analyze_config:
                return
            
            columns = analyze_config.get('columns', [])
            group_by_cols = analyze_config.get('group_by', [])
            
            # Validate columns exist
            missing_cols = [col for col in columns if col not in chunk.column_names]
            if missing_cols:
                log.warning(f"Missing columns for compound analysis: {missing_cols}")
                return
            
            # Create or get compound_column object
            columns_tuple = tuple(sorted(columns))
            if columns_tuple not in self.compound_columns:
                self.compound_columns[columns_tuple] = cc.compound_column(columns)
            
            comp_col = self.compound_columns[columns_tuple]
            
            # Find numeric columns for max/min
            numeric_cols = []
            categorical_cols = []
            
            for col in columns:
                col_type = chunk[col].type
                if pa.types.is_floating(col_type) or pa.types.is_integer(col_type):
                    numeric_cols.append(col)
                elif pa.types.is_string(col_type) or pa.types.is_dictionary(col_type):
                    categorical_cols.append(col)
            
            # Find max and min values for numeric columns
            for num_col in numeric_cols:
                self._update_max_min_values(chunk, num_col, columns, comp_col)
            
            # Count by group columns using PyArrow group_by
            for group_col in group_by_cols:
                if group_col in chunk.column_names:
                    self._update_group_counts(chunk, group_col, comp_col)
            
            # Update unique values for categorical columns
            for cat_col in categorical_cols:
                self._update_unique_values(chunk, cat_col, comp_col)
                
        except Exception as e:
            log.error(f"Error summarizing compound columns: {e}")
    
    def _update_max_min_values(self, chunk: pa.Table, value_col: str, all_cols: list[str], comp_col: cc.compound_column):
        """Updates max and min values for a numeric column with associated values from other columns."""
        try:
            # Filter out null values
            valid_mask = pc.is_valid(chunk[value_col])
            if not pc.any(valid_mask).as_py():
                return
            
            # Get max value from this chunk
            max_val = pc.max(chunk[value_col]).as_py()
            if max_val is not None:
                # Check if this is a new maximum (accumulate across chunks)
                existing_max_dict = comp_col.max_values.get(value_col, {})
                current_max = existing_max_dict.get(value_col, float('-inf'))
                
                if max_val > current_max:
                    # Find row with max value
                    mask = pc.equal(chunk[value_col], max_val)
                    filtered = chunk.filter(mask)
                    if filtered.num_rows > 0:
                        max_dict = {col: filtered[col][0].as_py() for col in all_cols if col in filtered.column_names}
                        max_dict[value_col] = max_val  # Include the value itself
                        comp_col.max_values[value_col] = max_dict
            
            # Get min value from this chunk
            min_val = pc.min(chunk[value_col]).as_py()
            if min_val is not None:
                # Check if this is a new minimum (accumulate across chunks)
                existing_min_dict = comp_col.min_values.get(value_col, {})
                current_min = existing_min_dict.get(value_col, float('inf'))
                
                if min_val < current_min:
                    # Find row with min value
                    mask = pc.equal(chunk[value_col], min_val)
                    filtered = chunk.filter(mask)
                    if filtered.num_rows > 0:
                        min_dict = {col: filtered[col][0].as_py() for col in all_cols if col in filtered.column_names}
                        min_dict[value_col] = min_val  # Include the value itself
                        comp_col.min_values[value_col] = min_dict
                    
        except Exception as e:
            log.error(f"Error updating max/min values for '{value_col}': {e}")
    
    def _update_group_counts(self, chunk: pa.Table, group_col: str, comp_col: cc.compound_column):
        """Updates group counts using PyArrow's efficient group_by operation."""
        try:
            if group_col not in chunk.column_names:
                return
            
            # Use PyArrow's group_by for efficient aggregation
            count_array = pa.array([1] * chunk.num_rows, type=pa.int64())
            chunk_with_count = chunk.append_column('_count', count_array)
            
            # Group by and aggregate
            grouped = chunk_with_count.group_by(group_col).aggregate([
                ('_count', 'sum')
            ])
            
            # Extract results and accumulate
            group_col_data = grouped[group_col]
            count_col = grouped['_count_sum']
            
            for i in range(grouped.num_rows):
                group_val = group_col_data[i].as_py()
                count = count_col[i].as_py()
                
                if group_val is not None:
                    if group_col not in comp_col.group_counts:
                        comp_col.group_counts[group_col] = {}
                    if group_val not in comp_col.group_counts[group_col]:
                        comp_col.group_counts[group_col][group_val] = 0
                    comp_col.group_counts[group_col][group_val] += count
                    
        except Exception as e:
            log.error(f"Error updating group counts for '{group_col}': {e}")
    
    def _update_unique_values(self, chunk: pa.Table, col: str, comp_col: cc.compound_column):
        """Updates unique values for categorical columns."""
        try:
            if col not in chunk.column_names:
                return
            
            # Get unique values using PyArrow
            unique_vals = pc.unique(chunk[col])
            
            # Convert to Python set and update
            unique_set = {val.as_py() for val in unique_vals if val.as_py() is not None}
            
            if col not in comp_col.unique_values:
                comp_col.unique_values[col] = set()
            
            comp_col.unique_values[col].update(unique_set)
            
        except Exception as e:
            log.error(f"Error updating unique values for '{col}': {e}")
                    
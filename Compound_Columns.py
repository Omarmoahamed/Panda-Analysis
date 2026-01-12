class compound_column:
    def __init__(self,columns:list):
        self.columns = columns
        self.unique_values = dict()
        self.max_values = dict()  # Store max values with associated column values
        self.min_values = dict()  # Store min values with associated column values
        self.group_counts = dict()  # Store counts grouped by specific columns
        
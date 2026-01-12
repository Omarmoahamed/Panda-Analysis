
class Column:
    def __init__(self,count:int):
        self.first = False
        self.col_max:float = None
        self.col_avg:float = None
        self.col_count = count
        self.col_expanded = False
        self.info = {}

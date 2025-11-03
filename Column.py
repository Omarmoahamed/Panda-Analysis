
class Column:
    def __init__(self,count:int):
        self.first = False
        self.col_max = None
        self.col_avg = None
        self.col_count = count
        self.col_expanded = False
        self.info = {}

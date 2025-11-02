
class Column:
    def __init__(self, file_path, chunk_size:int):
        self.first = False
        self.max = None
        self.avg = None
        self.row_count = 0
        self.info = {}

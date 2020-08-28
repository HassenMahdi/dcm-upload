from abc import abstractmethod


class DataFrameEngine:
    _frame = None

    @property
    def frame(self):
        return self._frame

    @frame.setter
    def frame(self, frame):
        self._frame = frame

    @property
    def columns(self):
        return self.frame.columns.tolist()

    engine = None

    @abstractmethod
    def read_csv(self, filepath, *args,**kwargs):
        pass

    # @abstractmethod
    def __getitem__(self, item):
        return self.frame[item]

    def __setitem__(self, key, value):
        self.frame[key] = value

    def to_dict_generator(self):
        columns = self.columns

        rows = (dict(zip(columns, row)) for row in self.frame.itertuples(index=False, name=None))

        return rows




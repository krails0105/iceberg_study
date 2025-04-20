from abc import abstractmethod, ABC


class Loader(ABC):
    
    @abstractmethod
    def load(cls):
        pass
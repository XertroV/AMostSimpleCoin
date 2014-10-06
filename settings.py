from collections import defaultdict

class Settings(defaultdict):
    __getattr__ = defaultdict.__getitem__
    __setattr__ = defaultdict.__setitem__

settings = Settings()


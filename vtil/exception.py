'''
Created on Jan 18, 2012

@author: vsekhar
'''

import contextlib

@contextlib.contextmanager
def swallowed(exc_type, exc_status_value=None):
    try:
        yield
    except exc_type as swallowed:
        if exc_status_value is not None and swallowed.status != exc_status_value:
            raise

def swallowedf(func, exc_type, exc_status_value=None):
    def wrapper(*args, **kwargs):
        with swallowed(exc_type, exc_status_value):
            return func(*args, **kwargs)
    return wrapper

# can't use a contextmanager because __exit__ cannot throw a new exception

def convertedf(func, src_type, target_type, exc_status_value=None):
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except src_type as e:
            if exc_status_value is not None and exc_status_value != e.status:
                raise
            else:
                raise target_type
    return wrapper

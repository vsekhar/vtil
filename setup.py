'''
Created on Feb 12, 2012

@author: vsekhar
'''

from setuptools import setup

__author__ = 'Vivek Sekhar'
__author_email__ = 'vivek@viveksekhar.ca'

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import sys

from vtil import __version__

if sys.version_info <= (2, 6):
    error = "ERROR: vtil requires Python version 2.6 or above... exiting"
    print >> sys.stderr, error
    sys.exit(1)

setup_args = dict(
        name = 'vtil'
        , description = "My personal util library"
        , version = __version__
        , author = __author__
        , author_email = __author_email__
        , packages = ['vtil']
        , test_suite = 'tests' 
        )

if sys.version_info[:2] >= (2, 4):
    setup_args['classifiers']=[
            "Development Status :: 2 - Pre-Alpha"
            , "Intended Audience :: Developers"
            , "License :: OSI Approved :: GNU General Public License (GPL)"
            , "Operating System :: POSIX :: Linux"
            , "Programming Language :: Python :: 2.7"
            , "Topic :: Utilities"
        ]

def run():
    return setup(**setup_args)

if __name__ == '__main__':
    run()

    # refresh plugin cache
    # from twisted.plugin import IPlugin, getPlugins
    # list(getPlugins(IPlugin))


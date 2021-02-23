# Encoding: utf-8

# --
# Copyright (c) 2008-2021 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

import os

from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as description:
    LONG_DESCRIPTION = description.read()

setup(
    name='nagare-sessions-memcache',
    author='Net-ng',
    author_email='alain.poirier@net-ng.com',
    description='Nagare sessions in memcache',
    long_description=LONG_DESCRIPTION,
    license='BSD',
    keywords='',
    url='https://github.com/nagareproject/sessions-memcache',
    packages=find_packages(),
    zip_safe=False,
    setup_requires=['setuptools_scm'],
    use_scm_version=True,
    install_requires=[
        'python-memcached',
        'nagare-services-memcache',
        'nagare-server-http',
        'nagare-services-sessions'
    ],
    entry_points='''
        [nagare.sessions]
        memcache = nagare.sessions.memcached_sessions:Sessions
    '''
)

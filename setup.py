# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

from django_fabfile import __doc__


setup(
    name='django-fabfile',
    url='http://redmine.odeskps.com/projects/django-fabfile',
    download_url='http://pypi.odeskps.com/django-fabfile',
    version='2011.08.13.2',
    description='Deployment Django projects with Fabric.',
    install_requires=[
        'boto>=2.0b4',
        'odeskps-Fabric>=1.2b1',
    ],
    long_description=__doc__,
    maintainer='Max Chervonec',
    maintainer_email='electedm@odesk.com',
    packages=find_packages(),
    include_package_data=False,
    package_data = {
        '': ['*.rst', '*.def'],
    },
    zip_safe=True,
    # Get more strings from http://www.python.org/pypi?:action=list_classifiers
    classifiers=[
        'Development Status :: ',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)

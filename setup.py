from setuptools import setup

from django_fabfile import __author__, __doc__, __version__


setup(
    name='django-fabfile',
    version=__version__,
    description='Deployment Django projects with Fabric.',
    long_description=__doc__,
    author=__author__,
    maintainer='Yuri Yurevich',
    include_package_data=True,
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

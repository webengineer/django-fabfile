from setuptools import setup, find_packages


execfile('django_fabfile/version.py')  # Set __version__ into namespace.


setup(
    name='django-fabfile',
    url='http://packages.python.org/django-fabfile',
    download_url='http://pypi.python.org/pypi/django-fabfile/',
    version=__version__,
    description='Fabric tasks for Django and Amazon Web Services',
    install_requires=[
        'boto>=2.0b4',
        'Fabric>=1.2',
        'python-dateutil>=1.5,<2.0',
    ],
    long_description=open('README.rst').read(),
    author='oDesk',
    author_email='django-fabfile@odeskps.com',
    maintainer='Max Chervonec',
    maintainer_email='electedm@odesk.com',
    packages=find_packages(),
    license='GNU GPLv3',
    package_data = {
        '': ['*.def', '*.txt', '*.rst', 'encrypted_root.tar.gz'],
    },
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
        'Topic :: System :: Software Distribution',
        'Topic :: System :: Systems Administration',
    ],
)

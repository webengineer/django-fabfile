from setuptools import setup, find_packages


setup(
    name='django-fabfile',
    url='https://github.com/webengineer/django-fabfile',
    download_url='http://pypi.python.org/pypi/django-fabfile/',
    version='2011.08.28.2',
    description='Fabric tasks for Django and Amazon Web Services',
    install_requires=[
        'boto>=2.0b4',
        'odeskps-Fabric>=1.2b1',
    ],
    long_description=open('README.rst').read(),
    author='Max Chervonec',
    author_email='electedm@odesk.com',
    packages=find_packages(),
    include_package_data=False,
    package_data = {
        'django-fabfile': ['*.def', '*.rst'],
    },
    zip_safe=True,
    # Get more strings from http://www.python.org/pypi?:action=list_classifiers
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
        'Topic :: System :: Software Distribution',
        'Topic :: System :: Systems Administration',
    ],
)

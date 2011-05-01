from setuptools import setup, find_packages

from django_fabfile import __doc__


setup(
    name='django-fabfile',
    url='http://redmine.odeskps.com/projects/django-fabfile',
    download_url='http://pypi.odeskps.com/django-fabfile',
    version='0.9.6.4',
    description='Deployment Django projects with Fabric.',
    install_requires=[
        'boto>=2.0b4',
        'Fabric>=0.9.3',
    ],
    long_description=__doc__,
    maintainer='Yury Yurevich',
    maintainer_email='yyurevich@jellycrystal.com',
    packages=find_packages(),
    include_package_data=False,
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

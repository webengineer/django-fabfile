Fabric tasks for Django and Amazon Web Services
***********************************************

Required arrangements
---------------------

.. note:: `django_fabfile` package should be listed in your path. It's already
   in place if your are installed it using `pip install django-fabfile`. But if
   you are working within repository, then::

    export PYTHONPATH=<path-to-folder-with-django_fabfile-package>

   in your shell.

Pre-run configuration
---------------------

Django settings will be checked first if environment variable
`DJANGO_SETTINGS_MODULE` configured properly. If option was not configured
within Django settings, then its value will be taken from ``./fabfile.cfg``
file - copy-paste there rows that should be overriden from defaults
:download:`django_fabfile/fabfile.cfg.def <../django_fabfile/fabfile.cfg.def>`.

Django settings should be configured in following form (with preserving
sections case)::

    FABFILE = {
        'DEFAULT': {
            'LOGGING_FOLDER': '.',
        },
        'Credentials': {
            'AWS_ACCESS_KEY_ID': 'AKIAABCDEFGHIJKLMNOP',
            'AWS_SECRET_ACCESS_KEY': 'ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLM',
        },
    }

.. note::
   Boto config files http://code.google.com/p/boto/wiki/BotoConfig will be
   examined at least - if ``[Credentials]`` or some other options was filled
   neither in Django settings nor in ``./fabfile.cfg`` or :download:`django_fabfile/fabfile.cfg.def <../django_fabfile/fabfile.cfg.def>`.

Example of required AWS IAM Policy
----------------------------------

For using Fabric tasks you need AWS account with following permissions:

* create-snapshot::

    {
      "Statement": [
        {
          "Sid": "Stmt1304003463574",
          "Action": [
            "ec2:CreateSnapshot",
            "ec2:CreateTags",
            "ec2:DescribeInstances",
            "ec2:DescribeRegions",
            "ec2:DescribeSnapshots",
            "ec2:DescribeTags",
            "ec2:DescribeVolumes"
          ],
          "Effect": "Allow",
          "Resource": "*"
        }
      ]
    }

* delete-snapshot::

    {
      "Statement": [
        {
          "Sid": "Stmt1306410750989",
          "Action": [
            "ec2:DescribeRegions",
            "ec2:DescribeSnapshots",
            "ec2:DeleteSnapshot"
          ],
          "Effect": "Allow",
          "Resource": "*"
        }
      ]
    }

* reboot-instance::

    {
      "Statement": [
        {
          "Sid": "Stmt1312204628195",
          "Action": [
            "ec2:RebootInstances"
          ],
          "Effect": "Allow",
          "Resource": "*"
        },
        {
          "Sid": "Stmt1312276311113",
          "Action": [
            "ec2:DetachVolume"
          ],
          "Effect": "Allow",
          "Resource": "*"
        }
      ]
    }

* rsync-snapshot::

    {
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "EC2:AttachVolume",
            "EC2:CreateKeyPair",
            "EC2:CreateVolume",
            "EC2:DeleteKeyPair",
            "EC2:DeleteVolume",
            "EC2:DescribeAvailabilityZones",
            "EC2:DescribeImages",
            "EC2:DescribeKeyPairs",
            "EC2:RunInstances",
            "EC2:TerminateInstances"
          ],
          "Resource": "*"
        }
      ]
    }

Backup script
-------------

.. note:: Instances and snaphots are filtered by tag "Earmarking" value
   "production". This can be configured in settings - see "Pre-run
   configuration" section above.

Following backup commands may be configured with crontab to run daily::

    #!/bin/sh

    VE=/home/backup-agent/.virtualenvs/backup

    . $VE/bin/activate && \
    fab --keepalive=60 -f $VE/lib/python2.6/site-packages/django_fabfile/backup.py \
            backup_instances_by_tag \
            trim_snapshots \
            rsync_region:src_region_name=us-east-1,dst_region_name=eu-west-1 \
            rsync_region:src_region_name=us-west-1,dst_region_name=eu-west-1 \
            rsync_region:src_region_name=eu-west-1,dst_region_name=us-east-1

With 30 production instances it tooks around 6 hours to replicate
snapshots updates. First replication tooks much more time obviously.

Running tests
-------------

The package has test cases to make sure, that all methods, being tested,
keep on the entire program logic. The test cases are using packages ``fudge``
and ``unittest``. The test files (``'test_*.py'``) are located in folder
``'/django_fabfile/tests/'``. To run tests from command line, implement::

    python -m unittest discover django_fabfile

Recent changes
--------------

See :doc:`changelog` for recent updates.

Licensing
---------

The product is licensed by oDesk http://www.odesk.com/ under
:download:`GNU GENERAL PUBLIC LICENSE Version 3<license.txt>` except
portions with attached inline license information like
:func:`django_fabfile.backup._trim_snapshots`.

========================================
Batch commands to be started with Fabric
========================================

Imported functions should be prepended with underscore to hide them from
``fabric --list``.

Git branches should be named as ``staging`` for staging server and
``production`` for production one.

Required settings
-----------------

Required settings should be placed in project settings::

    APPS_TO_TEST = (
        'sms',
        'testing',
        'timezone',
        'translations',
    )

    # Project repository.
    GIT_REMOTE = 'git@github.com:webengineer/ott2.git'

    # Logging setup.
    LOGS = {
        'common': 'var/messages.log',
        'payment': 'var/payments.log',
        'exception': 'var/errors.log',
        'sms': 'var/sms.log',
    }

    # Staging and production servers.
    STAGING_HOST = 'staging.ott.odeskps.com'
    STAGING_PRJ_DIR = '/var/www/ott2/ott2'
    PRODUCTION_HOST = 'ott.odeskps.com'
    PRODUCTION_PRJ_DIR = STAGING_PRJ_DIR
    VIRTUALENV_DIR = 'virtenv'

    # Project repository access key.
    SSH_KEYFILE = '/home/user/.ssh/id_user'

Optional settings
-----------------

    # Managing Websitepulse alerts.
    WB_PULSE_USERNAME = 'odeskps'
    WB_PULSE_API_KEY = 'OVERRIDE ME IN LOCAL SETTINGS'
    WB_PULSE_LOGIN_TARGET = '73992'
    WB_PULSE_HEALTHCHECK_TARGET = '74422'

    SUDO_EXPORT = 'HOME=/home/odeskps/ PYTHON_EGG_CACHE=/tmp'

fabfile.cfg help
----------------

  1. Section [main] must contain instance id and region for source instance;
     Sample:
        [main]
        instance_id = i-foobar
        region = us-east-1
  2. Section [purge_backups] must contain schedule for saving backups (needed by trim_snapshots command):
     Sample:
        [purge_backups]
        dry_run = True ;default True (run without deleting)
        hourly_backups = 24 ; # of hourly backups for this day
        daily_backups = 7 ; # of daily backups for last week
        weekly_backups = 4 ; # of weekly backups for this month
        monthly_backups = 12 ; # of monthly backups for this year
        quarterly_backups = 4 ; # of backups from every quarter for last year
        yearly_backups = 10 ; # of every year backups for 10 years
  3. Section [mount_backups] contains settings for mounting snapshots on temporary instance;
     Sample:
        [mount_backups]
        username = ubuntu
        root_device_type = ebs
        ami_regexp = ^ebs/ubuntu-images/ubuntu-[a-z]+-(?P<version>\d{1,2}\.\d{2,2})-[a-z3264]+-server-(?P<released_at>\d{8,8}(\.\d{1,1})?)$
        ami_ptrn_with_relase_date = ebs/ubuntu-images/ubuntu-*-{version}-*-server-{released_at}
        ami_ptrn_with_version = ebs/ubuntu-images/ubuntu-*-{version}-*-server-*
        architecture = x86_64
        mountpoint = /media/snapshot
        device = /dev/sdm
        ami_ptrn = ebs/ubuntu-images/ubuntu-*-server-*
        ubuntu_aws_account = 099720109477
  4. Region-named section must contain keyname and keyfile path for source instance (needed by mount_backup command)

USAGE:
------

  1. For backup creation you must specify instance_id and region in [main] section. To create snapshot of mounted volume, run:
          fab -f backup.py backup_instance
  2. To purge old snapshots you must specify # of snapshots to save in [mount_backups], specify instance_id and region in [main] section.
     Then run:
          fab -f backup.py trim_snapshots
  3. To mount backup specify needed values in [mount_backups] section of fabfile.cfg, specify instance_id and region in [main] section.
     Then run:
          fab -f backup.py mount_snapshot

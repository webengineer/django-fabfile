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

"""Batch commands to be started with Fabric.

Imported functions should be prepended with underscore to hide them from
``fabric --list``.

Git branches should be named as ``staging`` for STAGING server and
``production`` for PRODUCTION one.

Required settings should be placed in project settings::

    APPS_TO_TEST = (
        'sms',
        'testing',
        'timezone',
        'translations',
    )

    # Project repositories.
    PROD_REPO = 'git@github.com:user/mysite.git'
    DEV_REPO = 'git@github.com:webengineer/mysite.git'
    PULL_REPO = DEV_REPO    # PROD_REPO for gatekeeper.

    # Logging setup.
    LOGS = {
        'common': 'var/messages.log',
        'payment': 'var/payments.log',
        'exception': 'var/errors.log',
        'sms': 'var/sms.log',
    }

    # Staging and production servers.
    STAGING = {
            'hostname': 'staging.dot.com',
            'ec2_instance': 'i-d86a73b0',
            'ip': '204.236.215.216',
            'proj_dir': '/var/www/ott/mysite',
            'virtualenv_dir': 've',
    }
    PRODUCTION = {
            'hostname': 'translation.dot.com',
            'ec2_instance': 'i-67ae4b0c',
            'ip': '184.73.37.110',
            'proj_dir': '/var/www/ott/mysite',
            'virtualenv_dir': 've',
    }
    VIRTUALENV_DIR = 've'

    # Project repository access key.
    SSH_KEYFILE = '/home/user/.ssh/id_user'

    # Should be same on all instances.
    MAINTENANCE_FILE = 'var/maintenance.lck'

Optional settings::

    # Managing Websitepulse alerts.
    WB_PULSE_USERNAME = 'odeskps'
    WB_PULSE_API_KEY = 'OVERRIDE ME IN LOCAL SETTINGS'
    WB_PULSE_LOGIN_TARGET = '73812'
    WB_PULSE_HEALTHCHECK_TARGET = '79432'

    SUDO_EXPORT = 'HOME=/home/odeskps/ PYTHON_EGG_CACHE=/tmp'
"""

from os.path import exists as _exists
from re import search as _search
from time import sleep as _sleep
import urllib2 as _urllib2, urllib as _urllib

from boto.ec2.connection import EC2Connection as _EC2Connection
from fabric.api import abort, cd, env, hosts, local, prompt, run, settings
from fabric.contrib.console import confirm

# TODO Provide in project settings.
from settings import (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
        APPS_TO_TEST, PROD_REPO, DEV_REPO, PULL_REPO, LOGS,
        MAINTENANCE_FILE, PRODUCTION, SSH_KEYFILE, STAGING,
        VIRTUALENV_DIR)

try:
    from settings import (WB_PULSE_USERNAME, WB_PULSE_API_KEY,
            WB_PULSE_LOGIN_TARGET, WB_PULSE_HEALTHCHECK_TARGET)
except ImportError:
    NO_WB_PULSE_CONFIG = True
else:
    NO_WB_PULSE_CONFIG = False

try:
    from settings import SUDO_EXPORT
except:
    SUDO_EXPORT = ''


WB_API_URL='http://api.websitepulse.com/textserver.php'

env.key_filename = SSH_KEYFILE
env.user = 'odeskps'

SUDO_VIRTUALENV = 'source %s/bin/activate && %s ' % (VIRTUALENV_DIR, SUDO_EXPORT)
MANAGE = './manage.py '


def _check_git_branch_name(name):
    with settings(warn_only=True):
        res = local('git check-ref-format --branch %s' % name)
        if res.failed:
            raise Exception('"%s" is not a valid branch name' % name)
        else:
            return name


def _get_current_branch():
    """Returns name of current git branch.

    Creates new branch if not currently on any branch."""
    branches = local('git branch')
    branch = _search(r'\* (?P<branch>\(no branch\)|\S+)', branches).group(1)
    if branch == '(no branch)':
        local('git branch -r', capture=False)
        branch = prompt('Please specify new branch name:',
                        validate=_check_git_branch_name)
        local('git checkout -b %s' % branch)
    return branch


def pull_updates():
    """Pulls updates from remote staging and production branches."""
    local('git fetch %s staging:staging' % PULL_REPO)
    local('git checkout production')
    local('git pull %s production' % PULL_REPO)


def _disable_websitepulse_alerts(suspend=True):
    if NO_WB_PULSE_CONFIG: return
    data = {'username': WB_PULSE_USERNAME,
            'key': WB_PULSE_API_KEY,
            'action': 'suspend' if suspend else 'activate',
            'method': 'UpdateTargetStatus',
            'format': 'xml',
            'target': ','.join([WB_PULSE_LOGIN_TARGET,
                                WB_PULSE_HEALTHCHECK_TARGET])}
    print "%s monitoring for all targets" % (
            "Suspending" if suspend else "Activating")
    response = _urllib2.urlopen(_urllib2.Request(WB_API_URL,
                                               _urllib.urlencode(data))).read()


def _enable_websitepulse_alerts():
    _disable_websitepulse_alerts(suspend=False)


def prepare_deploy():
    """Run tests and commit staged chages."""
    feature = _get_current_branch()
    with settings(warn_only=True):
        res = local('git commit', capture=False)
        if res.failed and not confirm('Continue anyway?'):
            abort("Aborting at user request.")
    pull_updates()
    local('git checkout %s' % feature)
    with settings(warn_only=True):
        merge = local('git merge production')
    if merge.failed:
        local('git mergetool', capture=False)
        local('git commit', capture=False)
    local('python bootstrap.py')
    # Run tests.
    local(MANAGE + 'clean_pyc')
    local(MANAGE + 'test --settings=test_settings '
          + ' '.join(APPS_TO_TEST), capture=False)
    local(MANAGE + 'dryrun', capture=False)
    local(MANAGE + 'syncdb --noinput', capture = False)
    local(MANAGE + 'migrate --noinput', capture = False)
    if confirm('Do you like to run healthcheck?', default=False):
        local(MANAGE + 'healthcheck', capture=False)


@hosts(PRODUCTION['hostname'])
def backup_production():
    """Create backup dump and transfer to the STAGING server."""
    with cd(PRODUCTION['proj_dir']):
        run(SUDO_VIRTUALENV + 'bin/backup_db.sh', pty=True)
        run(SUDO_VIRTUALENV + 'bin/xfer_backups.sh', pty=True)


def _run_pull():
    """Update remote repo.

    Should be started within project directory."""
    if env['host_string'] == STAGING['hostname']:
        run('git checkout staging', pty=True)
    run('git pull', pty=True)
    # Setup virtualenv.
    run('python bootstrap.py')
    for log_file in LOGS.values():
        if not _exists(log_file):
            run('touch %s' % log_file, pty=True)
            run('chmod a+w %s' % log_file, pty=True)


def _run_migrate_restart():
    """Migrates DB and restarts HTTP server.

    Should be started within project directory."""
    run(MANAGE + 'syncdb --noinput', pty=True)
    run(MANAGE + 'migrate --noinput', pty=True)
    # Apache's mod_wsgi is used in daemon mode, so it's enough
    # to touch wsgi script to restart mod_wsgi processes without
    # restarting whole Apache.
    run('touch django.wsgi', pty=True)


def _run_tests():
    """Run tests.

    Should be started within project directory."""
    run(MANAGE + 'clean_pyc', pty=True)
    run(MANAGE + 'test --settings=test_settings ' + ' '.join(APPS_TO_TEST),
         pty=True)
    run(MANAGE + 'dryrun', pty=True)


def _run_healthcheck():
    """Restart remote server and test health.

    Should be started within project directory."""
    if env['host_string'] in (STAGING['hostname'], PRODUCTION['hostname']):
        host = 'http://%s/' % env['host_string']
        return run(MANAGE + 'healthcheck --realserver="%s"' % host, pty=True)
    else:
        return run(MANAGE + 'healthcheck', pty=True)


@hosts(PRODUCTION['hostname'])
def maintain_production(restore=False):
    """Enter maintenance mode on PRODUCTION."""
    with cd(PRODUCTION['proj_dir']):
        if not restore:
            run('touch %s' % MAINTENANCE_FILE, pty=True)
            _disable_websitepulse_alerts()
        else:
            run('rm %s' % MAINTENANCE_FILE, pty=True)
            _enable_websitepulse_alerts()

        # Apache's mod_wsgi is used in daemon mode, so it's enough
        # to touch wsgi script to restart mod_wsgi processes without
        # restarting whole Apache.
        run('touch django.wsgi', pty=True)


@hosts(PRODUCTION['hostname'])
def restore_production():
    """Exit from maintenance mode on PRODUCTION."""
    maintain_production(restore=True)


@hosts(STAGING['hostname'])
def deploy_staging():
    """Merges current branch into STAGING."""
    feature = _get_current_branch()
    local('git checkout staging')
    with settings(warn_only=True):
        merge = local('git merge --no-ff ' + feature)
    if merge.failed:
        local('git mergetool', capture=False)
    with settings(warn_only=True):
        local('git commit', capture=False)
    local(' '.join(['git push -f', DEV_REPO, 'staging']))
    local(' '.join(['git push', PROD_REPO, 'staging']))
    with cd(STAGING['proj_dir']):
        _run_pull()
        _run_tests()
        _run_migrate_restart()
        with settings(warn_only=True):
            res = _run_healthcheck()
            if res.failed and not confirm('Continue anyway?'):
                abort('Aborting at user request.')


@hosts(PRODUCTION['hostname'])
def deploy_production(maintain=False):
    """Deploy staging branch into PRODUCTION."""
    maintain = confirm('Do you like to ban users with maintenance mode?',
                       default=maintain)
    local('git checkout production')
    with settings(warn_only=True):
        merge = local('git merge staging')
    if merge.failed:
        local('git mergetool', capture=False)
        local('git commit', capture=False)
    local(' '.join(['git push -f', DEV_REPO, 'production']))
    local(' '.join(['git push', PROD_REPO, 'production']))
    backup_production()
    with cd(PRODUCTION['proj_dir']):
        if maintain:
            maintain_production()
        else:
            _disable_websitepulse_alerts()
        _run_pull()
        _run_migrate_restart()
        run('sudo -u www-data crontab -u www-data crontab', pty=True)
        _run_healthcheck()
        if maintain:
            restore_production()
        else:
            _enable_websitepulse_alerts()


def deploy(maintain=False):
    """Deploy staged updates of current branch."""
    start_staging()
    prepare_deploy()
    with settings(host_string=STAGING['hostname']):
        deploy_staging()
    if not confirm('STAGING updated, do you like to proceed with PRODUCTION?'):
        abort('Aborting at user request.')
    with settings(host_string=PRODUCTION['hostname']):
        deploy_production(maintain)
    if confirm('Do you like to shutdown STAGING server?'):
        stop_staging()


def stop_staging():
    """Shut down STAGING server to prevent environment pollution."""
    print 'Logging into Amazon AWS....'
    conn = _EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    print 'Stopping ' + STAGING['hostname'] + ' server:'
    print conn.stop_instances(STAGING['ec2_instance'])


def start_staging():
    """Start STAGING server."""
    print 'Logging into Amazon AWS....'
    conn = _EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    print 'Starting ' + STAGING['hostname'] + ' server:'
    print conn.start_instances(STAGING['ec2_instance'])
    _sleep(10) # seconds. Otherwise will get "The instance is not in the
              # 'running' state" error.
    print 'Associate Elastic IP with ' + STAGING['hostname'] + ' server:'
    print conn.associate_address(STAGING['ec2_instance'], STAGING['ip'])
    print '(Note that it might take a couple of minutes for the Elastic IP to work....)'


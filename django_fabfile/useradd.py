from fabric.api import env, settings, sudo, abort, put, task
from os.path import isfile as _isfile

from django_fabfile.utils import Config, get_inst_by_id


config = Config()


try:
    preconfigured_user = config.get('DEFAULT', 'USERNAME')
except:
    pass    # Expecting user to be provided as `-u` option.
else:
    FABRIC_DEFAULT_USER = 'user'    # XXX `-u user` will be overridden.
    if env['user'] == FABRIC_DEFAULT_USER:  # Not provided as `-u` option.
        env.update({'user': preconfigured_user})

env.update({'disable_known_hosts': True})


def _sudo(cmd):
    """ Shows output of cmd and allows interaction """
    sudo(cmd, shell=False, pty=True)


def _create_account(username, region, instance_ids, passwordless, sudo):
    if not _isfile(username + '.pub'):
        abort("%s.pub does not exist" % username)
    env.ssh_key = username + '.pub'
    env.username = username     # Own attribute for string formatting.
    if passwordless:
        _sudo('adduser --disabled-password %(username)s' % env)
        if sudo:
            _sudo('sed -i "s/# %sudo ALL=NOPASSWD: ALL/'
                            '%sudo ALL=NOPASSWD: ALL/" /etc/sudoers')
            for group in ['sudo']:
                with settings(group=group):
                    _sudo('adduser %(username)s %(group)s' % env)
    else:
        _sudo('adduser %(username)s' % env)
        if sudo:
            for group in ['adm', 'admin', 'staff']:
                with settings(group=group):
                    _sudo('adduser %(username)s %(group)s' % env)
    _sudo('mkdir -p /home/%(username)s/.ssh' % env)
    _sudo('touch /home/%(username)s/.ssh/authorized_keys' % env)
    _sudo('chown -R %(username)s: /home/%(username)s/.ssh' % env)
    _sudo('chmod 700 /home/%(username)s/.ssh' % env)
    put(env.ssh_key, '/home/%(username)s/.ssh/authorized_keys'
                                      % env, use_sudo=True)
    _sudo('chown -R %(username)s: /home/%(username)s/.'
                                    'ssh/authorized_keys' % env)
    _sudo('chmod 600 /home/%(username)s/.ssh/authorized_keys' % env)


@task
def deluser(name, region=None, instance_ids=None):
    """
    Removes user <name> with deluser from "host1;host2" list in <region>
    If region and instance_ids not set - script takes hosts amd key values
    from command line (-H and -i).
    """
    if instance_ids and region:
        instances_ids = list(unicode(instance_ids).split(';'))
        for inst in instances_ids:
            if inst:
                _instance = get_inst_by_id(region, inst)
                if not env.key_filename:
                    key_filename = config.get(_instance.region.name,
                                                      'KEY_FILENAME')
                    env.update({'key_filename': key_filename,
                                                  'warn_only': True})
                env.update({'host_string': _instance.public_dns_name})
                env.username = name
                _sudo('deluser %(username)s' % env)
    else:
        env.update({'warn_only': True})
        env.username = name
        _sudo('deluser %(username)s' % env)


@task
def adduser(username, region=None, instance_ids=None,
                                passwordless=None, sudo=None):
    """
    creates new <username> with public SSH key on "host1;host2" list in
    <region>. If you want to create passwordless account - set any value to
    <passwrdless> variable, if you want sudo rights - set any value to <sudo>.
    File with public key must be in same directory.
    If region and instance_ids not set - script takes hosts amd key values
    from command line (-H and -i).
    Usage:
    1. WIthout aws api keys and config present:
    :<username>,<passwordless=1>,<sudo=1> - in this case you have to specify
    hosts list in -H and your own account in -u fabric parameters.
    2. With aws api keys and config entries:
    :<username>,<region>,"instance1;instance2",<passwordless>,<sudo>
    Extracts IP's from instance description.
    """
    if instance_ids and region:
        instances_ids = list(unicode(instance_ids).split(';'))
        for inst in instances_ids:
            if inst:
                _instance = get_inst_by_id(region, inst)
                if not env.key_filename:
                    key_filename = config.get(_instance.region.name,
                                                      'KEY_FILENAME')
                    env.update({'key_filename': key_filename})
                env.update({'host_string': _instance.public_dns_name})
                _create_account(username, region, instance_ids, passwordless,
                                                                    sudo)
    else:
        _create_account(username, region, instance_ids, passwordless, sudo)

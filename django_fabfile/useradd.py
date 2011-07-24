from fabric.api import env, settings, sudo, abort, put
from ConfigParser import ConfigParser as _ConfigParser
from re import match as _match
from os.path import isfile as _isfile
from boto.ec2 import regions as _regions


config_file = 'fabfile.cfg'
config = _ConfigParser()
config.read(config_file)
username = config.get('DEFAULT', 'username')

env.update({'disable_known_hosts': True, 'user': username})


def _get_region_by_name(region_name):
    """
    Not using from backup import _get_region_by_name because
    backup.py reqires existing aws api keys.
    Allow to specify region name fuzzyly.
    """
    matched = [reg for reg in _regions() if _match(region_name, reg.name)]
    assert len(matched) > 0, 'No region matches {0}'.format(region_name)
    assert len(matched) == 1, 'Several regions matches {0}'.format(region_name)
    return matched[0]


def _get_inst_by_id(region, instance_id):
    conn = _get_region_by_name(region).connect()
    res = conn.get_all_instances([instance_id, ])
    assert len(res) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(res,
                                                      instance_id))
    instances = res[0].instances
    assert len(instances) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(instances,
                                                              instance_id))
    return instances[0]


def _sudo(cmd):
    """ Shows output of cmd and allows interaction """
    sudo(cmd, shell=False, pty=False)


def _create_account(user, region, instance_ids, passwordless, sudo):
    if not _isfile(user + '.pub'):
        abort("%s.pub does not exist" % user)
    env.ssh_key = user + '.pub'
    env.username = user
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
    put(user + '.pub', '/home/%(username)s/.ssh/authorized_keys'
                                      % env, use_sudo=True)
    _sudo('chown -R %(username)s: /home/%(username)s/.'
                                    'ssh/authorized_keys' % env)
    _sudo('chmod 600 /home/%(username)s/.ssh/authorized_keys' % env)


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
                _instance = _get_inst_by_id(region, inst)
                if not env.key_filename:
                    key_filename = config.get(_instance.region.name,
                                                      'key_filename')
                    env.update({'key_filename': key_filename,
                                                  'warn_only': True})
                env.update({'host_string': _instance.public_dns_name})
                env.username = name
                _sudo('deluser %(username)s' % env)
    else:
        env.username = name
        _sudo('deluser %(username)s' % env)


def adduser(user, region=None, instance_ids=None,
                                passwordless=None, sudo=None):
    """
    creates new <user> with public SSH key on "host1;host2" list in
    <region>. If you want to create passwordless account - set any value to
    <passwrdless> variable, if you want sudo rights - set any value to <sudo>.
    File with public key must be in same directory.
    If region and instance_ids not set - script takes hosts amd key values
    from command line (-H and -i).
    Usage:
    1. WIthout aws api keys and config present:
    :<user>,<passwordless=1>,<sudo=1> - in this case you have to specify
    hosts list in -H and your user in -u  fabric parameters.
    2. With aws api keys and config entries:
    :<user>,<region>,"instance1;instance2",<passwordless>,<sudo>
    Extracts IP's from instance description.
    """
    if instance_ids and region:
        instances_ids = list(unicode(instance_ids).split(';'))
        for inst in instances_ids:
            if inst:
                _instance = _get_inst_by_id(region, inst)
                if not env.key_filename:
                    key_filename = config.get(_instance.region.name,
                                                      'key_filename')
                    env.update({'key_filename': key_filename})
                env.update({'host_string': _instance.public_dns_name})
                _create_account(user, region, instance_ids, passwordless,
                                                                    sudo)
    else:
        _create_account(user, region, instance_ids, passwordless, sudo)

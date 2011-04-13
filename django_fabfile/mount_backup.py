'''
This script mounts an AWS EBS-store server backup snapshot for
inspection. A temporary server is spun-up, and the backup is attached to
that temporary server. The script can be run anywhere there exists a
Python environment and an appropriate version of boto library.

Default AWS EC2 security group should permit SSH access for attached
volume to be mounted.

Use configuration file ~/.boto for storing your credentials as described
at http://code.google.com/p/boto/wiki/BotoConfig#Credentials

All other options will be saved in ./fabfile.cfg file.
'''

from ConfigParser import ConfigParser as _ConfigParser
from pprint import PrettyPrinter as _PrettyPrinter
from pydoc import pager as _pager
from time import sleep as _sleep
from traceback import print_exc as _print_exc
from warnings import warn as _warn

from boto.ec2 import (connect_to_region as _connect_to_region,
                      regions as _regions)
from boto.exception import EC2ResponseError as _EC2ResponseError
from fabric.api import env, prompt, sudo


config_file = 'fabfile.cfg'

def _config_get_or_set(filename, option, section='DEFAULT', default_value=None,
                       info=None):

    """Open config `filename` and try to get `option` value.

    If no `default_value` provided, prompt user with `info`."""

    config = _ConfigParser()
    config.read(filename)
    if not config.has_option(section, option):
        if (not section in config.sections() and
            not section.lower() == 'default'):
            config.add_section(section)
        if default_value is not None:
            config.set(section, option, default_value)
        else:
            value = prompt(info or 'Please enter {0} for {1}'.format(option,
                                                                     section))
            config.set(section, option, value)
        with open(filename, 'w') as f_p:
            config.write(f_p)
    return config.get(section, option)

username = _config_get_or_set(config_file, 'username', default_value='ubuntu')
device = _config_get_or_set(config_file, 'device', default_value='/dev/sdm')
mountpoint = _config_get_or_set(config_file, 'mountpoint',
                                default_value='/media/snapshot')


def create_instance(region='us-east-1'):

    """Create AWS EC2 instance.

    Return created instance."""

    info = ('Please enter keypair name in the {0} region for person who will '
            'access the instance').format(region)
    key_pair = _config_get_or_set(config_file, 'key_pair', region, info=info)

    config = _ConfigParser()
    config.read(config_file)
    ami = config.get(region, 'ami') if config.has_option(region, 'ami') else ''
    image = None
    while not image:    # `ami` is rotated according to new Ubuntu releases.
        try:
            image = _connect_to_region(region).get_image(ami)
        except _EC2ResponseError:
            info = (
                'Please enter fresh ami ID listed under '
                'http://uec-images.ubuntu.com/releases/ for ' +
                region + ' and t1.micro instance type')
            ami = prompt(info)
            config.set(region, 'ami', ami)
            with open(config_file, 'w') as f_p:
                config.write(f_p)

    reservation = image.run(key_name=key_pair, instance_type='t1.micro',
                            placement=image.connection.get_all_zones()[0].name)
    print ('{res.instances[0]} created in {zone}.'.format(
        res=reservation, zone=image.connection.get_all_zones()[0]))

    assert len(reservation.instances) == 1, 'More than 1 instances created'

    return reservation.instances[0]


def _prompt_to_select(choices, query='Select from', paging=False):

    """Prompt to select an option from provided choices.

    choices: list or dict. If dict, then chioce will be made among keys.
    paging: render long list with pagination.

    Return solely possible value instantly without prompting."""

    keys = list(choices)
    while keys.count(None):
        keys.pop(choices.index(None))    # Remove empty values.
    assert len(keys), 'No choices provided'

    if len(keys) == 1: return keys[0]

    picked = None
    while not picked in keys:
        if paging:
            pp = _PrettyPrinter()
            _pager(query + '\n' + pp.pformat(choices))
            text = 'Enter your choice or press Return to view options again'
        else:
            text = '{query} {choices}'.format(query=query, choices=choices)
        picked = prompt(text)
    return picked


def _get_all_instances(region=None, id_only=False):
    if not region:
        _warn('There is no guarantee of instance id uniqueness across regions')
    reg_names = [region] if region else (reg.name for reg in _regions())
    connections = (_connect_to_region(reg) for reg in reg_names)
    for con in connections:
        for res in con.get_all_instances():
            for inst in res.instances:
                yield inst.id if id_only else inst

def _get_all_snapshots(region=None, id_only=False):
    if not region:
        _warn('There is no guarantee of snapshot id uniqueness across regions')
    reg_names = [region] if region else (reg.name for reg in _regions())
    connections = (_connect_to_region(reg) for reg in reg_names)
    for con in connections:
        for snap in con.get_all_snapshots(owner='self'):
            yield snap.id if id_only else snap


def _select_snapshot():

    region_name = _prompt_to_select([reg.name for reg in _regions()],
                                    'Select region from')

    snap_id = prompt('Please enter snapshot ID if it\'s known (press Return '
                     'otherwise)')
    if snap_id:
        if snap_id in _get_all_snapshots(region_name, id_only=True):
            return region_name, snap_id
        else:
            print 'No snapshot with provided ID found'

    instances_list = list(_get_all_instances(region_name))
    instances = dict((inst.id, {'Name': inst.tags.get('Name'),
                                'State': inst.state,
                                'Launched': inst.launch_time,
                                'Key pair': inst.key_name,
                                'Type': inst.instance_type,
                                'IP Address': inst.ip_address,
                                'DNS Name': inst.public_dns_name}
                     ) for inst in instances_list)
    instance_id = _prompt_to_select(instances, 'Select instance ID from',
                                    paging=True)

    all_instances = _get_all_instances(region_name)
    inst = [inst for inst in all_instances if inst.id == instance_id][0]
    volumes = dict((dev.volume_id, {'Status': dev.status,
                                    'Attached': dev.attach_time,
                                    'Size': dev.size,
                                    'Snapshot ID': dev.snapshot_id}
                   ) for dev in inst.block_device_mapping.values())
    volume_id = _prompt_to_select(volumes, 'Select volume ID from', paging=True)

    all_snaps = _get_all_snapshots(region_name)
    snaps_list = (snap for snap in all_snaps if snap.volume_id == volume_id)
    snaps = dict((snap.id, {'Volume': snap.volume_id,
                            'Date': snap.start_time,
                            'Description': snap.description}
                 ) for snap in snaps_list)
    return region_name, _prompt_to_select(snaps, 'Select snapshot ID from',
                                          paging=True)


def mount_snapshot(region=None, snap_id=None):

    """Mount snapshot to temporary created instance."""

    if not region or not snap_id:
        region, snap_id = _select_snapshot()
    conn = _connect_to_region(region)
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id,])[0]

    try:
        volume = conn.create_volume(snapshot=snap.id, size=snap.volume_size,
                                    zone=conn.get_all_zones()[0].name)
        print 'New {vol} created from {snap} in {zone}.'.format(
            vol=volume, snap=snap, zone=conn.get_all_zones()[0])

        try:
            inst = create_instance(region)

            print 'Waiting for the {inst} to be running...'.format(inst=inst)
            while inst.state != 'running':
                print 'still {inst.state}...'.format(inst=inst)
                _sleep(7)
                inst.update()
            print 'done.'

            attach = volume.attach(inst.id, device)
            volume.update()
            print 'Waiting for the {vol} to be attached...'.format(vol=volume)
            while volume.attach_data.status != 'attached':
                print 'still {vol.attach_data.status}...'.format(vol=volume)
                _sleep(4)
                volume.update()
            print 'done. Volume is attached to {inst} as {dev}.'.format(
                inst=inst, dev=device)

            info = ('Please enter private key location of your keypair '
                    'in {region} region').format(region=region)
            key_file = _config_get_or_set(config_file, 'key_filename', region,
                                          info=info)
            env.update({
                'host_string': inst.public_dns_name,
                'key_filename': key_file,
                'load_known_hosts': False,
                'user': username,
            })
            while True:
                try:
                    sudo('mkdir {mnt}'.format(mnt=mountpoint))
                    break
                except:
                    print 'sshd still launching, will try again in a moment...'
                    _sleep(5)
            sudo('mount {dev} {mnt}'.format(dev=device, mnt=mountpoint))

            info = ('\nYou may now SSH into the {inst} server, using: \n'
                    'ssh -i {key} {user}@{inst.public_dns_name} \n'
                    'and browse mounted at {mountpoint} backup volume {device}.')
            print info.format(inst=inst, device=device, key=key_file,
                              user=username, mountpoint=mountpoint)

            info = ('\nEnter FINISHED if you are finished looking at the '
                    'backup and would like to cleanup: ')
            while raw_input(info).strip() != 'FINISHED':
                pass

        except:
            _print_exc()
        # Cleanup processing: terminate temporary server.
        finally:
            print 'Deleting the {0}...'.format(inst)
            inst.terminate()
            print 'done.'

    except:
        _print_exc()
    # Cleanup processing: delete detached backup volume.
    finally:
        print 'Waiting for the {vol} to be available...'.format(vol=volume)
        while volume.status != 'available':
            print 'still {vol.status}...'.format(vol=volume)
            _sleep(8)   # Wait for the volume to fully detach.
            volume.update()
        print 'done.'

        print 'Deleting the backup {vol}...'.format(vol=volume)
        delete = volume.delete()
        print 'done.'

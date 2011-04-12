'''
This script mounts an AWS EBS-store server backup snapshot for inspection,
and currently covers the following two cases:

1. backups in us-east region are mounted on the existing "infrastructure" server,
   and in us-east this script should be run on the infrastructure server
2. for backups in us-west region, a temporary server is spun-up, and the backup
   is attached to that temporary server. For us-west, the script can be run anywhere
   there exists a python environment and an appropriate version of boto.

Use configuration file ~/.boto for storing your credentials as described
at http://code.google.com/p/boto/wiki/BotoConfig#Credentials
'''

from ConfigParser import ConfigParser as _ConfigParser
from pprint import PrettyPrinter as _PrettyPrinter
from pydoc import pager as _pager
from time import sleep as _sleep
from traceback import print_exc
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
        if not section in config.sections(): config.add_section(section)
        if default_value is not None:
            config.set(section, option, default_value)
        else:
            value = prompt(info or 'Please enter {0} for {1}'.format(option,
                                                                     section))
            config.set(section, option, value)
        with open(filename, 'w') as f_p:
            config.write(f_p)
    return config.get(section, option)

# Where the backup volume should be attached.
username = _config_get_or_set(config_file, 'username', default_value='ubuntu')
device = _config_get_or_set(config_file, 'device', default_value='/dev/sdm')
mountpoint = _config_get_or_set(config_file, 'mountpoint',
                               default_value='/media/snapshot')


def create_temp_instance(region='us-east-1'):

    """Create temporary server.

    Return reservation with created instance."""

    info = 'Please enter your login key in the {0} region'.format(region)
    key_pair = _config_get_or_set(config_file, 'key_pair', region, info=info)
    _config_get_or_set(config_file, 'key_filename', region,
                       info='and private key location')

    config = _ConfigParser()
    config.read(config_file)
    ami = config.get(region, 'ami') if config.has_option(region, 'ami') else ''
    image = None
    while not image:
        try:
            image = _connect_to_region(region).get_image(ami)
        except _EC2ResponseError:
            info = (
                'Please enter fresh ami ID listed at '
                'http://uec-images.ubuntu.com/maverick/current/ for ' +
                region + ' and t1.micro type')
            ami = prompt(info)
            config.set(region, 'ami', ami)
            with open(config_file, 'w') as f_p:
                config.write(f_p)

    reservation = image.run(key_name=key_pair, instance_type='t1.micro',
                            placement=image.connection.get_all_zones()[0].name)
    print ('{res.instances[0].id} instance created in {res.id} reservation at '
           '{region} region'.format(res=reservation, region=region))

    assert len(reservation.instances) == 1, 'More than 1 instances created'

    return reservation.instances[0]


def _prompt_to_select(choices, query='Select from', paging=False):

    """prompt to select an option from provided choices.

    choices: list or dict. If dict, then keys will be selected.
    paging: render long list with pagination.

    return solely possible value instantly."""

    keys = list(choices)
    while keys.count(None):
        keys.pop(choices.index(None))    # Remove empty values.
    assert len(keys), 'No choices provided'

    if len(keys) == 1: return keys[0]

    picked = None
    while not picked in keys:
        if paging:
            pp = _PrettyPrinter()
            _pager(pp.pformat(choices))
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

    def pick_out_snapshot(region_name, volume_id=None):
        snaps = _get_all_snapshots(region_name)
        if volume_id:
            snaps = (snap for snap in snaps if snap.volume_id == volume_id)
#           print "\nSnapshot ID\tVolume ID\tDate\t\t\t\tDescription\n"
#                   (item.id, item.volume_id, item.start_time, item.description)
        snaps = dict((snap.id, {'Volume': snap.volume_id,
                                'Date': snap.start_time,
                                'Description': snap.description}
                     ) for snap in snaps)
        return region_name, _prompt_to_select(snaps, 'Select snapshot from',
                                              paging=True)

    snap_id = prompt('Please enter snapshot ID if it\'s known (press Return '
                     'otherwise)')
    if snap_id:
        if snap_id in _get_all_snapshots(region_name, id_only=True):
            return region_name, snap_id
        else:
            print 'No snapshot with provided ID found'
    instances = list(_get_all_instances(region_name, id_only=True))
    instance_id = _prompt_to_select(instances, 'Select instance from')

    all_instances = _get_all_instances(region_name)
    inst = [inst for inst in all_instances if inst.id == instance_id][0]
    volumes = [dev.volume_id for dev in inst.block_device_mapping.values()]
    volume_id = _prompt_to_select(volumes, 'Select volume from')
    return pick_out_snapshot(region_name, volume_id)


def mount_snapshot(region=None, snap_id=None):

    """Mount snapshot to temporary created instance."""

    if not region or not snap_id:
        region, snap_id = _select_snapshot()
    conn = _connect_to_region(region)
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id,])[0]

#   print "volume ID\tInstance ID\tState\tInstance Type\tKey Name\tLocation\tIP Address\n"

#       (volumeid,  item.id, item.state,\
#        item.instance_type, item.key_name, item.placement, item.ip_address)

#                   (chosenSnapshot.id, chosenSnapshot.volume_id, \
#                    chosenSnapshot.start_time, chosenSnapshot.description)

#               (chosenInstance.block_device_mapping['/dev/sda1'].volume_id, chosenInstance.id, \
#                chosenInstance.state, chosenInstance.instance_type, \
#                chosenInstance.key_name, chosenInstance.ip_address)

#           print "\nSnapshot ID\tVolume ID\tDate\t\t\t\tDescription\n"
#                  defaultSnapshot = item # AWS by default lists from oldest to newest, so the last one will be newest
#                   (item.id, item.volume_id, item.start_time, item.description)

#   		print '\nYou have chosen:'
#   			(chosenSnapshot.id, chosenSnapshot.volume_id, \
#   			 chosenSnapshot.start_time, chosenSnapshot.description)

    try:
        volume = conn.create_volume(snapshot=snap.id, size=snap.volume_size,
                                    zone=conn.get_all_zones()[0].name)
        print 'New volume ID is: ' + volume.id

        try:
            inst = create_temp_instance(region)

            while inst.state != 'running':
                print 'Waiting: {inst} server is still {inst.state}'.format(
                    inst=inst)
                _sleep(4)
                inst.update()

            attach = volume.attach(inst.id, device)
            volume.update()
            while volume.attach_data.status != 'attached':
                print 'Waiting: volume status is still ' + volume.attach_data.status
                _sleep(4)
                volume.update()
            print 'Volume is attached to ' + inst.id + ' on ' + device

            key_file = _config_get_or_set(config_file, 'key_filename', region)
            env.update({
                'host_string': inst.public_dns_name,
                'key_filename': key_file,
                'user': username,
            })
            sudo('mkdir {mnt}'.format(mnt=mountpoint))
            sudo('mount {dev} {mnt}'.format(dev=device, mnt=mountpoint))

            info = ('\nYou may now SSH into the {inst} server, using: \n'
                    'ssh -i {key} {user}@{inst.public_dns_name} \n'
                    'and browse mounted at {mountpoint} backup volume {device}')
            print info.format(inst=inst, device=device, key=key_file,
                              user=username, mountpoint=mountpoint)

            info = ('\nEnter FINISHED if you are finished looking at the '
                    'backup and would like to cleanup: ')
            while raw_input(info).strip() != 'FINISHED':
                pass

#           sudo('umount {mnt}'.format(mnt=mountpoint))

        except Exception, err:
            print_exc()
        # Cleanup processing: terminate temporary server.
        finally:
            print 'Deleting the {0}...'.format(inst)
            inst.terminate()

    except Exception, err:
        print_exc()
    # Cleanup processing: detach and delete backup volume.
    finally:
        print 'Detaching the volume from {0} server....'.format(inst)
        detach = volume.detach()

        waitStep = 5
        totalWait = 0
        while volume.status != 'available':
            _sleep(waitStep) # wait for the volume to fully detach
            totalWait = totalWait + waitStep
            volume.update()
            print 'Waiting: volume status is still ' + volume.status
            if totalWait > 60: # something wrong if one minute is not enough
                print 'Failed to detach volume ' + volume.id
                break

        print 'Deleting the backup volume...'
        delete = volume.delete()

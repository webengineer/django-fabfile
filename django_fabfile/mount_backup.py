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

from pprint import PrettyPrinter as _PrettyPrinter
from pydoc import pager as _pager
from re import compile as _compile
from time import sleep as _sleep
from warnings import warn as _warn

from boto.ec2 import (connect_to_region as _connect_to_region,
                      regions as _regions)
from boto.exception import BotoServerError as _BotoServerError
from fabric.api import env, prompt, sudo

from utils import _config_get_or_set


config_file = 'fabfile.cfg'

username = _config_get_or_set(config_file, 'username', default_value='ubuntu')
device = _config_get_or_set(config_file, 'device', default_value='/dev/sdm')
mountpoint = _config_get_or_set(config_file, 'mountpoint',
                                default_value='/media/snapshot')
# Official Ubuntu AMIs are published in EC2 by the 'Canonical' user, see
# https://help.ubuntu.com/community/EC2StartersGuide#Getting%20the%20images
ubuntu_aws_account = _config_get_or_set(config_file, 'ubuntu_aws_account',
                                        default_value='099720109477')
architecture = _config_get_or_set(config_file, 'architecture',
                                  default_value='x86_64')
root_device_type = _config_get_or_set(config_file, 'root_device_type',
                                      default_value='ebs')
ami_ptrn = _config_get_or_set(
    config_file, 'ami_ptrn',
    default_value='ebs/ubuntu-images/ubuntu-*-server-*')
ami_ptrn_with_version = _config_get_or_set(
    config_file, 'ami_ptrn_with_version',
    default_value='ebs/ubuntu-images/ubuntu-*-{version}-*-server-*')
ami_ptrn_with_relase_date = _config_get_or_set(
    config_file, 'ami_ptrn_with_relase_date',
    default_value='ebs/ubuntu-images/ubuntu-*-{version}-*-server-'
                  '{released_at}')
ami_regexp = _config_get_or_set(
    config_file, 'ami_regexp',
    default_value=(
        r'^ebs/ubuntu-images/ubuntu-[a-z]+-(?P<version>\d{1,2}\.\d{2,2})-'
         '[a-z3264]+-server-(?P<released_at>\d{8,8}(\.\d{1,1})?)$')
)


def _wait_for(obj, attrs, state, update_attr='update', max_sleep=30):
    """Wait for attribute to go into state.

    attrs
        list of nested attribute names;
    update_attr
        will be called to refresh state."""
    print 'Waiting for the {0} to be {1}...'.format(obj, state)
    def get_nested_attr(obj, attrs):
        attr = obj
        for attr_name in attrs:
            attr = getattr(attr, attr_name)
        return attr
    sleep_for = 3
    while get_nested_attr(obj, attrs) != state:
        print 'still {0}...'.format(get_nested_attr(obj, attrs))
        sleep_for += 2
        _sleep(min(sleep_for, max_sleep))
        getattr(obj, update_attr)()
    print 'done.'


def create_instance(region_name='us-east-1', zone_name=None):

    """Create AWS EC2 instance.

    Return created instance.

    zone
        string-formatted name. By default will be used latest zone."""

    info = ('Please enter keypair name in the {0} region for person who will '
            'access the instance').format(region_name)
    key_pair = _config_get_or_set(config_file, 'key_pair', region_name,
                                  info=info)

    conn = _connect_to_region(region_name)

    filters={'owner_id': ubuntu_aws_account, 'architecture': architecture,
             'name': ami_ptrn, 'image_type': 'machine',
             'root_device_type': root_device_type}
    images = conn.get_all_images(filters=filters)

    # Filtering by latest version.
    ptrn = _compile(ami_regexp)
    versions = set([ptrn.search(img.name).group('version') for img in images])
    def complement(year_month):
        return '0' + year_month if len(year_month) == 4 else year_month
    latest_version = sorted(set(filter(complement, versions)))[-1]  # XXX Y3K.
    name_with_version = ami_ptrn_with_version.format(version=latest_version)
    filters.update({'name': name_with_version})
    images = conn.get_all_images(filters=filters)
    # Filtering by latest release date.
    dates = set([ptrn.search(img.name).group('released_at') for img in images])
    latest_date = sorted(set(dates))[-1]
    name_with_version_and_release = ami_ptrn_with_relase_date.format(
        version=latest_version, released_at=latest_date)
    filters.update({'name': name_with_version_and_release})
    image = conn.get_all_images(filters=filters)[0]
    zone = zone_name or conn.get_all_zones()[-1].name
    print 'Launching new instance in {zone} from {image}'.format(image=image,
                                                                 zone=zone)
    reservation = image.run(key_name=key_pair, instance_type='t1.micro',
                            placement=zone)
    print '{res.instances[0]} created in {zone}.'.format(res=reservation,
                                                         zone=zone)

    assert len(reservation.instances) == 1, 'More than 1 instances created'

    return reservation.instances[0]


def _prompt_to_select(choices, query='Select from', paging=False):

    """Prompt to select an option from provided choices.

    choices: list or dict. If dict, then choice will be made among keys.
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

    def _mount_snapshot_in_zone(snap_id, zone):
        volume = inst = None
        try:
            volume = conn.create_volume(
                snapshot=snap.id, size=snap.volume_size, zone=zone)
            print 'New {vol} created from {snap} in {zone}.'.format(
                vol=volume, snap=snap, zone=zone)

            try:
                inst = create_instance(zone.region.name, zone.name)
                _wait_for(inst, ['state',], 'running')

                attach = volume.attach(inst.id, device)
                volume.update()
                _wait_for(volume, ['attach_data', 'status'], 'attached')
                print 'Volume is attached to {inst} as {dev}.'.format(
                    inst=inst, dev=device)

                info = ('Please enter private key location of your keypair '
                        'in {region}').format(region=zone.region)
                key_file = _config_get_or_set(config_file, 'key_filename',
                                              zone.region.name, info=info)
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
                        print 'sshd still launching, waiting to try again...'
                        _sleep(5)
                info = ('\nYou may now SSH into the {inst} server, using:'
                        '\n ssh -i {key} {user}@{inst.public_dns_name}')
                try:
                    sudo('mount {dev} {mnt}'.format(dev=device, mnt=mountpoint))
                except:
                    info += ('\nand mount {device}. NOTE: device name may be '
                             'modified by system.')
                else:
                    info += ('\nand browse mounted at {mountpoint} backup '
                             'volume {device}.')
                print info.format(inst=inst, device=device, key=key_file,
                                  user=username, mountpoint=mountpoint)

                info = ('\nEnter FINISHED if you are finished looking at the '
                        'backup and would like to cleanup: ')
                while raw_input(info).strip() != 'FINISHED':
                    pass

            # Cleanup processing: terminate temporary server.
            finally:
                if inst:
                    print 'Deleting the {0}...'.format(inst)
                    inst.terminate()
                    print 'done.'

        # Cleanup processing: delete detached backup volume.
        finally:
            if volume:
                _wait_for(volume, ['status',], 'available')

                print 'Deleting the backup {vol}...'.format(vol=volume)
                delete = volume.delete()
                print 'done.'

    for zone in conn.get_all_zones():
        try:
            _mount_snapshot_in_zone(snap_id, zone)
        except _BotoServerError, err:
            print '{0} in {1}'.format(err, zone)
            continue
        else:
            break

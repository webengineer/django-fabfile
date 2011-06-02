# -*- coding: utf-8 -*-
'''
Use configuration file ~/.boto for storing your credentials as described
at http://code.google.com/p/boto/wiki/BotoConfig#Credentials

All other options will be taken from ./fabfile.cfg file.
'''

from datetime import timedelta as _timedelta, datetime
from ConfigParser import ConfigParser as _ConfigParser
from contextlib import contextmanager as _contextmanager
from itertools import groupby as _groupby
from pprint import PrettyPrinter as _PrettyPrinter
from pydoc import pager as _pager
from re import compile as _compile, match as _match
from string import lowercase
from time import sleep as _sleep
from warnings import warn as _warn

from boto.ec2 import (connect_to_region as _connect_to_region,
                      regions as _regions)
from boto.exception import BotoServerError as _BotoServerError
import os
import sys
from fabric.api import env, prompt, sudo


config_file = 'fabfile.cfg'
config = _ConfigParser()
config.read(config_file)
for region in _regions():
    if not config.has_section(region.name):
        config.add_section(region.name)


hourly_backups = config.getint('purge_backups', 'hourly_backups')
daily_backups = config.getint('purge_backups', 'daily_backups')
weekly_backups = config.getint('purge_backups', 'weekly_backups')
monthly_backups = config.getint('purge_backups', 'monthly_backups')
quarterly_backups = config.getint('purge_backups', 'quarterly_backups')
yearly_backups = config.getint('purge_backups', 'yearly_backups')


username = config.get('mount_backups', 'username')
device = config.get('mount_backups', 'device')
mountpoint = config.get('mount_backups', 'mountpoint')
ubuntu_aws_account = config.get('mount_backups', 'ubuntu_aws_account')
architecture = config.get('mount_backups', 'architecture')
root_device_type = config.get('mount_backups', 'root_device_type')
ami_ptrn = config.get('mount_backups', 'ami_ptrn')
ami_ptrn_with_version = config.get('mount_backups',
                                          'ami_ptrn_with_version')
ami_ptrn_with_relase_date = config.get('mount_backups',
                                      'ami_ptrn_with_relase_date')
ami_regexp = config.get('mount_backups', 'ami_regexp')


def _prompt_to_select(choices, query='Select from', paging=False):
    """Prompt to select an option from provided choices.

    choices: list or dict. If dict, then choice will be made among keys.
    paging: render long list with pagination.

    Return solely possible value instantly without prompting."""
    keys = list(choices)
    while keys.count(None):
        keys.pop(choices.index(None))    # Remove empty values.
    assert len(keys), 'No choices provided'

    if len(keys) == 1:
        return keys[0]

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


def _get_region_by_name(region_name):
    """Allow to specify region name fuzzyly."""
    matched = [reg for reg in _regions() if _match(region_name, reg.name)]
    assert len(matched) > 0, 'No region matches {0}'.format(region_name)
    assert len(matched) == 1, 'Several regions matches {0}'.format(region_name)
    return matched[0]


def _get_instance_by_id(region, instance_id):
    conn = _connect_to_region(region)
    res = conn.get_all_instances([instance_id, ])
    assert len(res) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(res,
                                                      instance_id))
    instances = res[0].instances
    assert len(instances) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(instances,
                                                              instance_id))
    return instances[0]


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
    instances = dict((inst.id, {
        'Name': inst.tags.get('Name'),
        'State': inst.state,
        'Launched': inst.launch_time,
        'Key pair': inst.key_name,
        'Type': inst.instance_type,
        'IP Address': inst.ip_address,
        'DNS Name': inst.public_dns_name}) for inst in instances_list)
    instance_id = _prompt_to_select(instances, 'Select instance ID from',
                                    paging=True)

    all_instances = _get_all_instances(region_name)
    inst = [inst for inst in all_instances if inst.id == instance_id][0]
    volumes = dict((dev.volume_id, {
        'Status': dev.status,
        'Attached': dev.attach_time,
        'Size': dev.size,
        'Snapshot ID': dev.snapshot_id}) for dev in
                                            inst.block_device_mapping.values())
    volume_id = _prompt_to_select(volumes, 'Select volume ID from',
                                  paging=True)

    all_snaps = _get_all_snapshots(region_name)
    snaps_list = (snap for snap in all_snaps if snap.volume_id == volume_id)
    snaps = dict((snap.id, {'Volume': snap.volume_id,
                            'Date': snap.start_time,
                            'Description': snap.description}) for snap in
                                                                    snaps_list)
    return region_name, _prompt_to_select(snaps, 'Select snapshot ID from',
                                          paging=True)


def create_snapshot(region, instance_id=None, instance=None, dev='/dev/sda1'):
    """Return newly created snapshot of specified instance device.

    region
        name of region where instance is located;
    instance, instance_id
        either `instance_id` or `instance` argument should be specified;
    dev
        by default /dev/sda1 will be snapshotted."""
    assert bool(instance_id) ^ bool(instance), ('Either instance_id or '
        'instance should be specified')
    if instance_id:
        instance = _get_instance_by_id(region, instance_id)
    vol_id = instance.block_device_mapping[dev].volume_id
    description = 'Created from {0} of {1}'.format(dev, instance.id)
    if instance.tags.get('Name'):
        description += ' named as {0}'.format(instance.tags['Name'])
    conn = _connect_to_region(region)
    snapshot = conn.create_snapshot(vol_id, description)
    for tag in instance.tags:   # Clone intance tags to the snapshot.
        snapshot.add_tag(tag, instance.tags[tag])
    print 'Waiting for the {snap} for {inst} to be completed...'.format(
                                        snap=snapshot, inst=instance)
    while snapshot.status != 'completed':
        print 'still {snap.status}...'.format(snap=snapshot)
        _sleep(5)
        snapshot.update()
    print 'done.'
    return snapshot


def backup_instance(region, instance_id=None, instance=None):
    """Return list of created snapshots for specified instance.

    instance, instance_id
        either `instance_id` or `instance` argument should be specified."""
    assert bool(instance_id) ^ bool(instance), ('Either instance_id or '
        'instance should be specified')
    if instance_id:
        instance = _get_instance_by_id(region, instance_id)
    snapshots = []  # NOTE Fabric doesn't supports generators.
    for dev in instance.block_device_mapping:
        snapshots.append(create_snapshot(region, instance=instance, dev=dev))
    return snapshots


def backup_instances_by_tag(region=None, tag_name=None, tag_value=None):
    """Creates backup for all instances with given tag in region.

    region
        will be applied across all regions by default;
    tag_name, tag_value
        will be fetched from config by default, may be configured
        per region."""
    snapshots = []
    reg_names = [region] if region else (reg.name for reg in _regions())
    for reg in reg_names:
        tag_name = tag_name or config.get(reg, 'tag_name')
        tag_value = tag_value or config.get(reg, 'tag_value')
        conn = _connect_to_region(reg)
        filters = {'resource-type': 'instance', 'key': tag_name,
                   'tag-value': tag_value}
        for tag in conn.get_all_tags(filters=filters):
            snapshots += backup_instance(reg, instance_id=tag.res_id)
    return snapshots


def _trim_snapshots(
    region, hourly_backups=hourly_backups, daily_backups=daily_backups,
    weekly_backups=weekly_backups, monthly_backups=monthly_backups,
    quarterly_backups=quarterly_backups, yearly_backups=yearly_backups,
    dry_run=False):

    """Delete snapshots back in time in logarithmic manner.

    dry_run
        just print snapshot to be deleted."""

    conn = _connect_to_region(region)
    # work with UTC time, which is what the snapshot start time is reported in
    now = datetime.utcnow()
    last_hour = datetime(now.year, now.month, now.day, now.hour)
    last_midnight = datetime(now.year, now.month, now.day)
    last_sunday = datetime(now.year, now.month,
          now.day) - _timedelta(days = (now.weekday() + 1) % 7)
    last_month = datetime(now.year, now.month -1, now.day)
    last_year = datetime(now.year-1, now.month, now.day)
    other_years = datetime(now.year-2, now.month, now.day)
    start_of_month = datetime(now.year, now.month, 1)

    target_backup_times = []
    # there are no snapshots older than 1/1/2000
    oldest_snapshot_date = datetime(2000, 1, 1)

    for hour in range(0, hourly_backups):
        target_backup_times.append(last_hour - _timedelta(hours = hour))

    for day in range(0, daily_backups):
        target_backup_times.append(last_midnight - _timedelta(days = day))

    for week in range(0, weekly_backups):
        target_backup_times.append(last_sunday - _timedelta(weeks = week))

    for month in range(0, monthly_backups):
        target_backup_times.append(last_month- _timedelta(weeks= month*4))

    for quart in range(0, quarterly_backups):
        target_backup_times.append(last_year- _timedelta(weeks= quart*16))

    for year in range(0, yearly_backups):
        target_backup_times.append(other_years- _timedelta(days = year*365))


    one_day = _timedelta(days = 1)
    while start_of_month > oldest_snapshot_date:
        # append the start of the month to the list of snapshot dates to save:
        target_backup_times.append(start_of_month)
        # there's no timedelta setting for one month, so instead:
        # decrement the day by one,
        #so we go to the final day of the previous month...
        start_of_month -= one_day
        # ... and then go to the first day of that previous month:
        start_of_month = datetime(start_of_month.year,
                               start_of_month.month, 1)

    temp = []

    for t in target_backup_times:
        if temp.__contains__(t) == False:
            temp.append(t)

    target_backup_times = temp
    target_backup_times.reverse() # make the oldest date first

    conn = _connect_to_region(region)

    # get all the snapshots, sort them by date and time,
    #and organize them into one array for each volume:
    all_snapshots = conn.get_all_snapshots(owner = 'self')
    # oldest first
    all_snapshots.sort(cmp = lambda x, y: cmp(x.start_time, y.start_time))

    snaps_for_each_volume = {}
    for snap in all_snapshots:
        # the snapshot name and the volume name are the same.
        # The snapshot name is set from the volume
        # name at the time the snapshot is taken
        volume_name = snap.volume_id

        if volume_name:
            # only examine snapshots that have a volume name
            snaps_for_volume = snaps_for_each_volume.get(volume_name)

            if not snaps_for_volume:
                snaps_for_volume = []
                snaps_for_each_volume[volume_name] = snaps_for_volume
            snaps_for_volume.append(snap)


    # Do a running comparison of snapshot dates to desired time periods,
    # keeping the oldest snapshot in each
    # time period and deleting the rest:
    for volume_name in snaps_for_each_volume:
        snaps = snaps_for_each_volume[volume_name]
        snaps = snaps[:-1]
        # never delete the newest snapshot, so remove it from consideration

        time_period_number = 0
        snap_found_for_this_time_period = False
        for snap in snaps:
            check_this_snap = True

            while (check_this_snap and
                  time_period_number < target_backup_times.__len__()):
                snap_date = datetime.strptime(snap.start_time,
                                      '%Y-%m-%dT%H:%M:%S.000Z')

                if snap_date < target_backup_times[time_period_number]:
                    # the snap date is before the cutoff date.
                    # Figure out if it's the first snap in this
                    # date range and act accordingly
                    #(since both date the date ranges and the snapshots
                    # are sorted chronologically, we know this
                    #snapshot isn't in an earlier date range):
                    if snap_found_for_this_time_period:
                        if not snap.tags.get('preserve_snapshot'):
                            if dry_run:
                                print('Dry-trimmed %s %s from %s' % (snap,
                                    snap.description, snap.start_time))
                            else:
                                # as long as the snapshot wasn't marked with
                                # the 'preserve_snapshot' tag, delete it:
                                conn.delete_snapshot(snap.id)
                                print('Trimmed snapshot %s %s from %s' % (snap,
                                    snap.description, snap.start_time))
                        # go on and look at the next snapshot,
                        # leaving the time period alone
                    else:
                        # this was the first snapshot found for this time
                        # period. Leave it alone and look at the next snapshot:
                        snap_found_for_this_time_period = True
                    check_this_snap = False
                else:
                    # the snap is after the cutoff date.
                    # Check it against the next cutoff date
                    time_period_number += 1
                    snap_found_for_this_time_period = False


def trim_snapshots(region=None, dry_run=False):
    reg_names = [region] if region else (reg.name for reg in _regions())
    for reg in reg_names:
        print reg
        regions_trim = _trim_snapshots(region=reg, dry_run=dry_run)


def create_instance(region_name='us-east-1', zone_name=None, key_pair=None,
                    security_group='default'):
    """Create AWS EC2 instance.

    Return created instance.

    region_name
        by default will be created in the us-east-1 region;
    zone
        string-formatted name. By default will be used latest zone;
    key_pair
        name of key_pair to be granted access. Will be fetched from
        config by default, may be configured per region."""

    # TODO Allow only zone_name to be passed.

    info = ('Please enter keypair name in the {0} region for person who will '
            'access the instance').format(region_name)

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

    key_pair = key_pair or config.get(region_name, 'key_pair')
    reservation = image.run(key_name=key_pair, instance_type='t1.micro',
                            placement=zone, security_groups=['default',
                            security_group])

    print '{res.instances[0]} created in {zone}.'.format(res=reservation,
                                                         zone=zone)

    assert len(reservation.instances) == 1, 'More than 1 instances created'

    return reservation.instances[0]


@_contextmanager
def _create_temporary_instance(zone):
    inst = create_instance(zone.region.name, zone.name)
    _wait_for(inst, ['state', ], 'running')
    try:
        yield inst
    finally:
        print 'Terminating the {0}...'.format(inst)
        inst.terminate()


def _get_avail_dev(instance):
    """Return next available device."""
    chars = lowercase
    for dev in instance.block_device_mapping:
        chars = chars.replace(dev[-2], '')
    return '/dev/sd{0}1'.format(chars[0])


def _get_natty_dev(dev):
    return dev.replace('sd', 'xvd')


@_contextmanager
def _attach_snapshot_in_zone(snap, zone):
    conn = zone.region.connect()
    volume = conn.create_volume(snapshot=snap.id, size=snap.volume_size,
                                zone=zone)
    try:
        with _create_temporary_instance(zone) as inst:
            dev = _get_avail_dev(inst)
            attach = volume.attach(inst.id, dev)
            volume.update()
            _wait_for(volume, ['attach_data', 'status'], 'attached')
            yield inst, dev
    finally:
        _wait_for(volume, ['status', ], 'available')
        print 'Deleting the {vol}...'.format(vol=volume)
        volume.delete()


def _mount_device(inst, device):
    """Issue commands via SSH to mount device. Return mountpoint on success.

    inst
        instance with the device;
    device
        device as it listed in keys of instance.block_device_mapping."""
    key_filename = config.get(inst.region.name, 'key_filename')
    env.update({
        'host_string': inst.public_dns_name,
        'key_filename': key_filename,
        'load_known_hosts': False,
        'user': username,
    })
    mountpoint = device.replace('/dev/', '/media/')
    while True:
        try:
            sudo('mkdir {0}'.format(mountpoint))
            break
        except:
            print 'sshd still launching, waiting for to try again...'
            _sleep(5)
    for dev in [device, _get_natty_dev(device)]:
        try:
            sudo('mount {dev} {mnt}'.format(dev=dev, mnt=mountpoint))
        except:
            pass
        else:
            return mountpoint


def mount_snapshot(region=None, snap_id=None):

    """Mount snapshot to temporary created instance."""

    if not region or not snap_id:
        region, snap_id = _select_snapshot()
    conn = _connect_to_region(region)
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id, ])[0]

    info = ('\nYou may now SSH into the {inst} server, using:'
            '\n ssh -i {key} {user}@{inst.public_dns_name}')
    for zone in conn.get_all_zones():
        try:
            with _attach_snapshot_in_zone(snap, zone) as (inst, dev):
                mountpoint = _mount_device(inst, dev)
                if mountpoint:
                    info += ('\nand browse snapshot, mounted at {mountpoint}.')
                else:
                    info += ('\nand mount {device}. NOTE: device name may be '
                             'modified by system.')
                key_filename = config.get(inst.region.name, 'key_filename')
                print info.format(inst=inst, device=dev, key=key_filename,
                                  user=username, mountpoint=mountpoint)

                info = ('\nEnter FINISHED if you are finished looking at the '
                        'backup and would like to cleanup: ')
                while raw_input(info).strip() != 'FINISHED':
                    pass
        except _BotoServerError, err:
            print '{0} in {1}'.format(err, zone)
            continue
        else:
            break


# FIXME Function up to 50 lines lenght easier to understand.
# FIXME `zone_name` argument is not used.
# FIXME `snap_id` is overwritten.

def move_snapshot(region_name=None, reserve_region=None,
                 instance_id=None, instance=None,
                 zone_name=None, snap_id=None, res_snap_id=None):
    """
    You must specify region_name and reserve_region for backup.
    If you already have backups in reserve_region - snapshot
    for specified instance will be mounted, updated and re-created
    """
    if not region_name:     # FIXME Slightly modified _select_snapshot()
                            # can be used here.
        region_name = _prompt_to_select([reg.name for reg in _regions()],
                                    'Select source region from')

    if not instance_id:
        instances_list = list(_get_all_instances(region_name))
        instances = dict((inst.id, {'Name': inst.tags.get('Name'),
                                    'State': inst.state,
                                    'Launched': inst.launch_time,
                                    'Key pair': inst.key_name,
                                    'Type': inst.instance_type,
                                    'IP Address': inst.ip_address,
                                    'DNS Name': inst.public_dns_name})
                         for inst in instances_list)
        instance_id = _prompt_to_select(instances, 'Select instance ID from',
                                        paging=True)
    all_instances = _get_all_instances(region_name)
    inst = [inst for inst in all_instances if inst.id == instance_id][0]
    volumes = dict((dev.volume_id, {'Status': dev.status,
                                    'Attached': dev.attach_time,
                                    'Size': dev.size,
                                    'Snapshot ID': dev.snapshot_id})
                   for dev in inst.block_device_mapping.values())
    volume_id = _prompt_to_select(volumes, 'Select volume ID from',
                                                          paging=True)
    if not reserve_region:
        reserve_region = _prompt_to_select([reg.name for reg in _regions()],
                                    'Select backup region from')
    all_snaps = _get_all_snapshots(region_name)
    snaps_list = (snap for snap in all_snaps if snap.volume_id == volume_id)
    snaps = dict((snap.id, {'Volume': snap.volume_id,
                            'Date': snap.start_time,
                            'Description': snap.description})
                 for snap in snaps_list)
    snap_id = _prompt_to_select(snaps, 'Select source snapshot ID from',
                                          paging=True)

    conn = _connect_to_region(region_name)
    if reserve_region:  # FIXME Always True.
        conn_reserve = _connect_to_region(reserve_region)
        try:
            conn_reserve.delete_key_pair('transfer_snapshot')
            conn_reserve.delete_security_group('transfer_snapshot')
        except:
            pass
        key_pair_reserve = conn_reserve.create_key_pair('transfer_snapshot')
        reserve_secuity_group = conn_reserve.create_security_group(
                      'transfer_snapshot', 'Group for opening ssh')
        reserve_secuity_group.authorize('tcp', '22', '22', '0.0.0.0/0')
        if os.path.exists('./transfer_snapshot.pem'):
            os.system('rm ./transfer_snapshot.pem')
        key_pair_reserve.save('./')
        reserve_zone = conn_reserve.get_all_zones()[0].name
        _reserve_zone = conn_reserve.get_all_zones()[0]
        os.system('chmod 600 ./transfer_snapshot.pem')

    snap = conn.get_all_snapshots(snapshot_ids=[snap_id, ])[0]

    filters = {'resource-type': 'snapshot', 'key': 'Source',
                   'tag-value': 'Created for {0} from {1}'.format(instance_id,
                                                        region_name)}

    res_snaps_list = (snap for snap in conn_reserve.get_all_tags(
                                                filters=filters))
    res_snaps = dict((snap.res_id, {}) for snap in res_snaps_list)
    if res_snaps:
        res_snap_id = _prompt_to_select(res_snaps,
                  'Select backup snapshot ID from',
                                      paging=True)
    res_snap = None
    if res_snap_id:
        res_snap = conn_reserve.get_all_snapshots(snapshot_ids=res_snap_id)[0]

    # FIXME Code from the above inner function
    # mount_snapshot._mount_snapshot_in_zone should be reused below.
    def _mount_snapshot_in_zone(snap_id, zone):
        volume = inst = None
        volume_reserve = inst_reserve = None
        try:
            volume = conn.create_volume(
                snapshot=snap.id, size=snap.volume_size, zone=zone)
            print 'New {vol} created from {snap} in {zone}.'.format(
                vol=volume, snap=snap, zone=zone)
            if res_snap:
                volume_reserve = conn_reserve.create_volume(
             snapshot=res_snap.id, size=res_snap.volume_size,
                                                      zone=reserve_zone)
                print 'New {vol} created from {snap} in {zone}.'.format(
                    vol=volume_reserve, snap=res_snap, zone=reserve_zone)
            else:
                volume_reserve = conn_reserve.create_volume(
                    size=snap.volume_size, zone=reserve_zone)
                print 'New {vol} created in {zone}.'.format(
                    vol=volume_reserve, zone=reserve_zone)
            try:
                key_pair = config.get(region_name, 'key_pair')
                inst = create_instance(zone.region.name, zone.name, key_pair)
                _wait_for(inst, ['state'], 'running')
                inst_reserve = create_instance(_reserve_zone.region.name,
                              _reserve_zone.name,
                              key_pair_reserve.name, 'transfer_snapshot')
                _wait_for(inst_reserve, ['state'], 'running')
                device = '/dev/sdk'
                attach = volume.attach(inst.id, device)
                volume.update()
                _wait_for(volume, ['attach_data', 'status'], 'attached')
                print 'Volume is attached to {inst} as {dev}.'.format(
                    inst=inst, dev=device)
                attach_reserve = volume_reserve.attach(
                                                  inst_reserve.id, device)
                volume_reserve.update()
                _wait_for(volume_reserve,
                                    ['attach_data', 'status'], 'attached')
                print 'Volume is attached to {inst_reserve} as {dev}.'.format(
                    inst_reserve=inst_reserve, dev=device)
                key_filename = config.get(zone.region.name, 'key_filename')
                env.update({
                    'host_string': inst.public_dns_name,
                    'key_filename': key_filename,
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
                    device = '/dev/xvdk'
                    sudo('mount {dev} {mnt}'.format(dev=device,
                                                mnt=mountpoint))
                    os.system('scp -i {key_filename} '
                    '-o StrictHostKeyChecking=no '
                    './transfer_snapshot.pem '
                    '{user}@{host}:/home/ubuntu/transfer_snapshot.pem'.format(
                    user=username, host=inst.public_dns_name,
                    key_filename=key_filename))
                except:     # FIXME Steps should be accomplished without
                            # user intervention.
                    info += ('\nand mount {device}. NOTE: device name may be '
                             'modified by system.')
                else:
                    info += ('\nand browse mounted at {mountpoint} backup '
                             'volume {device}.')
                print info.format(inst=inst, device=device, key=key_filename,
                                  user=username, mountpoint=mountpoint)

                key_filename_reserve = (os.getcwd()+'/transfer_snapshot.pem')
                env.update({
                    'host_string': inst_reserve.public_dns_name,
                    'key_filename': key_filename_reserve,
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
                info = ('\nYou may now SSH into'
                        'the {inst_reserve} server, using:'
                        '\n ssh -i {key} '
                        '{user}@{inst_reserve.public_dns_name}')
                try:
                    device = '/dev/xvdk'
                    if not res_snap:
                        sudo('mkfs.ext3 {dev}'.format(dev=device))
                    sudo('rm /root/.ssh/authorized_keys')
                    sudo('cp /home/ubuntu/.ssh/authorized_keys /root/.ssh/')
                except:
                    info += ('\nand create fs on {device}.')
                try:
                    device = '/dev/xvdk'
                    sudo('mount {dev} {mnt}'.format(dev=device,
                                                          mnt=mountpoint))
                except:
                    info += ('\nand mount {device}. NOTE: device name may be '
                             'modified by system.')
                else:
                    info += ('\nand browse mounted at {mountpoint} backup '
                             'volume {device}.')
                print info.format(inst_reserve=inst_reserve, device=device,
                key=key_filename_reserve, user=username,
                                                    mountpoint=mountpoint)
                env.update({
                    'host_string': inst.public_dns_name,
                    'key_filename': key_filename,
                    'load_known_hosts': False,
                    'user': username,
                })
                # SYNCHRONISATION!!!
                sudo('rsync -e "ssh -i /home/ubuntu/transfer_snapshot.pem '
                '-o StrictHostKeyChecking=no" -ar --delete '
                '/media/snapshot root@{rhost}:/media'
                ' '.format(rhost=inst_reserve.public_dns_name))
                # SNAPSHOTIZATION!!!
                description = 'Created for {0} from {1}'.format(instance_id,
                                                        zone.region.name)
                if inst.tags.get('Name'):
                    description += ' named as {0}'.format(inst.tags['Name'])
                reserved_snapshot = volume_reserve.create_snapshot(
                                                                description)
                for tag in snap.tags:# Clone intance tags to the snapshot.
                    reserved_snapshot.add_tag(tag, snap.tags[tag])
                    # FIXME Following steps shouldn't be iterated for
                    # each `snap.tags`.
                    reserved_snapshot.add_tag('Dest', 'Backup snapshots')
                    reserved_snapshot.add_tag('Source',
                        'Created for {0} from {1}'.format(instance_id,
                                                          zone.region.name))
                print 'Waiting for the {snap} to be completed...'.format(
                                        snap=reserved_snapshot)
                while reserved_snapshot.status != 'completed':
                    print 'still {snap.status}...'.format(
                                    snap=reserved_snapshot)
                    _sleep(5)
                    reserved_snapshot.update()
                print 'done.'
                info = ('\nEnter FINISHED if you are finished looking at the '
                        'backup and would like to cleanup: ')
                while raw_input(info).strip() != 'FINISHED':
                    pass
            # Cleanup processing: terminate temporary server.
            finally:
                if inst and inst_reserve:
                    print 'Deleting the {0}...'.format(inst)
                    inst.terminate()
                    print 'Deleting the {0}...'.format(inst_reserve)
                    inst_reserve.terminate()
                    conn_reserve.delete_key_pair('snapshot_transfer')
                print 'done.'
        # Cleanup processing: delete detached backup volume.
        finally:
            if volume and volume_reserve:
                _wait_for(volume, ['status'], 'available')
                print 'Deleting the backup {vol}...'.format(vol=volume)
                delete = volume.delete()
                _wait_for(volume_reserve, ['status'], 'available')
                reserve_secuity_group.delete()
                print 'Deleting the backup {vol}...'.format(
                                                        vol=volume_reserve)
                delete = volume_reserve.delete()
                if res_snap:
                    print 'Deleting old snap {sn}...'.format(sn=res_snap.id)
                    delete = res_snap.delete()
                os.system('rm ./transfer_snapshot.pem')

    for zone in conn.get_all_zones():
        try:
            _mount_snapshot_in_zone(snap_id, zone)
        except _BotoServerError, err:
            print '{0} in {1}'.format(err, zone)
            continue
        else:
            break


def rsync_snapshot(snapshot_id, dst_region_name):
    pass


def rsync_region(src_region_name, dst_region_name, tag_name=None,
                 tag_value=None):
    """Duplicates latest snapshots with given tag into dst_region.

    src_region_name, dst_region_name
        every latest snapshot from src_region will be `rsync`ed to
        dst_region. Thus only latest snapshot will be stored in
        dst_region;
    tag_name, tag_value
        snapshots will be filtered by tag. Tag will be fetched from
        config by default, may be configured per region."""
    conn = _get_region_by_name(src_region_name).connect()
    tag_name = tag_name or config.get(reg, 'tag_name')
    tag_value = tag_value or config.get(reg, 'tag_value')
    filters = {'tag-key': tag_name, 'tag-value': tag_value}
    snaps = conn.get_all_snapshots(owner='self', filters=filters)
    for vol, vol_snaps in _groupby(snaps, lambda x: x.volume_id):
        latest_snap = sorted(vol_snaps, key=lambda x: x.start_time)[-1]
        rsync_snapshot(latest_snap.id, dst_region_name)

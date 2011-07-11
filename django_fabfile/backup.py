# -*- coding: utf-8 -*-
'''
Use configuration file ~/.boto for storing your credentials as described
at http://code.google.com/p/boto/wiki/BotoConfig#Credentials

All other options will be taken from ./fabfile.cfg file.

Commands presented to user (i.e. functions without preceeding
underscore) should guess region by beginning if its name using
`_get_region_by_name()`.
'''
import glob
import logging
import logging.handlers

from datetime import timedelta as _timedelta, datetime
from ConfigParser import ConfigParser as _ConfigParser
from contextlib import contextmanager as _contextmanager
from itertools import groupby as _groupby
from json import dumps as _dumps, loads as _loads
from operator import attrgetter as _attrgetter
from os import chmod as _chmod, remove as _remove
from os.path import (
    exists as _exists, realpath as _realpath, split as _split,
    splitext as _splitext)
from pprint import PrettyPrinter as _PrettyPrinter
from pydoc import pager as _pager
from re import compile as _compile, match as _match
from string import lowercase
from time import sleep as _sleep
from traceback import format_exc as _format_exc
from warnings import warn as _warn

from boto.ec2 import (connect_to_region as _connect_to_region,
                      regions as _regions)
from boto.ec2.blockdevicemapping import (
    BlockDeviceMapping as _BlockDeviceMapping,
    EBSBlockDeviceType as _EBSBlockDeviceType)
from boto.exception import (BotoServerError as _BotoServerError,
                            EC2ResponseError as _EC2ResponseError)
from fabric.api import env, prompt, put, sudo


config_file = 'fabfile.cfg'
config = _ConfigParser()
config.read(config_file)
for reg in _regions():
    if not config.has_section(reg.name):
        config.add_section(reg.name)
        with open(config_file, 'w') as f_p:
            config.write(f_p)

debug = config.getboolean('DEFAULT', 'debug')
username = config.get('DEFAULT', 'username')
ubuntu_aws_account = config.get('DEFAULT', 'ubuntu_aws_account')
architecture = config.get('DEFAULT', 'architecture')
aki_ptrn = config.get('DEFAULT', 'aki_ptrn')
ami_ptrn = config.get('DEFAULT', 'ami_ptrn')
ami_ptrn_with_version = config.get('DEFAULT', 'ami_ptrn_with_version')
ami_ptrn_with_release_date = config.get('DEFAULT',
                                        'ami_ptrn_with_release_date')
ami_regexp = config.get('DEFAULT', 'ami_regexp')
ssh_grp = config.get('DEFAULT', 'ssh_security_group')
ssh_timeout_attempts = config.getint('DEFAULT', 'ssh_timeout_attempts')
ssh_timeout_interval = config.getint('DEFAULT', 'ssh_timeout_interval')

env.update({'disable_known_hosts': True, 'user': username})


_now = lambda: datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')


def _print_dbg(text):
    """
    Logging process
    """
    if debug:
        LOG_FILENAME = 'log_rotating_file.out'

        # Set up a specific logger with our desired output level
        my_logger = logging.getLogger('MyLogger')
        my_logger.setLevel(logging.DEBUG)

        # Add the log message handler to the logger
        handler = logging.handlers.RotatingFileHandler(
            LOG_FILENAME, maxBytes=512000, backupCount=30)

        my_logger.addHandler(handler)
        my_logger.debug('DEBUG: {0}'.format(text))

        print 'DEBUG: {0}'.format(text)

    return


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

    def get_nested_attr(obj, attrs):
        attr = obj
        for attr_name in attrs:
            attr = getattr(attr, attr_name)
        return attr
    sleep_for = 3
    _print_dbg('Calling {0} updates'.format(obj))
    for i in range(10):     # Resource may be reported as "not exists"
        try:                # right after creation.
            getattr(obj, update_attr)()
        except:
            pass
        else:
            break
    getattr(obj, update_attr)()
    _print_dbg('Called {0} update'.format(obj))
    obj_state = get_nested_attr(obj, attrs)
    obj_region = getattr(obj, 'region', None)
    _print_dbg('State fetched from {0} in {1}'.format(obj, obj_region))
    if obj_state != state:
        if obj_region:
            info = 'Waiting for the {obj} in {obj.region} to be {state}...'
        else:
            info = 'Waiting for the {obj} to be {state}...'
        print info.format(obj=obj, state=state)
        while get_nested_attr(obj, attrs) != state:
            print 'still {0}...'.format(get_nested_attr(obj, attrs))
            sleep_for += 5
            _sleep(min(sleep_for, max_sleep))
            getattr(obj, update_attr)()
        print 'done.'


class _WaitForProper(object):

    """Decorate consecutive exceptions eating.

    >>> @_WaitForProper(attempts=3, pause=5)
    ... def test():
    ...     1 / 0
    ... 
    >>> test()
    ZeroDivisionError('integer division or modulo by zero',)
     waiting next 5 sec (2 times left)
    ZeroDivisionError('integer division or modulo by zero',)
     waiting next 5 sec (1 times left)
    ZeroDivisionError('integer division or modulo by zero',)
    """

    def __init__(self, attempts=10, pause=10):
        self.attempts = attempts
        self.pause = pause

    def __call__(self, func):

        def wrapper(*args, **kwargs):
            attempts = self.attempts
            while attempts > 0:
                attempts -= 1
                try:
                    return func(*args, **kwargs)
                except BaseException as err:
                    print _format_exc() if debug else repr(err)
                    if attempts > 0:
                        msg = ' waiting next {0} sec ({1} times left)'
                        print msg.format(self.pause, attempts)
                        _sleep(self.pause)
                else:
                    break
        return wrapper

_wait_for_sudo = _WaitForProper(attempts=ssh_timeout_attempts,
                                pause=ssh_timeout_interval)(sudo)


def _clone_tags(src_res, dst_res):
    for tag in src_res.tags:
        dst_res.add_tag(tag, src_res.tags[tag])


def _get_descr_attr(resource, attr):
    try:
        return _loads(resource.description)[attr]
    except:
        pass


def _get_snap_vol(snap):
    return _get_descr_attr(snap, 'Volume')


def _get_snap_time(snap):
    return _get_descr_attr(snap, 'Time')


def _dumps_resources(res_dict={}, res_list=[]):
    for res in res_list:
        res_dict.update(dict([unicode(res).split(':')]))
    return _dumps(res_dict)


def _get_latest_aki(conn, architecture):
    kernels = conn.get_all_images(filters={
        'image-type': 'kernel',
        'architecture': architecture,
        'owner-id': ubuntu_aws_account,
        'name': aki_ptrn})
    return sorted(kernels, key=_attrgetter('name'))[-1]


def _get_region_by_name(region_name):
    """Allow to specify region name fuzzyly."""
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


def modify_instance_termination(region, instance_id):
    """Mark production instnaces as uneligible for termination.

    region
        name of region where instance is located;
    instance_id
        instance to be updated;

    You must change value of preconfigured tag_name and run this command
    before terminating production instance via API."""
    conn = _get_region_by_name(region).connect()
    inst = _get_inst_by_id(conn.region.name, instance_id)
    prod_tag = config.get(conn.region.name, 'tag_name')
    prod_val = config.get(conn.region.name, 'tag_value')
    inst_tag_val = inst.tags.get(prod_tag)
    inst.modify_attribute('disableApiTermination', inst_tag_val == prod_val)


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


def create_snapshot(region_name, instance_id=None, instance=None,
                    dev='/dev/sda1', synchronously=False):
    """Return newly created snapshot of specified instance device.

    region_name
        name of region where instance is located;
    instance, instance_id
        either `instance_id` or `instance` argument should be specified;
    dev
        by default /dev/sda1 will be snapshotted;
    synchronously
        wait for completion."""
    assert bool(instance_id) ^ bool(instance), (
        'Either instance_id or instance should be specified')
    region = _get_region_by_name(region_name)
    if instance_id:
        instance = _get_inst_by_id(region.name, instance_id)
    vol_id = instance.block_device_mapping[dev].volume_id
    description = _dumps_resources({
        'Volume': vol_id,
        'Region': region.name,
        'Device': dev,
        'Type': instance.instance_type,
        'Arch': instance.architecture,
        'Root_dev_name': instance.root_device_name,
        'Time': _now(),
        }, [instance])
    conn = region.connect()
    snapshot = conn.create_snapshot(vol_id, description)
    _clone_tags(instance, snapshot)
    print '{0} initiated from Volume:{1} of {2}'.format(snapshot, vol_id,
                                                        instance)
    if synchronously:
        _wait_for(snapshot, ['status', ], 'completed')
    return snapshot


def backup_instance(region_name, instance_id=None, instance=None,
                    synchronously=False):
    """Return list of created snapshots for specified instance.

    region_name
        instance location;
    instance, instance_id
        either `instance_id` or `instance` argument should be specified;
    synchronously
        wait for completion."""
    assert bool(instance_id) ^ bool(instance), ('Either instance_id or '
        'instance should be specified')
    region = _get_region_by_name(region_name)
    if instance_id:
        instance = _get_inst_by_id(region.name, instance_id)
    snapshots = []  # NOTE Fabric doesn't supports generators.
    for dev in instance.block_device_mapping:
        snapshots.append(create_snapshot(
            region.name, instance=instance, dev=dev,
            synchronously=synchronously))
    return snapshots


def backup_instances_by_tag(region_name=None, tag_name=None, tag_value=None):
    """Creates backup for all instances with given tag in region.

    region_name
        will be applied across all regions by default;
    tag_name, tag_value
        will be fetched from config by default, may be configured
        per region."""
    snapshots = []
    region = _get_region_by_name(region_name) if region_name else None
    reg_names = [region.name] if region else (reg.name for reg in _regions())
    for reg in reg_names:
        tag_name = tag_name or config.get(reg, 'tag_name')
        tag_value = tag_value or config.get(reg, 'tag_value')
        conn = _connect_to_region(reg)
        filters = {'resource-type': 'instance', 'key': tag_name,
                   'tag-value': tag_value}
        for tag in conn.get_all_tags(filters=filters):
            snapshots += backup_instance(reg, instance_id=tag.res_id)
    return snapshots


def _trim_snapshots(region_name, dry_run=False):

    """Delete snapshots back in time in logarithmic manner.

    dry_run
        just print snapshot to be deleted."""
    hourly_backups = config.getint('purge_backups', 'hourly_backups')
    daily_backups = config.getint('purge_backups', 'daily_backups')
    weekly_backups = config.getint('purge_backups', 'weekly_backups')
    monthly_backups = config.getint('purge_backups', 'monthly_backups')
    quarterly_backups = config.getint('purge_backups', 'quarterly_backups')
    yearly_backups = config.getint('purge_backups', 'yearly_backups')

    conn = _get_region_by_name(region_name).connect()
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
                                try:
                                    conn.delete_snapshot(snap.id)
                                except _EC2ResponseError as err:
                                    print str(err)
                                else:
                                    print('Trimmed %s %s from %s' % (snap,
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


def trim_snapshots(region_name=None, dry_run=False):
    """Delete old snapshots logarithmically back in time.

    region_name
        by default process all regions;
    dry_run
        boolean, only print info about old snapshots to be deleted."""
    region = _get_region_by_name(region_name) if region_name else None
    reg_names = [region.name] if region else (reg.name for reg in _regions())
    for reg in reg_names:
        print reg


def create_instance(region_name='us-east-1', zone_name=None, key_pair=None,
                    security_groups=None):
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

    region = _get_region_by_name(region_name)
    conn = region.connect()

    filters={'owner_id': ubuntu_aws_account, 'architecture': architecture,
             'name': ami_ptrn, 'image_type': 'machine',
             'root_device_type': 'ebs'}
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
    name_with_version_and_release = ami_ptrn_with_release_date.format(
        version=latest_version, released_at=latest_date)
    filters.update({'name': name_with_version_and_release})
    image = conn.get_all_images(filters=filters)[0]
    zone = zone_name or conn.get_all_zones()[-1].name
    print 'Launching new instance in {zone} using {image}'.format(image=image,
                                                                 zone=zone)

    key_pair = key_pair or config.get(region.name, 'key_pair')
    reservation = image.run(key_name=key_pair, instance_type='t1.micro',
                            placement=zone, security_groups=security_groups)
    assert len(reservation.instances) == 1, 'More than 1 instances created'
    inst = reservation.instances[0]
    _wait_for(inst, ['state', ], 'running')
    print '{inst} created in {zone}'.format(inst=inst, zone=zone)

    return inst


@_contextmanager
def _create_temp_inst(zone, key_pair=None, security_groups=None):
    inst = create_instance(zone.region.name, zone.name, key_pair=key_pair,
                           security_groups=security_groups)
    inst.add_tag('Earmarking', 'temporary')
    try:
        yield inst
    finally:
        print 'Terminating the {0} in {0.region}...'.format(inst)
        inst.terminate()
        _wait_for(inst, ['state'], 'terminated')


def _get_avail_dev(instance):
    """Return next unused device name."""
    chars = lowercase
    for dev in instance.block_device_mapping:
        chars = chars.replace(dev[-2], '')
    return '/dev/sd{0}1'.format(chars[0])


@_contextmanager
def _attach_snapshot(snap, key_pair=None, security_groups=None):
    """Create temporary instance and attach the snapshot."""
    _wait_for(snap, ['status', ], 'completed')
    conn = snap.region.connect()
    for zone in conn.get_all_zones():
        try:
            volume = conn.create_volume(snap.volume_size, zone, snap)
            _clone_tags(snap, volume)
            _print_dbg('Tags cloned from {0} to {1}'.format(snap, volume))
            try:
                with _create_temp_inst(
                    zone, key_pair=key_pair, security_groups=security_groups) \
                    as inst:
                    dev_name = _get_avail_dev(inst)
                    _print_dbg('Got avail {0} from {1}'.format(dev_name, inst))
                    volume.attach(inst.id, dev_name)
                    _print_dbg('Attached {0} to {1}'.format(volume, inst))
                    volume.update()
                    _wait_for(volume, ['attach_data', 'status'], 'attached')
                    yield volume
            finally:
                _wait_for(volume, ['status', ], 'available')
                print 'Deleting {vol} in {vol.region}...'.format(vol=volume)
                volume.delete()
        except _BotoServerError as err:
            print _format_exc() if debug else '{0} in {1}'.format(err, zone)
            continue
        else:
            break


def _get_vol_dev(vol, key_filename=None):
    if not vol.attach_data.instance_id:
        return
    inst = _get_inst_by_id(vol.region.name, vol.attach_data.instance_id)
    if not inst.public_dns_name:    # The instance is down.
        return
    key_filename = key_filename or config.get(vol.region.name, 'key_filename')
    env.update({'host_string': inst.public_dns_name,
                'key_filename': key_filename})
    attached_dev = vol.attach_data.device.replace('/dev/', '')
    natty_dev = attached_dev.replace('sd', 'xvd')
    _print_dbg(_PrettyPrinter().pformat(env))
    inst_devices = _wait_for_sudo('ls /dev').split()
    for dev in [attached_dev, natty_dev]:
        if dev in inst_devices:
            return '/dev/{0}'.format(dev)


def _mount_volume(vol, key_filename=None, mkfs=False):

    """Mount the device by SSH. Return mountpoint on success.

    vol
        volume to be mounted on the instance it is attached to;
    key_filename
        location of the private key to access instance, where `vol` is
        mounted. Fetched from config by default."""

    vol.update()
    inst = _get_inst_by_id(vol.region.name, vol.attach_data.instance_id)
    key_filename = key_filename or config.get(vol.region.name, 'key_filename')

    env.update({'host_string': inst.public_dns_name,
                'key_filename': key_filename})
    dev = _get_vol_dev(vol, key_filename)
    mountpoint = dev.replace('/dev/', '/media/')
    _wait_for_sudo('mkdir {0}'.format(mountpoint))
    if mkfs:
        sudo('mkfs.ext3 {dev}'.format(dev=dev))
    sudo('mount {dev} {mnt}'.format(dev=dev, mnt=mountpoint))
    if mkfs:
        sudo('chown -R {user}:{user} {mnt}'.format(user=username,
                                                   mnt=mountpoint))
    return mountpoint


@_contextmanager
def _config_temp_ssh(conn):
    config_name = '{region}-temp-ssh-{now}'.format(region=conn.region.name,
                                                   now=_now())

    if config_name in [k_p.name for k_p in conn.get_all_key_pairs()]:
        conn.delete_key_pair(config_name)
    key_pair = conn.create_key_pair(config_name)
    key_filename = key_pair.name + '.pem'
    if _exists(key_filename):
        _remove(key_filename)
    key_pair.save('./')
    _chmod(key_filename, 0600)

    try:
        yield _realpath(key_filename)
    finally:
        key_pair.delete()
        _remove(key_filename)


def mount_snapshot(region_name=None, snap_id=None):

    """Mount snapshot to temporary created instance.

    region_name
        snapshot location
    snap_id."""

    if not region_name or not snap_id:
        region_name, snap_id = _select_snapshot()
    region = _get_region_by_name(region_name)
    conn = region.connect()
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id, ])[0]

    info = ('\nYou may now SSH into the {inst} server, using:'
            '\n ssh -i {key} {user}@{inst.public_dns_name}')
    with _attach_snapshot(snap, security_groups=[ssh_grp]) as vol:
        mountpoint = _mount_volume(vol)
        if mountpoint:
            info += ('\nand browse snapshot, mounted at {mountpoint}.')
        else:
            info += ('\nand mount {device}. NOTE: device name may be '
                     'altered by system.')
        key_file = config.get(region.name, 'key_filename')
        inst = _get_inst_by_id(region.name, vol.attach_data.instance_id)
        print info.format(
            inst=inst, user=username, key=key_file,
            device=vol.attach_data.device, mountpoint=mountpoint)

        info = ('\nEnter FINISHED if you are finished looking at the '
                'backup and would like to cleanup: ')
        while raw_input(info).strip() != 'FINISHED':
            pass


def _rsync_mountpoints(src_inst, src_mnt, dst_inst, dst_mnt, dst_key_file):
    """Run `rsync` against mountpoints.

    dst_key_file
        private key that will be used in src_inst to access dst_inst."""
    env.update({'host_string': dst_inst.public_dns_name,
                'key_filename': dst_key_file})
    sudo('cp /root/.ssh/authorized_keys /root/.ssh/authorized_keys.bak')
    sudo('cp .ssh/authorized_keys /root/.ssh/')
    src_key_filename = config.get(src_inst.region.name, 'key_filename')
    env.update({'host_string': src_inst.public_dns_name,
                'key_filename': src_key_filename})
    put(dst_key_file, '.ssh/', mirror_local_mode=True)
    dst_key_filename = _split(dst_key_file)[1]
    cmd = ('rsync -e "ssh -i .ssh/{key_file} -o StrictHostKeyChecking=no" '
           '-a --delete {src_mnt}/ root@{rhost}:{dst_mnt}')
    sudo(cmd.format(rhost=dst_inst.public_dns_name, key_file=dst_key_filename,
                    src_mnt=src_mnt, dst_mnt=dst_mnt))
    env.update({'host_string': dst_inst.public_dns_name,
                'key_filename': dst_key_file})
    sudo('mv /root/.ssh/authorized_keys.bak /root/.ssh/authorized_keys')


def _rsync_snap_to_vol(src_snap, dst_vol, dst_key_file, mkfs=False):

    """Run `rsync` to update dst_vol from src_snap."""

    with _attach_snapshot(src_snap, security_groups=[ssh_grp]) as src_vol:
        src_mnt = _mount_volume(src_vol)
        dst_mnt = _mount_volume(dst_vol, dst_key_file, mkfs=mkfs)
        src_inst = _get_inst_by_id(src_vol.region.name,
                                   src_vol.attach_data.instance_id)
        dst_inst = _get_inst_by_id(dst_vol.region.name,
                                   dst_vol.attach_data.instance_id)
        _rsync_mountpoints(src_inst, src_mnt, dst_inst, dst_mnt,
                           dst_key_file)


def _create_fresh_snap(dst_vol, src_snap):
    """Create new snapshot with same description and tags."""
    new_dst_snap = dst_vol.create_snapshot(src_snap.description)
    _clone_tags(src_snap, new_dst_snap)
    _wait_for(new_dst_snap, ['status', ], 'completed')


def rsync_snapshot(src_region_name, snapshot_id, dst_region_name):

    """Duplicate the snapshot into dst_region.

    src_region_name, dst_region_name
        Amazon region names. Allowed to be contracted, e.g.
        `ap-southeast-1` will be recognized in `ap-south` or even
        `ap-s`;
    snapshot_id
        snapshot to duplicate."""
    src_conn = _get_region_by_name(src_region_name).connect()
    dst_reg = _get_region_by_name(dst_region_name)
    dst_conn = dst_reg.connect()
    src_snap = src_conn.get_all_snapshots([snapshot_id])[0]

    info = 'Transmitting {snap} {snap.description}'
    if src_snap.tags.get('Name'):
        info += ' of {name}'
    info += ' from {src} to {dst}'
    print info.format(snap=src_snap, src=src_conn.region, dst=dst_conn.region,
                      name=src_snap.tags.get('Name'))

    snaps = dst_conn.get_all_snapshots(owner='self')
    src_vol = _get_snap_vol(src_snap)
    dst_snaps = [snp for snp in snaps if _get_snap_vol(snp) == src_vol]

    dst_snap = sorted(dst_snaps, key=_get_snap_time)[-1] if dst_snaps else None

    if _get_snap_time(dst_snap) >= _get_snap_time(src_snap):
        info = ' {src} is not newer than {dst} {dst.description} in {dst_reg}'
        print info.format(src=src_snap, dst=dst_snap, dst_reg=dst_reg)
        return

    with _config_temp_ssh(dst_conn) as key_file:
        key_pair = _splitext(_split(key_file)[1])[0]

        if dst_snap:
            with _attach_snapshot(dst_snap, key_pair, [ssh_grp]) as dst_vol:
                _rsync_snap_to_vol(src_snap, dst_vol, key_file)
                _create_fresh_snap(dst_vol, src_snap)
            dst_snap.delete()
        else:
            dst_zn = dst_conn.get_all_zones()[-1]     # Just latest zone.
            with _create_temp_inst(dst_zn, key_pair, [ssh_grp]) as dst_inst:
                dst_vol = dst_conn.create_volume(src_snap.volume_size, dst_zn)
                _clone_tags(src_snap, dst_vol)
                dst_dev = _get_avail_dev(dst_inst)
                dst_vol.attach(dst_inst.id, dst_dev)
                _rsync_snap_to_vol(src_snap, dst_vol, key_file, mkfs=True)
                _create_fresh_snap(dst_vol, src_snap)
            _wait_for(dst_vol, ['status', ], 'available')
            dst_vol.delete()


def rsync_region(src_region_name, dst_region_name, tag_name=None,
                 tag_value=None, native_only=True):
    """Duplicates latest snapshots with given tag into dst_region.

    src_region_name, dst_region_name
        every latest snapshot from src_region will be `rsync`ed to
        dst_region. Thus only latest snapshot will be stored in
        dst_region;
    tag_name, tag_value
        snapshots will be filtered by tag. Tag will be fetched from
        config by default, may be configured per region;
    native
        sync only snapshots, created in the src_region_name."""
    src_region = _get_region_by_name(src_region_name)
    conn = src_region.connect()
    tag_name = tag_name or config.get(src_region.name, 'tag_name')
    tag_value = tag_value or config.get(src_region.name, 'tag_value')
    filters = {'tag-key': tag_name, 'tag-value': tag_value}
    snaps = conn.get_all_snapshots(owner='self', filters=filters)
    _is_described = lambda snap: _get_snap_vol(snap) and _get_snap_time(snap)
    snaps = [snp for snp in snaps if _is_described(snp)]
    if native_only:

        def _is_native(snap, region):
            return _get_descr_attr(snap, 'Region') == region.name
        snaps = [snp for snp in snaps if _is_native(snp, src_region)]
    snaps = sorted(snaps, key=_get_snap_vol)    # Prepare for grouping.
    for vol, vol_snaps in _groupby(snaps, _get_snap_vol):
        latest_snap = sorted(vol_snaps, key=_get_snap_time)[-1]
        try:
            rsync_snapshot(src_region_name, latest_snap.id, dst_region_name)
        except:
            print _format_exc()


def launch_instance_from_ami(region_name, ami_id, inst_type=None):
    """Create instance from specified AMI.

    region_name
        location of the AMI and new instance;
    ami_id
        "ami-..."
    inst_type
        by default will be fetched from AMI description or used
        't1.micro' if not mentioned in the description."""
    conn = _get_region_by_name(region_name).connect()
    image = conn.get_all_images([ami_id])[0]
    inst_type = inst_type or _get_descr_attr(image, 'Type') or 't1.micro'
    _security_groups = _prompt_to_select(
        [sec.name for sec in conn.get_all_security_groups()],
        'Select security group')
    _wait_for(image, ['state'], 'available')
    reservation = image.run(
        key_name = config.get(conn.region.name, 'key_pair'),
        security_groups = [_security_groups, ],
        instance_type = inst_type,
        kernel_id=_get_latest_aki(conn, image.architecture).id)
    new_instance = reservation.instances[0]
    _wait_for(new_instance, ['state', ], 'running')
    _clone_tags(image, new_instance)
    modify_instance_termination(conn.region.name, new_instance.id)
    info = ('\nYou may now SSH into the {inst} server, using:'
            '\n ssh -i {key} {user}@{inst.public_dns_name}')
    key_file = config.get(conn.region.name, 'key_filename')
    print info.format(inst=new_instance, user=username, key=key_file)


def create_ami(region=None, snap_id=None, force=None, root_dev='/dev/sda1',
               inst_arch='x86_64', inst_type='t1.micro'):
    """
    Creates AMI image from given snapshot.

    Force option removes prompt request and creates new instance from
    created ami image.
    region, snap_id
        specify snapshot to be processed; snapshot description must be
        json description of snapshotted instance.
    force
        Run instance from ami after creation without confirmation. To
        enable set value to "RUN";
    """
    if not region or not snap_id:
        region, snap_id = _select_snapshot()
    conn = _get_region_by_name(region).connect()
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id, ])[0]
    # setup for building an EBS boot snapshot"
    ebs = _EBSBlockDeviceType()
    ebs.snapshot_id = snap_id
    block_map = _BlockDeviceMapping()
    block_map[root_dev] = ebs

    name = 'Created {0} using access key {1}'.format(_now(), conn.access_key)
    name = name.replace(":", ".").replace(" ", "_")

    # create the new AMI all options from snap JSON description:
    result = conn.register_image(
        name=name,
        description = snap.description,
        architecture = _get_descr_attr(snap, 'Arch') or inst_arch,
        root_device_name = _get_descr_attr(snap, 'Root_dev_name') or root_dev,
        block_device_map = block_map)
    image = conn.get_all_images(image_ids=[result, ])[0]
    _clone_tags(snap, image)

    print 'The new AMI ID = ', result

    info = ('\nEnter RUN if you want to launch instance using '
            'just created {0}: '.format(image))
    if force == 'RUN' or raw_input(info).strip() == 'RUN':
        launch_instance_from_ami(region, image.id, inst_type=inst_type)

# -*- coding: utf-8 -*-
'''
Use configuration file ~/.boto for storing your credentials as described
at http://code.google.com/p/boto/wiki/BotoConfig#Credentials

All other options will be taken from ./fabfile.cfg file - copy-paste it
from `django_fabfile/fabfile.cfg.def`.

USAGE:
------

  1. For backup creation you must specify instance_id and region in
  [main] section. To create snapshot of mounted volume, run:
          fab -f backup.py backup_instance
  2. To purge old snapshots you must specify # of snapshots to save in
  [mount_backups], specify instance_id and region in [main] section.
  Then run:
          fab -f backup.py trim_snapshots
  3. To mount backup specify needed values in [mount_backups] section of
  fabfile.cfg, specify instance_id and region in [main] section. Then run:
          fab -f backup.py mount_snapshot
  4. To backup all instances in all regions, which tagged with some tag
  ('Earmarking':'production' for example), add this tags to [main]
  section of fabfile.cfg and run:
          fab -f backup.py backup_instances_by_tag
  5. To purge old snapshots in all regions, run:
          fab -f backup.py trim_snapshots_for_regions
'''
import logging
import logging.handlers
import os
import sys

from datetime import timedelta as _timedelta, datetime
from ConfigParser import (ConfigParser as _ConfigParser, NoOptionError as
                          _NoOptionError)
from contextlib import contextmanager as _contextmanager, nested as _nested
from itertools import groupby as _groupby
from json import dumps as _dumps, loads as _loads
from os import chmod as _chmod, remove as _remove
from os.path import (realpath as _realpath, split as _split)
from pprint import PrettyPrinter as _PrettyPrinter
from pydoc import pager as _pager
from re import compile as _compile
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
from fabric.api import env, local, output, prompt, put, settings, sudo
from fabric.contrib.files import append, exists

from django_fabfile.utils import _get_region_by_name


config_file = 'fabfile.cfg'
config = _ConfigParser()
config.read(config_file)
try:
    username = config.get('DEFAULT', 'username')
    debug = config.getboolean('DEFAULT', 'debug')
    logging_folder = config.get('DEFAULT', 'logging_folder')
except _NoOptionError:
    username = 'ubuntu'
    debug = logging_folder = False
else:
    env.update({'user': username})

env.update({'disable_known_hosts': True})

# Set up a specific logger with desired output level
LOG_FORMAT = '%(asctime)-15s %(levelname)s:%(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S %Z'

logger = logging.getLogger(__name__)

if debug:
    logger.setLevel(logging.DEBUG)
    output['debug'] = True
else:
    logger.setLevel(logging.INFO)

if logging_folder:
    LOG_FILENAME = os.path.join(logging_folder, __name__ + '.log')
    handler = logging.handlers.TimedRotatingFileHandler(
        LOG_FILENAME, 'midnight', backupCount=30)

    class StreamLogger():

        def __init__(self, level=logging.INFO):
            self.logger = logging.getLogger(__name__)
            self.level = level

        def write(self, row):
            row = row.strip()
            if row:
                self.logger.log(self.level, row)

        def flush(self):
            pass
    # Redirect Fabric output to log file.
    sys.stdout = StreamLogger()
    sys.stderr = StreamLogger(level=logging.ERROR)
else:
    handler = logging.StreamHandler(sys.stdout)

fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFORMAT)
handler.setFormatter(fmt)
logger.addHandler(handler)


_now = lambda: datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')


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


class StateNotChangedError(Exception):

    def __init__(self, state):
        self.state = state

    def __str__(self):
        return 'State remain {0} after limited time gone'.format(self.state)


def _wait_for(obj, state, attrs=None, max_sleep=30, limit=5 * 60):
    """Wait for attribute to go into state.

    attrs
        list of nested attribute names."""

    def get_state(obj, attrs=None):
        obj_state = obj.update()
        if not attrs:
            return obj_state
        else:
            attr = obj
            for attr_name in attrs:
                attr = getattr(attr, attr_name)
            return attr
    logger.debug('Calling {0} updates'.format(obj))
    for i in range(10):     # Resource may be reported as "not exists"
        try:                # right after creation.
            obj_state = get_state(obj, attrs)
        except Exception as err:
            logger.debug(str(err))
        else:
            break
    logger.debug('Called {0} update'.format(obj))
    obj_region = getattr(obj, 'region', None)
    logger.debug('State fetched from {0} in {1}'.format(obj, obj_region))
    if obj_state != state:
        if obj_region:
            info = 'Waiting for the {obj} in {obj.region} to be {state}...'
        else:
            info = 'Waiting for the {obj} to be {state}...'
        logger.info(info.format(obj=obj, state=state))
        slept, sleep_for = 0, 3
        while obj_state != state and slept < limit:
            logger.info('still {0}...'.format(obj_state))
            sleep_for = sleep_for + 5 if sleep_for < max_sleep else max_sleep
            _sleep(sleep_for)
            slept += sleep_for
            obj_state = get_state(obj, attrs)
        if obj_state == state:
            logger.info('done.')
        else:
            raise StateNotChangedError(obj_state)


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
                    logger.debug(_format_exc())
                    logger.error(repr(err))

                    if attempts > 0:
                        logger.info('waiting next {0} sec ({1} times left)'
                            .format(self.pause, attempts))
                        _sleep(self.pause)
                else:
                    break
        return wrapper

try:
    ssh_timeout_attempts = config.getint('DEFAULT', 'ssh_timeout_attempts')
    ssh_timeout_interval = config.getint('DEFAULT', 'ssh_timeout_interval')
except _NoOptionError as err:
    _warn(str(err))
else:
    _wait_for_sudo = _WaitForProper(attempts=ssh_timeout_attempts,
                                    pause=ssh_timeout_interval)(sudo)
    _wait_for_exists = _WaitForProper(attempts=ssh_timeout_attempts,
                                      pause=ssh_timeout_interval)(exists)


def _add_tags(res, tags):
    for tag in tags:
        res.add_tag(tag, tags[tag])
    logger.debug('Tags added to {0}'.format(res))


def _get_descr_attr(resource, attr):
    try:
        return _loads(resource.description)[attr]
    except:
        pass


def _get_snap_vol(snap):
    return _get_descr_attr(snap, 'Volume')


def _get_snap_time(snap):
    return _get_descr_attr(snap, 'Time')


def _get_inst_by_id(region, instance_id):
    conn = region.connect()
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


def update_volumes_tags(filters=None):
    """Clone tags from instances to volumes.

    filters
        apply optional filtering for the `get_all_instances`."""
    for region in _regions():
        reservations = region.connect().get_all_instances(filters=filters)
        for res in reservations:
            inst = res.instances[0]
            for bdm in inst.block_device_mapping.keys():
                vol_id = inst.block_device_mapping[bdm].volume_id
                vol = inst.connection.get_all_volumes([vol_id])[0]
                _add_tags(vol, inst.tags)


def modify_instance_termination(region, instance_id):
    """Mark production instnaces as uneligible for termination.

    region
        name of region where instance is located;
    instance_id
        instance to be updated;

    You must change value of preconfigured tag_name and run this command
    before terminating production instance via API."""
    conn = _get_region_by_name(region).connect()
    inst = _get_inst_by_id(conn.region, instance_id)
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
            logger.info('No snapshot with provided ID found')

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


def _create_snapshot(vol, description='', tags=None, synchronously=True):
    """Return new snapshot for the volume.

    vol
        volume to snapshot;
    synchronously
        wait for successful completion;
    description
        description for snapshot. Will be compiled from instnace
        parameters by default;
    tags
        tags to be added to snapshot. Will be cloned from volume by
        default."""
    if not description and vol.attach_data:
        instance = _get_inst_by_id(vol.region, vol.attach_data.instance_id)
        description = _dumps({
            'Volume': vol.id,
            'Region': vol.region.name,
            'Device': vol.attach_data.device,
            'Instance': instance.id,
            'Type': instance.instance_type,
            'Arch': instance.architecture,
            'Root_dev_name': instance.root_device_name,
            'Time': _now(),
            })

    def _initiate_snapshot():
        snapshot = vol.create_snapshot(description)
        _add_tags(snapshot, tags or vol.tags)
        logger.info('{0} initiated from {1}'.format(snapshot, vol))
        return snapshot

    if synchronously:
        timeout = config.getint('DEFAULT', 'minutes_for_snap')
        while True:     # Iterate unless success and delete failed snapshots.
            snapshot = _initiate_snapshot()
            try:
                _wait_for(snapshot, '100%', limit=timeout * 60)
                assert snapshot.status == 'completed', (
                    'completed with wrong status {0}'.format(snapshot.status))
            except (StateNotChangedError, AssertionError) as err:
                logger.error(str(err) + ' - deleting')
                snapshot.delete()
            else:
                break
    else:
        snapshot = _initiate_snapshot()
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
        instance = _get_inst_by_id(region, instance_id)
    snapshots = []
    for dev in instance.block_device_mapping:
        vol_id = instance.block_device_mapping[dev].volume_id
        vol = region.connect().get_all_volumes([vol_id])[0]
        snapshots.append(_create_snapshot(vol, synchronously=synchronously))
    return snapshots


def backup_instances_by_tag(region_name=None, tag_name=None, tag_value=None,
                            synchronously=True):
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
            snapshots += backup_instance(reg, instance_id=tag.res_id,
                                         synchronously=synchronously)
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
          now.day) - _timedelta(days=(now.weekday() + 1) % 7)
    last_month = datetime(now.year, now.month -1, now.day)
    last_year = datetime(now.year-1, now.month, now.day)
    other_years = datetime(now.year-2, now.month, now.day)
    start_of_month = datetime(now.year, now.month, 1)

    target_backup_times = []
    # there are no snapshots older than 1/1/2000
    oldest_snapshot_date = datetime(2000, 1, 1)

    for hour in range(0, hourly_backups):
        target_backup_times.append(last_hour - _timedelta(hours=hour))

    for day in range(0, daily_backups):
        target_backup_times.append(last_midnight - _timedelta(days=day))

    for week in range(0, weekly_backups):
        target_backup_times.append(last_sunday - _timedelta(weeks=week))

    for month in range(0, monthly_backups):
        target_backup_times.append(last_month - _timedelta(weeks=month * 4))

    for quart in range(0, quarterly_backups):
        target_backup_times.append(last_year - _timedelta(weeks=quart * 16))

    for year in range(0, yearly_backups):
        target_backup_times.append(other_years - _timedelta(days=year * 365))


    one_day = _timedelta(days=1)
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
    all_snapshots = conn.get_all_snapshots(owner='self')
    # oldest first
    all_snapshots.sort(cmp=lambda x, y: cmp(x.start_time, y.start_time))

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
                                logger.info('Dry-trimmed {0} {1} from {2}'
                                    .format(snap, snap.description,
                                    snap.start_time))
                            else:
                                # as long as the snapshot wasn't marked with
                                # the 'preserve_snapshot' tag, delete it:
                                try:
                                    conn.delete_snapshot(snap.id)
                                except _EC2ResponseError as err:
                                    logger.exception(str(err))
                                else:
                                    logger.info('Trimmed {0} {1} from {2}'
                                        .format(snap, snap.description,
                                        snap.start_time))
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


def delete_broken_snapshost():
    for region in _regions():
        conn = region.connect()
        filters = {'status': 'error'}
        snaps = conn.get_all_snapshots(owner='self', filters=filters)
        for snp in snaps:
            logger.info('Deleting broken {0}'.format(snp))
            snp.delete()


def trim_snapshots(region_name=None, dry_run=False):
    """Delete old snapshots logarithmically back in time.

    region_name
        by default process all regions;
    dry_run
        boolean, only print info about old snapshots to be deleted."""
    delete_broken_snapshost()
    region = _get_region_by_name(region_name) if region_name else None
    reg_names = [region.name] if region else (reg.name for reg in _regions())
    for reg in reg_names:
        logger.info('Processing {0}'.format(reg))
        _trim_snapshots(reg)


def create_instance(region_name='us-east-1', zone_name=None, key_pair=None,
                    security_groups=None):
    """Create AWS EC2 instance.

    Return created instance.

    region_name
        by default will be created in the us-east-1 region;
    zone_name
        string-formatted name. By default will be used latest zone;
    key_pair
        name of key_pair to be granted access. Will be fetched from
        config by default, may be configured per region."""

    region = _get_region_by_name(region_name)
    conn = region.connect()

    ami_ptrn = config.get('DEFAULT', 'ami_ptrn')
    architecture = config.get('DEFAULT', 'architecture')
    ubuntu_aws_account = config.get('DEFAULT', 'ubuntu_aws_account')
    filters = {'owner_id': ubuntu_aws_account, 'architecture': architecture,
             'name': ami_ptrn, 'image_type': 'machine',
             'root_device_type': 'ebs'}
    images = conn.get_all_images(filters=filters)

    # Filtering by latest version.
    ptrn = _compile(config.get('DEFAULT', 'ami_regexp'))
    versions = set([ptrn.search(img.name).group('version') for img in images])

    def complement(year_month):
        return '0' + year_month if len(year_month) == 4 else year_month

    latest_version = sorted(set(filter(complement, versions)))[-1]  # XXX Y3K.
    ami_ptrn_with_version = config.get('DEFAULT', 'ami_ptrn_with_version')
    name_with_version = ami_ptrn_with_version.format(version=latest_version)
    filters.update({'name': name_with_version})
    images = conn.get_all_images(filters=filters)
    # Filtering by latest release date.
    dates = set([ptrn.search(img.name).group('released_at') for img in images])
    latest_date = sorted(set(dates))[-1]
    ami_ptrn_with_release_date = config.get('DEFAULT',
                                            'ami_ptrn_with_release_date')
    name_with_version_and_release = ami_ptrn_with_release_date.format(
        version=latest_version, released_at=latest_date)
    filters.update({'name': name_with_version_and_release})
    image = conn.get_all_images(filters=filters)[0]
    zone = zone_name or conn.get_all_zones()[-1].name
    logger.info('Launching new instance in {zone} using {image}'
        .format(image=image, zone=zone))

    key_pair = key_pair or config.get(region.name, 'key_pair')
    ssh_grp = config.get('DEFAULT', 'ssh_security_group')
    reservation = image.run(
        key_name=key_pair, instance_type='t1.micro', placement=zone,
        security_groups=security_groups or [ssh_grp])
    assert len(reservation.instances) == 1, 'More than 1 instances created'
    inst = reservation.instances[0]
    _wait_for(inst, 'running')
    logger.info('{inst} created in {zone}'.format(inst=inst, zone=zone))

    return inst


@_contextmanager
def _create_temp_inst(region=None, zone=None, key_pair=None,
                      security_groups=None, synchronously=False):
    if region and zone:
        assert zone in region.connect().get_all_zones(), (
            '{0} doesn\'t belong to {1}'.format(zone, region))

    def _create_inst_in_zone(zone, key_pair, sec_grps):
        inst = create_instance(zone.region.name, zone.name, key_pair=key_pair,
                               security_groups=sec_grps)
        inst.add_tag(config.get(zone.region.name, 'tag_name'), 'temporary')
        return inst

    if zone:
        inst = _create_inst_in_zone(zone, key_pair, security_groups)
    else:
        for zone in region.connect().get_all_zones():
            try:
                inst = _create_inst_in_zone(zone, key_pair, security_groups)
            except _BotoServerError as err:
                logging.debug(_format_exc())
                logging.error('{0} in {1}'.format(err, zone))
                continue
            else:
                break
    try:
        yield inst
    finally:
        logger.info('Terminating the {0} in {0.region}...'.format(inst))
        inst.terminate()
        if synchronously:
            _wait_for(inst, 'terminated')


def _get_avail_dev(inst):
    """Return next unused device name."""
    inst.update()
    chars = lowercase
    for dev in inst.block_device_mapping:
        chars = chars.replace(dev[-2], '')
    return '/dev/sd{0}1'.format(chars[0])


def _mount_volume(vol, mkfs=False):

    """Mount the device by SSH. Return mountpoint on success.

    vol
        volume to be mounted on the instance it is attached to."""

    vol.update()
    assert vol.attach_data
    inst = _get_inst_by_id(vol.region, vol.attach_data.instance_id)
    key_filename = config.get(vol.region.name, 'key_filename')
    with settings(host_string=inst.public_dns_name, key_filename=key_filename):
        dev = _get_vol_dev(vol)
        mountpoint = dev.replace('/dev/', '/media/')
        _wait_for_sudo('mkdir -p {0}'.format(mountpoint))
        if mkfs:
            _wait_for_sudo('mkfs.ext3 {dev}'.format(dev=dev))
        """Add disk label for normal boot on created volume"""
        _wait_for_sudo('e2label {dev} uec-rootfs'.format(dev=dev))
        _wait_for_sudo('mount {dev} {mnt}'.format(dev=dev, mnt=mountpoint))
        if mkfs:
            _wait_for_sudo('chown -R {user}:{user} {mnt}'.format(
                           user=env.user, mnt=mountpoint))
    logger.debug('Mounted {0} to {1} at {2}'.format(vol, inst, mountpoint))
    return mountpoint


@_contextmanager
def _attach_snapshot(snap, key_pair=None, security_groups=None, inst=None):

    """Attach `snap` to `inst` or to new temporary instance.

    Yield volume, created from the `snap` and its mountpoint.

    Create temporary instance if `inst` not provided. Provide access to
    newly created temporary instance for `key_pair` and with
    `security_groups`."""

    _wait_for(snap, '100%', limit=10 * 60)

    def _force_snap_attach(inst, snap):
        """Iterate over devices until successful attachment."""
        volumes_to_delete = []
        while _get_avail_dev(inst):
            vol = inst.connection.create_volume(snap.volume_size,
                                                inst.placement, snap)
            _add_tags(vol, snap.tags)
            vol.add_tag(config.get(inst.region.name, 'tag_name'), 'temporary')
            volumes_to_delete.append(vol)
            dev_name = _get_avail_dev(inst)
            logger.debug('Got avail {0} from {1}'.format(dev_name, inst))
            vol.attach(inst.id, dev_name)
            try:
                _wait_for(vol, 'attached', ['attach_data', 'status'], limit=60)
            except StateNotChangedError:
                logger.error('Attempt to attach as next device')
            else:
                break
        return vol, volumes_to_delete

    @_contextmanager
    def _attach_snap_to_inst(inst, snap):
        """Cleanup volume(s)."""
        _wait_for(inst, 'running')
        try:
            vol, volumes = _force_snap_attach(inst, snap)
            mnt = _mount_volume(vol)
            yield vol, mnt
        except BaseException as err:
            logger.exception(str(err))
        finally:
            key_filename = config.get(inst.region.name, 'key_filename')
            with settings(host_string=inst.public_dns_name,
                          key_filename=key_filename):
                _wait_for_sudo('umount {0}'.format(mnt))
            for vol in volumes:
                if vol.status != 'available':
                    vol.detach(force=True)
                _wait_for(vol, 'available')
                logger.info('Deleting {vol} in {vol.region}.'.format(vol=vol))
                vol.delete()

    if inst:
        with _attach_snap_to_inst(inst, snap) as (vol, mountpoint):
            yield vol, mountpoint
    else:
        with _create_temp_inst(snap.region, key_pair=key_pair,
                               security_groups=security_groups) as inst:
            with _attach_snap_to_inst(inst, snap) as (vol, mountpoint):
                yield vol, mountpoint


def _get_vol_dev(vol):
    """Return volume representation as attached OS device."""
    if not vol.attach_data.instance_id:
        return
    inst = _get_inst_by_id(vol.region, vol.attach_data.instance_id)
    if not inst.public_dns_name:    # The instance is down.
        return
    key_filename = config.get(vol.region.name, 'key_filename')
    env.update({'host_string': inst.public_dns_name,
                'key_filename': key_filename})
    attached_dev = vol.attach_data.device
    natty_dev = attached_dev.replace('sd', 'xvd')
    logger.debug(env, output)
    for dev in [attached_dev, natty_dev]:
        if _wait_for_exists(dev):
            return dev


@_contextmanager
def _config_temp_ssh(conn):
    config_name = '{region}-temp-ssh-{now}'.format(
        region=conn.region.name, now=datetime.utcnow().isoformat())
    key_pair = conn.create_key_pair(config_name)
    key_filename = key_pair.name + '.pem'
    key_pair.save('./')
    _chmod(key_filename, 0600)
    try:
        yield _realpath(key_filename)
    finally:
        key_pair.delete()
        _remove(key_filename)


def mount_snapshot(region_name=None, snap_id=None, inst_id=None):

    """Mount snapshot to temporary created instance or `inst_id`.

    region_name
        snapshot location
    snap_id
    inst_id
        attach to existing instance. Will be created temporary if
        None."""

    if not region_name or not snap_id:
        region_name, snap_id = _select_snapshot()
    region = _get_region_by_name(region_name)
    conn = region.connect()
    inst = _get_inst_by_id(region, inst_id) if inst_id else None
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id, ])[0]

    info = ('\nYou may now SSH into the {inst} server, using:'
            '\n ssh -i {key} {user}@{inst.public_dns_name}')
    with _attach_snapshot(snap, inst) as (vol, mountpoint):
        if mountpoint:
            info += ('\nand browse snapshot, mounted at {mountpoint}.')
        else:
            info += ('\nand mount {device}. NOTE: device name may be '
                     'altered by system.')
        key_file = config.get(region.name, 'key_filename')
        inst = _get_inst_by_id(region, vol.attach_data.instance_id)
        logger.info(info.format(inst=inst, user=env.user, key=key_file,
            device=vol.attach_data.device, mountpoint=mountpoint))

        info = ('\nEnter FINISHED if you are finished looking at the '
                'backup and would like to cleanup: ')
        while raw_input(info).strip() != 'FINISHED':
            pass


def _rsync_mountpoints(src_inst, src_mnt, dst_inst, dst_mnt):
    """Run `rsync` against mountpoints."""
    src_key_filename = config.get(src_inst.region.name, 'key_filename')
    dst_key_filename = config.get(dst_inst.region.name, 'key_filename')
    with _config_temp_ssh(dst_inst.connection) as key_file:
        with settings(host_string=dst_inst.public_dns_name,
                      key_filename=dst_key_filename):
            _wait_for_sudo('cp /root/.ssh/authorized_keys '
                           '/root/.ssh/authorized_keys.bak')
            pub_key = local('ssh-keygen -y -f {0}'.format(key_file), True)
            append('/root/.ssh/authorized_keys', pub_key, use_sudo=True)
        with settings(host_string=src_inst.public_dns_name,
                      key_filename=src_key_filename):
            put(key_file, '.ssh/', mirror_local_mode=True)
            dst_key_filename = _split(key_file)[1]
            cmd = (
                'rsync -e "ssh -i .ssh/{key_file} -o StrictHostKeyChecking=no"'
                ' -aHAXz --delete --exclude /root/.bash_history '
                '--exclude /home/*/.bash_history --exclude /etc/ssh/moduli '
                '--exclude /etc/ssh/ssh_host_* '
                '--exclude /etc/udev/rules.d/*persistent-net.rules '
                '--exclude /var/lib/ec2/* --exclude=/mnt/* --exclude=/proc/* '
                '--exclude=/tmp/* {src_mnt}/ root@{rhost}:{dst_mnt}')
            _wait_for_sudo(cmd.format(
                rhost=dst_inst.public_dns_name, dst_mnt=dst_mnt,
                key_file=dst_key_filename, src_mnt=src_mnt))
        with settings(host_string=dst_inst.public_dns_name,
                      key_filename=dst_key_filename):
            _wait_for_sudo('mv /root/.ssh/authorized_keys.bak '
                           '/root/.ssh/authorized_keys')


def _update_snap(src_vol, src_mnt, dst_vol, dst_mnt, delete_old=False):

    """Update destination region from `src_vol`.

    Create new snapshot with same description and tags. Delete previous
    snapshot (if exists) of the same volume in destination region."""

    src_inst = _get_inst_by_id(src_vol.region, src_vol.attach_data.instance_id)
    dst_inst = _get_inst_by_id(dst_vol.region, dst_vol.attach_data.instance_id)
    _rsync_mountpoints(src_inst, src_mnt, dst_inst, dst_mnt)
    if dst_vol.snapshot_id:
        old_snap = dst_vol.connection.get_all_snapshots(
            [dst_vol.snapshot_id])[0]
    else:
        old_snap = None
    src_snap = src_vol.connection.get_all_snapshots([src_vol.snapshot_id])[0]
    _create_snapshot(dst_vol, tags=src_snap.tags, synchronously=False)
    if old_snap and delete_old:
        logger.info('Deleting previous {0} in {1}'.format(old_snap,
                                                          dst_vol.region))
        old_snap.delete()


def _create_empty_snapshot(region, size):
    """Format new filesystem."""
    with _create_temp_inst(region) as inst:
        vol = region.connect().create_volume(size, inst.placement)
        earmarking_tag = config.get(region.name, 'tag_name')
        vol.add_tag(earmarking_tag, 'temporary')
        vol.attach(inst.id, _get_avail_dev(inst))
        _mount_volume(vol, mkfs=True)
        snap = vol.create_snapshot()
        snap.add_tag(earmarking_tag, 'temporary')
        vol.detach(True)
        _wait_for(vol, 'available')
        vol.delete()
        return snap


def rsync_snapshot(src_region_name, snapshot_id, dst_region_name,
                   src_inst=None, dst_inst=None):

    """Duplicate the snapshot into dst_region.

    src_region_name, dst_region_name
        Amazon region names. Allowed to be contracted, e.g.
        `ap-southeast-1` will be recognized in `ap-south` or even
        `ap-s`;
    snapshot_id
        snapshot to duplicate;
    src_inst, dst_inst
        will be used instead of creating new for temporary."""

    src_conn = _get_region_by_name(src_region_name).connect()
    src_snap = src_conn.get_all_snapshots([snapshot_id])[0]
    dst_conn = _get_region_by_name(dst_region_name).connect()

    info = 'Going to transmit {snap.volume_size} GiB {snap} {snap.description}'
    if src_snap.tags.get('Name'):
        info += ' of {name}'
    info += ' from {snap.region} to {dst}'
    logger.info(info.format(snap=src_snap, dst=dst_conn.region,
                            name=src_snap.tags.get('Name')))

    dst_snaps = dst_conn.get_all_snapshots(owner='self')
    dst_snaps = [snp for snp in dst_snaps if not snp.status == 'error']
    src_vol = _get_snap_vol(src_snap)
    vol_snaps = [snp for snp in dst_snaps if _get_snap_vol(snp) == src_vol]
    if vol_snaps:
        dst_snap = sorted(vol_snaps, key=_get_snap_time)[-1]
        if _get_snap_time(dst_snap) >= _get_snap_time(src_snap):
            kwargs = dict(src=src_snap, dst=dst_snap, dst_reg=dst_conn.region)
            logger.info('Stepping over {src} - it\'s not newer than {dst} '
                        '{dst.description} in {dst_reg}'.format(**kwargs))
            return
    else:
        dst_snap = _create_empty_snapshot(dst_conn.region,
                                          src_snap.volume_size)

    with _nested(_attach_snapshot(src_snap, inst=src_inst),
                 _attach_snapshot(dst_snap, inst=dst_inst)) as (
        (src_vol, src_mnt), (dst_vol, dst_mnt)):
        _update_snap(src_vol, src_mnt, dst_vol, dst_mnt)


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
    dst_region = _get_region_by_name(dst_region_name)
    conn = src_region.connect()
    tag_name = tag_name or config.get(src_region.name, 'tag_name')
    tag_value = tag_value or config.get(src_region.name, 'tag_value')
    filters = {'tag-key': tag_name, 'tag-value': tag_value}
    snaps = conn.get_all_snapshots(owner='self', filters=filters)
    snaps = [snp for snp in snaps if not snp.status == 'error']
    _is_described = lambda snap: _get_snap_vol(snap) and _get_snap_time(snap)
    snaps = [snp for snp in snaps if _is_described(snp)]
    if native_only:

        def _is_native(snap, region):
            return _get_descr_attr(snap, 'Region') == region.name
        snaps = [snp for snp in snaps if _is_native(snp, src_region)]

    with _nested(_create_temp_inst(src_region),
                 _create_temp_inst(dst_region)) as (src_inst, dst_inst):
        snaps = sorted(snaps, key=_get_snap_vol)    # Prepare for grouping.
        for vol, vol_snaps in _groupby(snaps, _get_snap_vol):
            latest_snap = sorted(vol_snaps, key=_get_snap_time)[-1]
            for inst in src_inst, dst_inst:
                logger.debug('Rebooting {0} in {0.region} '
                             'to refresh attachments'.format(inst))
                inst.reboot()
            args = (src_region_name, latest_snap.id, dst_region_name, src_inst,
                    dst_inst)
            try:
                rsync_snapshot(*args)
            except:
                logger.exception('rsync of {1} from {0} to {2} failed'.format(
                    *args))


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
    _wait_for(image, 'available')
    reservation = image.run(
        key_name = config.get(conn.region.name, 'key_pair'),
        security_groups = [_security_groups, ],
        instance_type = inst_type,
        # XXX Kernel workaround, not tested with natty
        kernel_id=config.get(conn.region.name, 'kernel' + image.architecture))
    new_instance = reservation.instances[0]
    _wait_for(new_instance, 'running')
    _add_tags(new_instance, image.tags)
    modify_instance_termination(conn.region.name, new_instance.id)
    info = ('\nYou may now SSH into the {inst} server, using:'
            '\n ssh -i {key} {user}@{inst.public_dns_name}')
    key_file = config.get(conn.region.name, 'key_filename')
    logger.info(info.format(inst=new_instance, user=env.user, key=key_file))


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
    ebs.delete_on_termination = True
    block_map = _BlockDeviceMapping()
    block_map[root_dev] = ebs

    name = 'Created {0} using access key {1}'.format(_now(), conn.access_key)
    name = name.replace(":", ".").replace(" ", "_")

    # create the new AMI all options from snap JSON description:
    _wait_for(snap, '100%', limit=10 * 60)
    result = conn.register_image(
        name=name,
        description=snap.description,
        architecture=_get_descr_attr(snap, 'Arch') or inst_arch,
        root_device_name=_get_descr_attr(snap, 'Root_dev_name') or root_dev,
        block_device_map=block_map)
    image = conn.get_all_images(image_ids=[result, ])[0]
    _add_tags(image, snap.tags)

    logger.info('The new AMI ID = {0}'.format(result))

    info = ('\nEnter RUN if you want to launch instance using '
            'just created {0}: '.format(image))
    if force == 'RUN' or raw_input(info).strip() == 'RUN':
        launch_instance_from_ami(region, image.id, inst_type=inst_type)


def modify_kernel(region, instance_id):
    """
    Modify old kernel for stopped instance
    (needed for make pv-grub working)
    NOTICE: install grub-legacy-ec2 and upgrades before run this.
    region
        specify instance region;
    instance_id
        specify instance id for kernel change
    Kernels list:
        ap-southeast-1      x86_64  aki-11d5aa43
        ap-southeast-1  i386    aki-13d5aa41
        eu-west-1       x86_64  aki-4feec43b
        eu-west-1       i386    aki-4deec439
        us-east-1       x86_64  aki-427d952b
        us-east-1       i386    aki-407d9529
        us-west-1       x86_64  aki-9ba0f1de
        us-west-1       i386    aki-99a0f1dc
    """
    key_filename = config.get(region, 'key_filename')
    region = _get_region_by_name(region)
    instance = _get_inst_by_id(region, instance_id)
    env.update({
        'host_string': instance.public_dns_name,
        'key_filename': key_filename,
    })
    sudo('env DEBIAN_FRONTEND=noninteractive apt-get update && '
         'sudo env DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade && '
         'env DEBIAN_FRONTEND=noninteractive apt-get install grub-legacy-ec2')
    kernel = config.get(region.name, 'kernel'+instance.architecture)
    instance.stop()
    _wait_for(instance, 'stopped')
    instance.modify_attribute('kernel', kernel)
    instance.start()

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
import os.path
import re
import sys
from datetime import timedelta, datetime
from ConfigParser import SafeConfigParser, NoOptionError
from contextlib import contextmanager, nested
from itertools import groupby
from json import dumps, loads
from pprint import PrettyPrinter
from pydoc import pager
from string import lowercase
from time import sleep
from traceback import format_exc
from warnings import warn

from boto.ec2 import connect_to_region, regions
from boto.ec2.blockdevicemapping import BlockDeviceMapping, EBSBlockDeviceType
from boto.exception import BotoServerError, EC2ResponseError
from fabric.api import env, local, output, prompt, put, settings, sudo, task
from fabric.context_managers import hide
from fabric.contrib.files import append, exists

from django_fabfile.utils import get_region_by_name


config_file = 'fabfile.cfg'
config = SafeConfigParser()
config.read(config_file)
try:
    username = config.get('DEFAULT', 'username')
    debug = config.getboolean('DEFAULT', 'debug')
    logging_folder = config.get('DEFAULT', 'logging_folder')
except NoOptionError:
    username = 'ubuntu'
    debug = logging_folder = False

env.update({'user': username, 'disable_known_hosts': True})

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


def prompt_to_select(choices, query='Select from', paging=False,
                     multiple=False):
    """Prompt to select an option from provided choices.

    choices
        list or dict. If dict, then choice will be made among keys.
    paging
        render long list with pagination.

    Return solely possible value instantly without prompting."""
    keys = list(choices)
    while keys.count(None):
        keys.pop(choices.index(None))    # Remove empty values.
    assert len(keys), 'No choices provided'
    if len(keys) == 1:
        return keys[0]

    def in_list(input_, avail_list, multiple=False):
        selected_list = re.split('[\s,]+', input_)
        if not multiple:
            assert len(selected_list) == 1, 'Only one item allowed'
        for item in selected_list:
            if not item in avail_list:
                raise ValueError('{0} not in {1}'.format(item, avail_list))
        return selected_list if multiple else selected_list[0]

    if paging:
        pp = PrettyPrinter()
        pager(query + '\n' + pp.pformat(choices))
        text = 'Enter your choice or press Return to view options again'
    else:
        text = '{query} {choices}'.format(query=query, choices=choices)
    input_in_list = lambda input_: in_list(input_, choices, multiple)
    return prompt(text, validate=input_in_list)


class StateNotChangedError(Exception):

    def __init__(self, state):
        self.state = state

    def __str__(self):
        return 'State remain {0} after limited time gone'.format(self.state)


def wait_for(obj, state, attrs=None, max_sleep=30, limit=5 * 60):
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
            sleep(sleep_for)
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
                    logger.debug(format_exc())
                    logger.error(repr(err))

                    if attempts > 0:
                        logger.info('waiting next {0} sec ({1} times left)'
                            .format(self.pause, attempts))
                        sleep(self.pause)
                else:
                    break
        return wrapper

try:
    ssh_timeout_attempts = config.getint('DEFAULT', 'ssh_timeout_attempts')
    ssh_timeout_interval = config.getint('DEFAULT', 'ssh_timeout_interval')
except NoOptionError as err:
    warn(str(err))
else:
    wait_for_sudo = _WaitForProper(attempts=ssh_timeout_attempts,
                                    pause=ssh_timeout_interval)(sudo)
    wait_for_exists = _WaitForProper(attempts=ssh_timeout_attempts,
                                      pause=ssh_timeout_interval)(exists)


def add_tags(res, tags):
    for tag in tags:
        if tags[tag]:
            res.add_tag(tag, tags[tag])
    logger.debug('Tags added to {0}'.format(res))


def get_descr_attr(resource, attr):
    try:
        return loads(resource.description)[attr]
    except:
        pass


def get_snap_vol(snap):
    return get_descr_attr(snap, 'Volume') or snap.volume_id


def get_snap_instance(snap):
    return get_descr_attr(snap, 'Instance')


def get_snap_device(snap):
    return get_descr_attr(snap, 'Device')


def get_snap_time(snap):
    for format_ in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f'):
        try:
            return datetime.strptime(get_descr_attr(snap, 'Time'), format_)
        except (TypeError, ValueError):
            continue
    # Use attribute if can't parse description.
    return datetime.strptime(snap.start_time, '%Y-%m-%dT%H:%M:%S.000Z')


def get_inst_by_id(region, instance_id):
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


def get_all_instances(region=None, id_only=False):
    if not region:
        warn('There is no guarantee of instance id uniqueness across regions')
    reg_names = [region] if region else (reg.name for reg in regions())
    connections = (connect_to_region(reg) for reg in reg_names)
    for con in connections:
        for res in con.get_all_instances():
            for inst in res.instances:
                yield inst.id if id_only else inst


def get_all_snapshots(region=None, id_only=False):
    if not region:
        warn('There is no guarantee of snapshot id uniqueness across regions')
    reg_names = [region] if region else (reg.name for reg in regions())
    connections = (connect_to_region(reg) for reg in reg_names)
    for con in connections:
        for snap in con.get_all_snapshots(owner='self'):
            yield snap.id if id_only else snap


@task
def update_volumes_tags(filters=None):
    """Clone tags from instances to volumes.

    filters
        apply optional filtering for the `get_all_instances`."""
    for region in regions():
        reservations = region.connect().get_all_instances(filters=filters)
        for res in reservations:
            inst = res.instances[0]
            for bdm in inst.block_device_mapping.keys():
                vol_id = inst.block_device_mapping[bdm].volume_id
                vol = inst.connection.get_all_volumes([vol_id])[0]
                add_tags(vol, inst.tags)


@task
def modify_instance_termination(region, instance_id):
    """Mark production instnaces as uneligible for termination.

    region
        name of region where instance is located;
    instance_id
        instance to be updated;

    You must change value of preconfigured tag_name and run this command
    before terminating production instance via API."""
    conn = get_region_by_name(region).connect()
    inst = get_inst_by_id(conn.region, instance_id)
    prod_tag = config.get(conn.region.name, 'tag_name')
    prod_val = config.get(conn.region.name, 'tag_value')
    inst_tag_val = inst.tags.get(prod_tag)
    inst.modify_attribute('disableApiTermination', inst_tag_val == prod_val)


def select_snapshot():
    region_name = prompt_to_select([reg.name for reg in regions()],
                                   'Select region from')
    snap_id = prompt('Please enter snapshot ID if it\'s known (press Return '
                     'otherwise)')
    if snap_id:
        if snap_id in get_all_snapshots(region_name, id_only=True):
            return region_name, snap_id
        else:
            logger.info('No snapshot with provided ID found')

    instances_list = list(get_all_instances(region_name))
    instances = dict((inst.id, {
        'Name': inst.tags.get('Name'),
        'State': inst.state,
        'Launched': inst.launch_time,
        'Key pair': inst.key_name,
        'Type': inst.instance_type,
        'IP Address': inst.ip_address,
        'DNS Name': inst.public_dns_name}) for inst in instances_list)
    instance_id = prompt_to_select(instances, 'Select instance ID from',
                                   paging=True)

    all_instances = get_all_instances(region_name)
    inst = [inst for inst in all_instances if inst.id == instance_id][0]
    volumes = dict((dev.volume_id, {
        'Status': dev.status,
        'Attached': dev.attach_time,
        'Size': dev.size,
        'Snapshot ID': dev.snapshot_id}) for dev in
                                            inst.block_device_mapping.values())
    volume_id = prompt_to_select(volumes, 'Select volume ID from',
                                 paging=True)

    all_snaps = get_all_snapshots(region_name)
    snaps_list = (snap for snap in all_snaps if snap.volume_id == volume_id)
    snaps = dict((snap.id, {'Volume': snap.volume_id,
                            'Date': snap.start_time,
                            'Description': snap.description}) for snap in
                                                                    snaps_list)
    return region_name, prompt_to_select(snaps, 'Select snapshot ID from',
                                         paging=True)


def create_snapshot(vol, description='', tags=None, synchronously=True):
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
    if vol.attach_data:
        inst = get_inst_by_id(vol.region, vol.attach_data.instance_id)
    else:
        inst = None
    if not description and inst:
        description = dumps({
            'Volume': vol.id,
            'Region': vol.region.name,
            'Device': vol.attach_data.device,
            'Instance': inst.id,
            'Type': inst.instance_type,
            'Arch': inst.architecture,
            'Root_dev_name': inst.root_device_name,
            'Time': _now(),
            })

    def initiate_snapshot():
        snapshot = vol.create_snapshot(description)
        if tags:
            add_tags(snapshot, tags)
        else:
            if inst:
                add_tags(snapshot, inst.tags)
            add_tags(snapshot, vol.tags)
        logger.info('{0} initiated from {1}'.format(snapshot, vol))
        return snapshot

    if synchronously:
        timeout = config.getint('DEFAULT', 'minutes_for_snap')
        while True:     # Iterate unless success and delete failed snapshots.
            snapshot = initiate_snapshot()
            try:
                wait_for(snapshot, '100%', limit=timeout * 60)
                assert snapshot.status == 'completed', (
                    'completed with wrong status {0}'.format(snapshot.status))
            except (StateNotChangedError, AssertionError) as err:
                logger.error(str(err) + ' - deleting')
                snapshot.delete()
            else:
                break
    else:
        snapshot = initiate_snapshot()
    return snapshot


@task
def backup_instance(region_name, instance_id=None, instance=None,
                    synchronously=False):
    """Return list of created snapshots for specified instance.

    region_name
        instance location;
    instance, instance_id
        either `instance_id` or `instance` argument should be specified;
    synchronously
        wait for completion. False by default."""
    assert bool(instance_id) ^ bool(instance), ('Either instance_id or '
        'instance should be specified')
    region = get_region_by_name(region_name)
    if instance_id:
        instance = get_inst_by_id(region, instance_id)
    snapshots = []
    for dev in instance.block_device_mapping:
        vol_id = instance.block_device_mapping[dev].volume_id
        vol = region.connect().get_all_volumes([vol_id])[0]
        snapshots.append(create_snapshot(vol, synchronously=synchronously))
    return snapshots


@task
def backup_instances_by_tag(region_name=None, tag_name=None, tag_value=None,
                            synchronously=False):
    """Creates backup for all instances with given tag in region.

    region_name
        will be applied across all regions by default;
    tag_name, tag_value
        will be fetched from config by default, may be configured
        per region;
    synchronously
        will be accomplished without checking results. False by default.
        NOTE: when ``create_ami`` task compiles AMI from several
        snapshots it restricts snapshot start_time difference with 10
        minutes interval at most. Snapshot completion may take much more
        time and due to this only asynchronously generated snapshots
        will be assembled assurely."""
    snapshots = []
    region = get_region_by_name(region_name) if region_name else None
    reg_names = [region.name] if region else (reg.name for reg in regions())
    for reg in reg_names:
        tag_name = tag_name or config.get(reg, 'tag_name')
        tag_value = tag_value or config.get(reg, 'tag_value')
        conn = connect_to_region(reg)
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

    conn = get_region_by_name(region_name).connect()
    # work with UTC time, which is what the snapshot start time is reported in
    now = datetime.utcnow()
    last_hour = datetime(now.year, now.month, now.day, now.hour)
    last_midnight = datetime(now.year, now.month, now.day)
    last_sunday = datetime(now.year, now.month,
          now.day) - timedelta(days=(now.weekday() + 1) % 7)
    last_month = datetime(now.year, now.month - 1, now.day)
    last_year = datetime(now.year - 1, now.month, now.day)
    other_years = datetime(now.year - 2, now.month, now.day)
    start_of_month = datetime(now.year, now.month, 1)

    target_backup_times = []
    # there are no snapshots older than 1/1/2000
    oldest_snapshot_date = datetime(2000, 1, 1)

    for hour in range(0, hourly_backups):
        target_backup_times.append(last_hour - timedelta(hours=hour))

    for day in range(0, daily_backups):
        target_backup_times.append(last_midnight - timedelta(days=day))

    for week in range(0, weekly_backups):
        target_backup_times.append(last_sunday - timedelta(weeks=week))

    for month in range(0, monthly_backups):
        target_backup_times.append(last_month - timedelta(weeks=month * 4))

    for quart in range(0, quarterly_backups):
        target_backup_times.append(last_year - timedelta(weeks=quart * 16))

    for year in range(0, yearly_backups):
        target_backup_times.append(other_years - timedelta(days=year * 365))

    one_day = timedelta(days=1)
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
    target_backup_times.reverse()  # make the oldest date first

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
        volume_name = get_snap_vol(snap)

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

        time_period_num = 0
        snap_found_for_this_time_period = False
        for snap in snaps:
            check_this_snap = True

            while (check_this_snap and
                   time_period_num < target_backup_times.__len__()):

                if get_snap_time(snap) < target_backup_times[time_period_num]:
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
                                except EC2ResponseError as err:
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
                    time_period_num += 1
                    snap_found_for_this_time_period = False


@task
def delete_broken_snapshots():
    """Delete snapshots with status 'error'."""
    for region in regions():
        conn = region.connect()
        filters = {'status': 'error'}
        snaps = conn.get_all_snapshots(owner='self', filters=filters)
        for snp in snaps:
            logger.info('Deleting broken {0}'.format(snp))
            snp.delete()


@task
def trim_snapshots(region_name=None, dry_run=False):
    """Delete old snapshots logarithmically back in time.

    region_name
        by default process all regions;
    dry_run
        boolean, only print info about old snapshots to be deleted."""
    delete_broken_snapshots()
    region = get_region_by_name(region_name) if region_name else None
    reg_names = [region.name] if region else (reg.name for reg in regions())
    for reg in reg_names:
        logger.info('Processing {0}'.format(reg))
        _trim_snapshots(reg, dry_run=dry_run)


@task
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

    region = get_region_by_name(region_name)
    conn = region.connect()

    ami_ptrn = config.get('DEFAULT', 'ami_ptrn')
    architecture = config.get('DEFAULT', 'architecture')
    ubuntu_aws_account = config.get('DEFAULT', 'ubuntu_aws_account')
    filters = {'owner_id': ubuntu_aws_account, 'architecture': architecture,
             'name': ami_ptrn, 'image_type': 'machine',
             'root_device_type': 'ebs'}
    images = conn.get_all_images(filters=filters)

    # Filtering by latest version.
    ptrn = re.compile(config.get('DEFAULT', 'ami_regexp'))
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
    wait_for(inst, 'running')
    logger.info('{inst} created in {zone}'.format(inst=inst, zone=zone))

    return inst


@contextmanager
def create_temp_inst(region=None, zone=None, key_pair=None,
                      security_groups=None, synchronously=False):
    if region and zone:
        assert zone in region.connect().get_all_zones(), (
            '{0} doesn\'t belong to {1}'.format(zone, region))

    def create_inst_in_zone(zone, key_pair, sec_grps):
        inst = create_instance(zone.region.name, zone.name, key_pair=key_pair,
                               security_groups=sec_grps)
        inst.add_tag(config.get(zone.region.name, 'tag_name'), 'temporary')
        return inst

    if zone:
        inst = create_inst_in_zone(zone, key_pair, security_groups)
    else:
        for zone in region.connect().get_all_zones():
            try:
                inst = create_inst_in_zone(zone, key_pair, security_groups)
            except BotoServerError as err:
                logging.debug(format_exc())
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
            wait_for(inst, 'terminated')


def get_avail_dev(inst):
    """Return next unused device name."""
    inst.update()
    chars = lowercase
    for dev in inst.block_device_mapping:
        chars = chars.replace(dev[-2], '')
    return '/dev/sd{0}1'.format(chars[0])


def get_avail_dev_encr(instance):
    """Return next unused device name."""
    chars = lowercase
    for dev in instance.block_device_mapping:
        chars = chars.replace(dev[-2], '')
    return '/dev/sd{0}'.format(chars[1])


def get_vol_dev(vol):
    """Return volume representation as attached OS device."""
    if not vol.attach_data.instance_id:
        return
    inst = get_inst_by_id(vol.region, vol.attach_data.instance_id)
    if not inst.public_dns_name:    # The instance is down.
        return
    key_filename = config.get(vol.region.name, 'key_filename')
    env.update({'host_string': inst.public_dns_name,
                'key_filename': key_filename})
    attached_dev = vol.attach_data.device
    natty_dev = attached_dev.replace('sd', 'xvd')
    logger.debug(env, output)
    for dev in [attached_dev, natty_dev]:
        if wait_for_exists(dev):
            return dev


def mount_volume(vol, mkfs=False, mkrootfs=False):

    """Mount the device by SSH. Return mountpoint on success.

    vol
        volume to be mounted on the instance it is attached to."""

    vol.update()
    assert vol.attach_data
    inst = get_inst_by_id(vol.region, vol.attach_data.instance_id)
    key_filename = config.get(vol.region.name, 'key_filename')
    with settings(host_string=inst.public_dns_name, key_filename=key_filename):
        dev = get_vol_dev(vol)
        mountpoint = dev.replace('/dev/', '/media/')
        wait_for_sudo('mkdir -p {0}'.format(mountpoint))
        if mkfs:
            wait_for_sudo('mkfs.ext3 {dev}'.format(dev=dev))
        """Add disk label for normal boot on created volume"""
        logger.debug('mkrootfs value in mount_volume {0}'.format(mkrootfs))
        
        if mkrootfs:
            wait_for_sudo('e2label {dev} uec-rootfs'.format(dev=dev))
            
        wait_for_sudo('mount {dev} {mnt}'.format(dev=dev, mnt=mountpoint))
        if mkfs:
            wait_for_sudo('chown -R {user}:{user} {mnt}'.format(
                           user=env.user, mnt=mountpoint))
    logger.debug('Mounted {0} to {1} at {2}'.format(vol, inst, mountpoint))
    return mountpoint


@contextmanager
def attach_snapshot(snap, key_pair=None, security_groups=None, inst=None,
                    mkrootfs=False):

    """Attach `snap` to `inst` or to new temporary instance.

    Yield volume, created from the `snap` and its mountpoint.

    Create temporary instance if `inst` not provided. Provide access to
    newly created temporary instance for `key_pair` and with
    `security_groups`."""

    wait_for(snap, '100%', limit=10 * 60)

    def force_snap_attach(inst, snap):
        """Iterate over devices until successful attachment."""
        volumes_to_delete = []
        while get_avail_dev(inst):
            vol = inst.connection.create_volume(snap.volume_size,
                                                inst.placement, snap)
            add_tags(vol, snap.tags)
            vol.add_tag(config.get(inst.region.name, 'tag_name'), 'temporary')
            volumes_to_delete.append(vol)
            dev_name = get_avail_dev(inst)
            logger.debug('Got avail {0} from {1}'.format(dev_name, inst))
            vol.attach(inst.id, dev_name)
            try:
                wait_for(vol, 'attached', ['attach_data', 'status'], limit=60)
            except StateNotChangedError:
                logger.error('Attempt to attach as next device')
            else:
                break
        return vol, volumes_to_delete

    @contextmanager
    def attach_snap_to_inst(inst, snap):
        """Cleanup volume(s)."""
        wait_for(inst, 'running')
        try:
            vol, volumes = force_snap_attach(inst, snap)
            logger.debug('mkrootfs value {0}'.format(mkrootfs))
            mnt = mount_volume(vol, mkrootfs=mkrootfs)
            yield vol, mnt
        except BaseException as err:
            logger.exception(str(err))
        finally:
            key_filename = config.get(inst.region.name, 'key_filename')
            with settings(host_string=inst.public_dns_name,
                          key_filename=key_filename):
                wait_for_sudo('umount {0}'.format(mnt))
            for vol in volumes:
                if vol.status != 'available':
                    vol.detach(force=True)
                wait_for(vol, 'available')
                logger.info('Deleting {vol} in {vol.region}.'.format(vol=vol))
                vol.delete()

    if inst:
        with attach_snap_to_inst(inst, snap) as (vol, mountpoint):
            yield vol, mountpoint
    else:
        with create_temp_inst(snap.region, key_pair=key_pair,
                              security_groups=security_groups) as inst:
            with attach_snap_to_inst(inst, snap) as (vol, mountpoint):
                yield vol, mountpoint


@contextmanager
def config_temp_ssh(conn):
    config_name = '{region}-temp-ssh-{now}'.format(
        region=conn.region.name, now=datetime.utcnow().isoformat())
    key_pair = conn.create_key_pair(config_name)
    key_filename = key_pair.name + '.pem'
    key_pair.save('./')
    os.chmod(key_filename, 0600)
    try:
        yield os.path.realpath(key_filename)
    finally:
        key_pair.delete()
        os.remove(key_filename)


@task
def mount_snapshot(region_name=None, snap_id=None, inst_id=None):

    """Mount snapshot to temporary created instance or `inst_id`.

    region_name
        snapshot location
    snap_id
    inst_id
        attach to existing instance. Will be created temporary if
        None."""

    if not region_name or not snap_id:
        region_name, snap_id = select_snapshot()
    region = get_region_by_name(region_name)
    conn = region.connect()
    inst = get_inst_by_id(region, inst_id) if inst_id else None
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id, ])[0]

    info = ('\nYou may now SSH into the {inst} server, using:'
            '\n ssh -i {key} {user}@{inst.public_dns_name}')
    with attach_snapshot(snap, inst) as (vol, mountpoint):
        if mountpoint:
            info += ('\nand browse snapshot, mounted at {mountpoint}.')
        else:
            info += ('\nand mount {device}. NOTE: device name may be '
                     'altered by system.')
        key_file = config.get(region.name, 'key_filename')
        inst = get_inst_by_id(region, vol.attach_data.instance_id)
        logger.info(info.format(inst=inst, user=env.user, key=key_file,
            device=vol.attach_data.device, mountpoint=mountpoint))

        info = ('\nEnter FINISHED if you are finished looking at the '
                'backup and would like to cleanup: ')
        while raw_input(info).strip() != 'FINISHED':
            pass


@task
def rsync_mountpoints(src_inst, src_mnt, dst_inst, dst_mnt):
    """Run `rsync` against mountpoints."""
    src_key_filename = config.get(src_inst.region.name, 'key_filename')
    dst_key_filename = config.get(dst_inst.region.name, 'key_filename')
    with config_temp_ssh(dst_inst.connection) as key_file:
        with settings(host_string=dst_inst.public_dns_name,
                      key_filename=dst_key_filename):
            wait_for_sudo('cp /root/.ssh/authorized_keys '
                          '/root/.ssh/authorized_keys.bak')
            pub_key = local('ssh-keygen -y -f {0}'.format(key_file), True)
            append('/root/.ssh/authorized_keys', pub_key, use_sudo=True)
        with settings(host_string=src_inst.public_dns_name,
                      key_filename=src_key_filename):
            put(key_file, '.ssh/', mirror_local_mode=True)
            dst_key_filename = os.path.split(key_file)[1]
            cmd = (
                'rsync -e "ssh -i .ssh/{key_file} -o StrictHostKeyChecking=no"'
                ' -aHAXz --delete --exclude /root/.bash_history '
                '--exclude /home/*/.bash_history --exclude /etc/ssh/moduli '
                '--exclude /etc/ssh/ssh_host_* '
                '--exclude /etc/udev/rules.d/*persistent-net.rules '
                '--exclude /var/lib/ec2/* --exclude=/mnt/* --exclude=/proc/* '
                '--exclude=/tmp/* {src_mnt}/ root@{rhost}:{dst_mnt}')
            wait_for_sudo(cmd.format(
                rhost=dst_inst.public_dns_name, dst_mnt=dst_mnt,
                key_file=dst_key_filename, src_mnt=src_mnt))
        with settings(host_string=dst_inst.public_dns_name,
                      key_filename=dst_key_filename):
            wait_for_sudo('mv /root/.ssh/authorized_keys.bak '
                          '/root/.ssh/authorized_keys')


def update_snap(src_vol, src_mnt, dst_vol, dst_mnt, delete_old=False):

    """Update destination region from `src_vol`.

    Create new snapshot with same description and tags. Delete previous
    snapshot (if exists) of the same volume in destination region if
    ``delete_old`` is True."""

    src_inst = get_inst_by_id(src_vol.region, src_vol.attach_data.instance_id)
    dst_inst = get_inst_by_id(dst_vol.region, dst_vol.attach_data.instance_id)
    rsync_mountpoints(src_inst, src_mnt, dst_inst, dst_mnt)
    if dst_vol.snapshot_id:
        old_snap = dst_vol.connection.get_all_snapshots(
            [dst_vol.snapshot_id])[0]
    else:
        old_snap = None
    src_snap = src_vol.connection.get_all_snapshots([src_vol.snapshot_id])[0]
    create_snapshot(dst_vol, description=src_snap.description,
                                    tags=src_snap.tags, synchronously=False)
    if old_snap and delete_old:
        logger.info('Deleting previous {0} in {1}'.format(old_snap,
                                                          dst_vol.region))
        old_snap.delete()


def create_empty_snapshot(region, size, mkrootfs=False):
    """Format new filesystem."""
    with create_temp_inst(region) as inst:
        vol = region.connect().create_volume(size, inst.placement)
        earmarking_tag = config.get(region.name, 'tag_name')
        vol.add_tag(earmarking_tag, 'temporary')
        vol.attach(inst.id, get_avail_dev(inst))
        mount_volume(vol, mkfs=True, mkrootfs=mkrootfs)
        snap = vol.create_snapshot()
        snap.add_tag(earmarking_tag, 'temporary')
        vol.detach(True)
        wait_for(vol, 'available')
        vol.delete()
        return snap


@task
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

    src_conn = get_region_by_name(src_region_name).connect()
    src_snap = src_conn.get_all_snapshots([snapshot_id])[0]
    dst_conn = get_region_by_name(dst_region_name).connect()

    info = 'Going to transmit {snap.volume_size} GiB {snap} {snap.description}'
    if src_snap.tags.get('Name'):
        info += ' of {name}'
    info += ' from {snap.region} to {dst}'
    logger.info(info.format(snap=src_snap, dst=dst_conn.region,
                            name=src_snap.tags.get('Name')))

    dst_snaps = dst_conn.get_all_snapshots(owner='self')
    dst_snaps = [snp for snp in dst_snaps if not snp.status == 'error']
    src_vol = get_snap_vol(src_snap)
    vol_snaps = [snp for snp in dst_snaps if get_snap_vol(snp) == src_vol]
    _device = get_snap_device(src_snap)
    logger.debug('device in rsync_snapshot {0}'.format(_device))
    mkrootfs = False
    try:
        _dev = re.match(r'^/dev/sda[0-9]$', _device)
    except:
        _dev = None
    if _dev:
        mkrootfs = True

    if vol_snaps:
        dst_snap = sorted(vol_snaps, key=get_snap_time)[-1]
        if get_snap_time(dst_snap) >= get_snap_time(src_snap):
            kwargs = dict(src=src_snap, dst=dst_snap, dst_reg=dst_conn.region)
            logger.info('Stepping over {src} - it\'s not newer than {dst} '
                        '{dst.description} in {dst_reg}'.format(**kwargs))
            return
    else:
        dst_snap = create_empty_snapshot(dst_conn.region,
                                         src_snap.volume_size, mkrootfs)

    with nested(attach_snapshot(src_snap, inst=src_inst, mkrootfs=mkrootfs),
                attach_snapshot(dst_snap, inst=dst_inst,
                mkrootfs=mkrootfs)) as (
        (src_vol, src_mnt), (dst_vol, dst_mnt)):
        update_snap(src_vol, src_mnt, dst_vol, dst_mnt,
                    delete_old=not vol_snaps)  # Delete only empty snapshots.


@task
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
    src_region = get_region_by_name(src_region_name)
    dst_region = get_region_by_name(dst_region_name)
    conn = src_region.connect()
    tag_name = tag_name or config.get(src_region.name, 'tag_name')
    tag_value = tag_value or config.get(src_region.name, 'tag_value')
    filters = {'tag-key': tag_name, 'tag-value': tag_value}
    snaps = conn.get_all_snapshots(owner='self', filters=filters)
    snaps = [snp for snp in snaps if not snp.status == 'error']
    _is_described = lambda snap: get_snap_vol(snap) and get_snap_time(snap)
    snaps = [snp for snp in snaps if _is_described(snp)]
    if native_only:

        def is_native(snap, region):
            return get_descr_attr(snap, 'Region') == region.name
        snaps = [snp for snp in snaps if is_native(snp, src_region)]

    with nested(create_temp_inst(src_region),
                create_temp_inst(dst_region)) as (src_inst, dst_inst):
        snaps = sorted(snaps, key=get_snap_vol)    # Prepare for grouping.
        for vol, vol_snaps in groupby(snaps, get_snap_vol):
            latest_snap = sorted(vol_snaps, key=get_snap_time)[-1]
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


@task
def launch_instance_from_ami(region_name, ami_id, inst_type=None):
    """Create instance from specified AMI.

    region_name
        location of the AMI and new instance;
    ami_id
        "ami-..."
    inst_type
        by default will be fetched from AMI description or used
        't1.micro' if not mentioned in the description."""
    try:
        user_data = config.get('user_data', 'user_data')
    except:
        user_data = None
    conn = get_region_by_name(region_name).connect()
    image = conn.get_all_images([ami_id])[0]
    inst_type = inst_type or get_descr_attr(image, 'Type') or 't1.micro'

    avail_grps = [sec.name for sec in conn.get_all_security_groups()]
    sec_grps = prompt_to_select(avail_grps, 'Select security groups',
                                multiple=True)
    wait_for(image, 'available')
    reservation = image.run(
        key_name=config.get(conn.region.name, 'key_pair'),
        security_groups=sec_grps,
        instance_type=inst_type,
        user_data=user_data)
    new_instance = reservation.instances[0]
    wait_for(new_instance, 'running')
    add_tags(new_instance, image.tags)
    modify_instance_termination(conn.region.name, new_instance.id)
    info = ('\nYou may now SSH into the {inst} server, using:'
            '\n ssh -i {key} {user}@{inst.public_dns_name}')
    key_file = config.get(conn.region.name, 'key_filename')
    logger.info(info.format(inst=new_instance, user=env.user, key=key_file))


@task
def create_ami(region=None, snap_id=None, force=None, root_dev='/dev/sda1',
               device=None, default_arch='x86_64', default_type='t1.micro'):
    """
    Creates AMI image from given snapshot.

    Force option removes prompt request and creates new instance from
    created ami image.
    region, snap_id
        specify snapshot to be processed. Snapshot description in json
        format will be used to restore instance with same parameters.
        Will automaticaly process snapshots for same instance with near
        time (10 minutes or shorter), but for other devices (/dev/sdb,
        /dev/sdc, etc);
    force
        Run instance from ami after creation without confirmation. To
        enable set value to "RUN";
    default_arch
        architecture to use if not mentioned in snapshot description;
    default_type
        instance type to use if not mentioned in snapshot description.
        Used only if ``force`` is "RUN".
    """
    if not region or not snap_id:
        region, snap_id = select_snapshot()
    conn = get_region_by_name(region).connect()
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id, ])[0]
    instance_id = get_snap_instance(snap)
    _device = get_snap_device(snap) or device
    snaps = conn.get_all_snapshots(owner='self')
    snapshots = [snp for snp in snaps if
        get_snap_instance(snp) == instance_id and
        get_snap_device(snp) != _device and
        abs(get_snap_time(snap) - get_snap_time(snp)) <= timedelta(minutes=10)]
    snapshot = sorted(snapshots, key=get_snap_time,
                      reverse=True) if snapshots else None
    # setup for building an EBS boot snapshot
    arch = get_descr_attr(snap, 'Arch') or default_arch
    kernel= config.get(conn.region.name, 'kernel' + arch)
    dev = re.match(r'^/dev/sd[a-z]$', _device)
    if dev:
        kernel = config.get(conn.region.name, 'kernel_encr_' + arch)
    ebs = EBSBlockDeviceType()
    ebs.snapshot_id = snap_id
    ebs.delete_on_termination = True
    block_map = BlockDeviceMapping()
    block_map[_device] = ebs
    if snapshot:
        for s in snapshot:
            s_dev = get_snap_device(s) or device
            s_ebs = EBSBlockDeviceType()
            s_ebs.delete_on_termination = True
            s_ebs.snapshot_id = s.id
            block_map[s_dev] = s_ebs

    name = 'Created {0} using access key {1}'.format(_now(), conn.access_key)
    name = name.replace(":", ".").replace(" ", "_")

    # create the new AMI all options from snap JSON description:
    wait_for(snap, '100%', limit=10 * 60)
    result = conn.register_image(
        name=name,
        description=snap.description,
        architecture=get_descr_attr(snap, 'Arch') or default_arch,
        root_device_name=get_descr_attr(snap, 'Root_dev_name') or root_dev,
        block_device_map=block_map, kernel_id=kernel)
    sleep(2)
    image = conn.get_all_images(image_ids=[result, ])[0]
    wait_for(image, 'available', limit=10 * 60)
    add_tags(image, snap.tags)

    logger.info('The new AMI ID = {0}'.format(result))

    info = ('\nEnter RUN if you want to launch instance using '
            'just created {0}: '.format(image))
    if force == 'RUN' or raw_input(info).strip() == 'RUN':
        instance_type = get_descr_attr(snap, 'Type') or default_type
        launch_instance_from_ami(region, image.id, inst_type=instance_type)
    return image


@task
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
    region = get_region_by_name(region)
    instance = get_inst_by_id(region, instance_id)
    env.update({
        'host_string': instance.public_dns_name,
        'key_filename': key_filename,
    })
    sudo('env DEBIAN_FRONTEND=noninteractive apt-get update && '
         'sudo env DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade && '
         'env DEBIAN_FRONTEND=noninteractive apt-get install grub-legacy-ec2')
    kernel = config.get(region.name, 'kernel'+instance.architecture)
    instance.stop()
    wait_for(instance, 'stopped')
    instance.modify_attribute('kernel', kernel)
    instance.start()


def make_encrypted_ubuntu(host_string, key_filename, user, hostname,
                          architecture, dev, name, release, pw1, pw2):
    with settings(host_string=host_string, user=user,
                  key_filename=key_filename):
        ext = config.get('DEFAULT', release + '_ext')
        data = '/home/' + user + '/data'
        page = 'https://uec-images.ubuntu.com/releases/' \
               + release + '/release-' + ext + '/'
        image = release + '-server-uec-' + architecture + '.img'
        pattern = '<a href=\\"([^\\"]*-' \
                  + architecture + '\.tar\.gz)\\">\\1</a>'
        bootlabel = "bootfs"

        def check(message, program, sums):
            with hide('running', 'stdout'):
                options = '--keyring=' + data + '/encrypted_root/uecimage.gpg'
                logger.info('{0}'.format(message))
                sudo('curl -fs "{page}/{sums}.gpg" > "{data}/{sums}.gpg"'
                     .format(page=page, sums=sums, data=data))
                try:
                    sudo('curl -fs "{page}/{sums}" > "{data}/{sums}"'
                         .format(page=page, sums=sums, data=data))
                except:
                    logger.exception('N/A')
                try:
                    sudo('gpgv {options} "{data}/{sums}.gpg" '
                         '"{data}/{sums}" 2> /dev/null'
                         .format(options=options, sums=sums, data=data))
                except:
                    logger.exception('Evil.')
                try:
                    sudo('grep "{file}" "{data}/{sums}" | (cd {data};'
                         ' {program} --check --status)'
                         .format(file=file, sums=sums, data=data,
                         program=program))
                except:
                    logger.exception('Failed.')
                logger.info('Ok')

        with hide('running', 'stdout'):
            try:
                wait_for_sudo('date')
            except BaseException as err:
                logger.exception(str(err))

            while pw1 == pw2:
                pw1 = prompt('Type in first password for enryption: ')
                pw2 = prompt('Type in second password for enryption: ')
                if pw1 == pw2:
                    logger.info('\nPasswords can\'t be the same.\n')
            logger.info('Installing cryptsetup.....')
            sudo('apt-get -y install cryptsetup')
            sudo('mkdir -p {0}'.format(data))
            try:
                logger.info('Downloading releases list.....')
                sudo('curl -fs "{0}" > "{1}/release.html"'.format(page, data))
            except:
                logger.exception('Invalid system: {0}{1}'.format(release, ext))
            logger.info('Uploading uecimage.gpg.....')
            put('./encrypted_root.tar.gz', data + '/encrypted_root.tar.gz',
                use_sudo=True, mirror_local_mode=True)
            sudo('cd {data}; tar -xf {data}/encrypted_root.tar.gz'
                 .format(data=data))
            file = sudo('pattern=\'<a href="([^"]*-{arch}\.tar\.gz)">'
                        '\\1</a>\'; perl -ne "m[$pattern] && "\'print "$1\\n'
                        '"\' "{data}/release.html"'
                        .format(data=data, pattern=pattern, arch=architecture))
            logger.info('Downloading ubuntu image.....')
            sudo('wget -P "{data}" "{page}{file}"'
                 .format(data=data, page=page, file=file))
            check('Checking SHA256...', 'sha256sum', 'SHA256SUMS')
            check('Checking SHA1.....', 'sha1sum', 'SHA1SUMS')
            check('Checking MD5......', 'md5sum', 'MD5SUMS')
            work = sudo('mktemp --directory')
            sudo('touch {work}/{image}'.format(work=work, image=image))
            logger.info('Unpacking ubuntu image.....')
            sudo('tar xfz "{data}/{file}" -C "{work}" {image}'
                 .format(data=data, file=file, work=work, image=image))
            sudo('mkdir "{work}/ubuntu"'.format(work=work))
            logger.info('Mounting ubuntu image to working directory.....')
            sudo('mount -o loop,ro "{work}/{image}" "{work}/ubuntu"'
                 .format(image=image, work=work))
            logger.info('Creating separate boot volume.....')
            sudo('echo -e "0 1024 83 *\n;\n" | /sbin/sfdisk -uM {dev}'
                 .format(dev=dev))
            logger.info('Formatting boot volume.....')
            sudo('/sbin/mkfs -t ext3 -L "{bootlabel}" "{dev}1"'
                 .format(bootlabel=bootlabel, dev=dev))
            sudo('touch {work}/pw2.txt | echo -n {pw1} > "{work}/pw1.txt" | '
                 'chmod 700 "{work}/pw1.txt"'
                 .format(pw1=pw1, work=work))
            sudo('touch {work}/pw2.txt | echo -n {pw2} > "{work}/pw2.txt" | '
                 'chmod 700 "{work}/pw2.txt"'
                 .format(pw2=pw2, work=work))
            logger.info('Creating luks encrypted volume.....')
            sudo('cryptsetup luksFormat -q --key-size=256 {dev}2 "{work}/'
                 'pw1.txt"'.format(dev=dev, work=work))
            logger.info('Adding second key to encrypted volume.....')
            sudo('cryptsetup luksAddKey -q --key-file="{work}/pw1.txt" '
                 '{dev}2 "{work}/pw2.txt"'.format(work=work, dev=dev))
            logger.info('Opening luks encrypted volume.....')
            sudo('cryptsetup luksOpen --key-file="{work}/pw1.txt" '
                 '{dev}2 {name}'.format(work=work, dev=dev, name=name))
            sudo('shred --remove "{work}/pw1.txt"; shred --remove'
                 ' "{work}/pw2.txt"'.format(work=work))
            fs_type = sudo('df -T "{work}/ubuntu" | tail -1 | cut -d " " -f 5'
                           .format(work=work))
            logger.info('Creating filesystem on luks encrypted volume.....')
            sudo('mkfs -t {fs_type} "/dev/mapper/{name}"'
                 .format(fs_type=fs_type, name=name))
            sudo('/sbin/e2label "/dev/mapper/{name}" "uec-rootfs"'
                 .format(name=name))
            logger.info('Mounting luks encrypted volume.....')
            sudo('mkdir -p "{work}/root"; mount /dev/mapper/{name}'
                 ' "{work}/root"'.format(work=work, name=name))
            logger.info('Starting syncronisation of working dir with image')
            sudo('rsync --archive --hard-links "{work}/ubuntu/"'
                 ' "{work}/root/"'.format(work=work))
            boot_device = 'LABEL=' + bootlabel
            root_device = 'UUID=$(cryptsetup luksUUID ' + dev + '2)'
            sudo('mkdir "{work}/boot"; mount "{dev}1" "{work}/boot"'
                 .format(work=work, dev=dev))
            sudo('rsync --archive "{work}/root/boot/" "{work}/boot"'
                 .format(work=work))
            sudo('rm -rf "{work}/root/boot/"*'.format(work=work))
            sudo('mount --move "{work}/boot" "{work}/root/boot"'
                 .format(work=work))
            sudo('echo "{boot_device} /boot ext3" >> "{work}/root/etc/fstab"'
                 .format(boot_device=boot_device, work=work))
            sudo('sed -i -e \'s/(hd0)/(hd0,0)/\' "{work}/root/boot/grub/menu.'
                 'lst"'.format(work=work))
            bozo_target = work + '/root/etc/initramfs-tools/boot'
            sudo('mkdir -p {bozo_target}'.format(bozo_target=bozo_target))
            logger.info('Copying files for preboot web-auth.....')
            sudo('cp {data}/encrypted_root/cryptsetup '
                 '{work}/root/etc/initramfs-tools/hooks/cryptsetup'
                 .format(data=data, work=work))
            sudo('cp {data}/encrypted_root/boot.key {bozo_target}/boot.key'
                 .format(data=data, bozo_target=bozo_target))
            sudo('cp {data}/encrypted_root/boot.crt {bozo_target}/boot.crt'
                 .format(data=data, bozo_target=bozo_target))
            sudo('cp {data}/encrypted_root/cryptsetup.sh '
                 '{bozo_target}/cryptsetup.sh'
                 .format(data=data, bozo_target=bozo_target))
            sudo('cp {data}/encrypted_root/make_bozo_dir.sh '
                 '{bozo_target}/make_bozo_dir.sh'
                 .format(data=data, bozo_target=bozo_target))
            sudo('cp {data}/encrypted_root/index.html '
                 '{bozo_target}/index.html'
                 .format(data=data, bozo_target=bozo_target))
            sudo('cp {data}/encrypted_root/activate.cgi '
                 '{bozo_target}/activate.cgi'
                 .format(data=data, bozo_target=bozo_target))
            sudo('cp {data}/encrypted_root/hiding.gif '
                 '{bozo_target}/hiding.gif'
                 .format(data=data, bozo_target=bozo_target))
            logger.info('Modifying scripts to match our volumes.....')
            sudo('sed -i "s/\/dev\/sda2/{root_device}/" '
                 '{work}/root/etc/initramfs-tools/hooks/cryptsetup'.format(
                 root_device=root_device, work=work))
            sudo('perl -i -p - "{0}/root/etc/initramfs-tools/boot/'
                 'cryptsetup.sh" <<- EOT\ns[^(cs_host=).*][\\$1"{1}"];'
                '\nEOT\n'.format(work, hostname))
            sudo('mkdir -p "{work}/root/etc/ec2"'.format(work=work))
            if release == 'lucid':
                logger.info('Adding apt entries for lucid.....')
                listfile = work + '/root/etc/apt/sources.list'
                sudo('grep "lucid main" {listfile} | sed "'
                     's/lucid/maverick/g" >> {work}/root/etc/'
                     'apt/sources.list.d/bozohttpd.list'
                     .format(listfile=listfile, work=work))
                sudo('echo -e "Package: *\nPin: release a=lucid\nPin-Priority:'
                     ' 600\n\nPackage: bozohttpd\nPin: release a=maverick\n'
                     'Pin-Priority: 1000\n\nPackage: libssl0.9.8\nPin: release'
                     ' a=maverick\nPin-Priority: 1000\n\nPackage: *\n'
                     'Pin: release o=Ubuntu\nPin-Priority: -10\n" | tee '
                     '"{work}/root/etc/apt/preferences"'.format(work=work))
            menufile = work + '/root/boot/grub/menu.lst'
            initrd = sudo('grep "^initrd" "{menufile}" | head -1 | cut -f 3'
                          .format(menufile=menufile))
            kernel = sudo('grep "^kernel" "{menufile}" | head -1 | cut -f 3 | '
                          'cut -d " " -f 1'.format(menufile=menufile))
            sudo('rm -f "{work}/root/initrd.img.old";'
                 'rm -f "{work}/root/vmlinuz.old";'
                 'rm -f "{work}/root/initrd.img";'
                 'rm -f "{work}/root/vmlinuz"'.format(work=work))
            logger.info('Creating symbolic links for kernel.....')
            sudo('ln -s "{initrd}" "{work}/root/initrd.img";'
                 'ln -s "{kernel}" "{work}/root/vmlinuz"'
                 .format(initrd=initrd, kernel=kernel, work=work))
            sudo('mv "{work}/root/etc/resolv.conf" '
                 '"{work}/root/etc/resolv.conf.old";cp "/etc/resolv.conf" '
                 '"{work}/root/etc/"'.format(work=work))
            logger.info('Chrooting and installing needed apps..')
            sudo('chroot "{work}/root" <<- EOT\n'
                 'set -e\n'
                 'mount -t devpts devpts /dev/pts/\n'
                 'mount -t proc proc /proc/\n'
                 'mount -t sysfs sysfs /sys/\n'
                 'localedef -f UTF-8 -i en_US --no-archive en_US.utf8\n'
                 'apt-get -y update\n'
                 'apt-get -y install ssl-cert\n'
                 'apt-get -y install update-inetd\n'
                 'mv /usr/sbin/update-inetd /usr/sbin/update-inetd.old\n'
                 'touch /usr/sbin/update-inetd\n'
                 'chmod a+x /usr/sbin/update-inetd\n'
                 'apt-get -y install bozohttpd\n'
                 'mv /usr/sbin/update-inetd.old /usr/sbin/update-inetd\n'
                 'EOT'.format(work=work))
            logger.info('Fixing permissions and symlinking bozohttpd...')
            sudo('chroot "{work}/root" <<- EOT\n'
                 'chown root:ssl-cert /etc/initramfs-tools/boot/boot.key\n'
                 'chmod 640 /etc/initramfs-tools/boot/boot.key\n'
                 'ln -s /usr/sbin/bozohttpd /etc/initramfs-tools/boot/\n'
                 'ln -s . /boot/boot\n'
                 'EOT'.format(work=work))
            logger.info('Instaling cryptsetup and unmounting.....')
            sudo('chroot "{work}/root" <<- EOT\n'
                 'apt-get -y install cryptsetup\n'
                 'apt-get -y clean\n'
                 'update-initramfs -uk all\n'
                 'mv /etc/resolv.conf.old /etc/resolv.conf\n'
                 'umount /dev/pts\n'
                 'umount /proc\n'
                 'umount /sys\n'
                 'EOT'.format(work=work))
            logger.info('Shutting down temporary instance')
            sudo('shutdown -h now')
    return


@task
def create_encrypted_instance(region_name, release='lucid', volume_size='8',
                             security_groups=None, architecture='x86_64',
                             type='t1.micro', name='encr_root',
                             hostname="boot.odeskps.com", pw1=None, pw2=None):
    """
    Creates ubuntu instance with luks-encryted root volume.

    region_name
        Region where you want to create instance;
    release
        Ubuntu release name (lucid or natty);
    volume_size
        Size of volume in Gb (always remember, that script creates boot volume
        with size 1Gb, so minimal size of whole volume is 3Gb (1Gb for /boot
        2Gb for /));
    type
        Type of instance;
    hostname
        Hostname that will be used for access unlocking root volume;
    pw1, pw2
        You can specify passwords in parameters to suppress password prompt;

    To unlock go to https://ip_address_of_instance (only after reboot).
    You can set up to 8 passwords. Defaut boot.key and boot.crt created for
    *.amazonaws.com so must work for all instances.
    Process of creation is about 20 minutes long.
    """
    region = get_region_by_name(region_name)
    conn = region.connect()

    with config_temp_ssh(conn) as key_filename:
        key_pair = os.path.splitext(os.path.split(key_filename)[1])[0]
        zn = conn.get_all_zones()[-1]
        ssh_grp = config.get('DEFAULT', 'ssh_security_group')
        with create_temp_inst(zone=zn, key_pair=key_pair,
                              security_groups=[ssh_grp]) as inst:
            vol = conn.create_volume(size=volume_size, zone=zn)
            dev = get_avail_dev_encr(inst)
            vol.attach(inst.id, dev)
            if architecture == 'x86_64':
                arch = 'amd64'
            else:
                arch = architecture
            make_encrypted_ubuntu(inst.public_dns_name, key_filename, 'ubuntu',
                                  hostname, arch, dev, name, release, pw1, pw2)
            snap = vol.create_snapshot()
            wait_for(snap, '100%', limit=20 * 60)
            vol.detach(force=True)
            wait_for(vol, 'available')
            vol.delete()
            img = create_ami(region_name, snap.id, 'RUN', device='/dev/sda',
                             default_arch=architecture, default_type=type)
            img.deregister()
            snap.delete()

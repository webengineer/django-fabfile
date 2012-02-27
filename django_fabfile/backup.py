"""Check :doc:`README` or :class:`django_fabfile.utils.Config` docstring
for setup instructions."""

from contextlib import contextmanager
import logging
import os
import re
from datetime import timedelta, datetime
from contextlib import nested
from itertools import groupby
from json import dumps

from boto.exception import EC2ResponseError
from dateutil.relativedelta import relativedelta
from fabric.api import env, local, put, settings, sudo, task, run
from fabric.contrib.files import append

from django_fabfile.instances import (attach_snapshot, create_temp_inst,
                                      get_avail_dev, get_vol_dev, mount_volume)
from django_fabfile.utils import (
    Config, StateNotChangedError, add_tags, config_temp_ssh,
    get_descr_attr, get_inst_by_id, get_region_conn, get_snap_device,
    get_snap_time, get_snap_vol, timestamp, wait_for, wait_for_sudo)


config = Config()
username = config.get('DEFAULT', 'USERNAME')
env.update({'user': username, 'disable_known_hosts': True})

logger = logging.getLogger(__name__)


DEFAULT_TAG_NAME = config.get('DEFAULT', 'TAG_NAME')
DEFAULT_TAG_VALUE = config.get('DEFAULT', 'TAG_VALUE')
DESCRIPTION_TAG = 'Description'
SNAP_STATUSES = ['pending', 'completed']    # All but "error".
VOL_STATUSES = ['creating', 'available', 'in-use']


def create_snapshot(vol, description='', tags=None, synchronously=True,
                    consistent=False):
    """Return new snapshot for the volume.

    vol
        volume to snapshot;
    synchronously
        wait for successful completion;
    description
        description for snapshot. Will be compiled from instnace
        parameters by default;
    tags
        tags to be added to snapshot. Will be cloned from volume and from
        instance by default.
    consistent
        if consistent True, script will try to freeze fs mountpoint and create
        snapshot while it's freezed with all buffers dumped to disk.
    """
    if vol.attach_data:
        inst = get_inst_by_id(vol.region.name, vol.attach_data.instance_id)
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
            'Time': timestamp(),
            })

    def freeze_volume():
        key_filename = config.get(inst.region.name, 'KEY_FILENAME')
        try:
            _user = config.get('SYNC', 'USERNAME')
        except:
            _user = username
        with settings(host_string=inst.public_dns_name,
                      key_filename=key_filename, user=_user):
            wait_for_sudo('sync', shell=False)
            run('for i in {1..20}; do sudo sync; sleep 1; done &')

    def initiate_snapshot():
        if consistent:
            if inst.state == 'running':
                try:
                    freeze_volume()
                except:
                    logger.info('FS NOT FREEZED! '
                                'Do you have access to this server?')
        snapshot = vol.create_snapshot(description)
        if tags:
            add_tags(snapshot, tags)
        else:
            add_tags(snapshot, vol.tags)
            if inst:
                add_tags(snapshot, inst.tags)
        logger.info('{0} started from {1} in {0.region}'.format(snapshot, vol))
        return snapshot

    if synchronously:
        timeout = config.getint('DEFAULT', 'MINUTES_FOR_SNAP')
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
                    synchronously=False, consistent=False):
    """
    Return list of created snapshots for specified instance.

    region_name
        instance location;
    instance, instance_id
        either `instance_id` or `instance` argument should be specified;
    synchronously
        wait for successful completion. False by default.
    consistent
        if True, then FS mountpoint will be frozen before snapshotting.
        False by default.
    """
    assert bool(instance_id) ^ bool(instance), ('Either instance_id or '
        'instance should be specified')
    conn = get_region_conn(region_name)
    if instance_id:
        instance = get_inst_by_id(conn.region.name, instance_id)
    snapshots = []
    for dev in instance.block_device_mapping:
        vol_id = instance.block_device_mapping[dev].volume_id
        vol = conn.get_all_volumes([vol_id])[0]
        snapshots.append(create_snapshot(vol, synchronously=synchronously,
                         consistent=consistent))
    return snapshots


@task
def backup_instances_by_tag(
        region_name=None, tag_name=DEFAULT_TAG_NAME,
        tag_value=DEFAULT_TAG_VALUE, synchronously=False, consistent=False):
    """Creates backup for all instances with given tag in region.

    region_name
        will be applied across all regions by default;
    tag_name, tag_value
        will be fetched from config by default;
    synchronously
        will be accomplished with assuring successful result. False by
        default;
    consistent
        if True, then FS mountpoint will be frozen before snapshotting.

    .. note:: when ``create_ami`` task compiles AMI from several
              snapshots it restricts snapshot start_time difference with
              10 minutes interval at most. Snapshot completion may take
              much more time and due to this only asynchronously
              generated snapshots will be assembled assurely."""
    if region_name:
        regions = [get_region_conn(region_name).region]
    else:
        regions = get_region_conn().get_all_regions()
    for reg in regions:
        conn = get_region_conn(reg.name)
        filters = {'resource-type': 'instance', 'key': tag_name,
                   'tag-value': tag_value}
        for tag in conn.get_all_tags(filters=filters):
            backup_instance(reg.name, instance_id=tag.res_id,
                            synchronously=synchronously, consistent=consistent)


def _trim_snapshots(region, dry_run=False):

    """Delete snapshots back in time in logarithmic manner.

    dry_run
        just print snapshot to be deleted.

    Modified version of the `boto.ec2.connection.trim_snapshots
    <http://pypi.python.org/pypi/boto/2.0>_`. Licensed under MIT license
    by Mitch Garnaat, 2011."""
    hourly_backups = config.getint('purge_backups', 'HOURLY_BACKUPS')
    daily_backups = config.getint('purge_backups', 'DAILY_BACKUPS')
    weekly_backups = config.getint('purge_backups', 'WEEKLY_BACKUPS')
    monthly_backups = config.getint('purge_backups', 'MONTHLY_BACKUPS')
    quarterly_backups = config.getint('purge_backups', 'QUARTERLY_BACKUPS')
    yearly_backups = config.getint('purge_backups', 'YEARLY_BACKUPS')

    # work with UTC time, which is what the snapshot start time is reported in
    now = datetime.utcnow()
    last_hour = datetime(now.year, now.month, now.day, now.hour)
    last_midnight = datetime(now.year, now.month, now.day)
    last_sunday = datetime(now.year, now.month,
          now.day) - timedelta(days=(now.weekday() + 1) % 7)
    last_month = datetime.now() - relativedelta(months=1)
    last_year = datetime.now() - relativedelta(years=1)
    other_years = datetime.now() - relativedelta(years=2)
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
        target_backup_times.append(last_month - relativedelta(months=month))

    for quart in range(0, quarterly_backups):
        target_backup_times.append(last_year - relativedelta(months=4 * quart))

    for year in range(0, yearly_backups):
        target_backup_times.append(other_years - relativedelta(years=year))

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
    conn = get_region_conn(region.name)
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
    for region in get_region_conn().get_all_regions():
        conn = get_region_conn(region.name)
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
    if region_name:
        regions = [get_region_conn(region_name).region]
    else:
        regions = get_region_conn().get_all_regions()
    for reg in regions:
        logger.info('Processing {0}'.format(reg))
        _trim_snapshots(reg, dry_run=dry_run)


@task
def rsync_mountpoints(src_inst, src_vol, src_mnt, dst_inst, dst_vol, dst_mnt,
                      encr=False):
    """Run `rsync` against mountpoints, copy disk label.

    :param src_inst: source instance;
    :param src_vol: source volume with label that will be copied to
                    dst_vol;
    :param src_mnt: root or directory hierarchy to replicate;
    :param dst_inst: destination instance;
    :param dst_vol: destination volume, that will be marked with label
                    from src_vol;
    :param dst_mnt: destination point where source hierarchy to place;
    :param encr: True if volume is encrypted;
    :type encr: bool."""
    src_key_filename = config.get(src_inst.region.name, 'KEY_FILENAME')
    dst_key_filename = config.get(dst_inst.region.name, 'KEY_FILENAME')
    with config_temp_ssh(dst_inst.connection) as key_file:
        with settings(host_string=dst_inst.public_dns_name,
                      key_filename=dst_key_filename):
            wait_for_sudo('cp /root/.ssh/authorized_keys '
                          '/root/.ssh/authorized_keys.bak')
            pub_key = local('ssh-keygen -y -f {0}'.format(key_file), True)
            append('/root/.ssh/authorized_keys', pub_key, use_sudo=True)
            if encr:
                sudo('screen -d -m sh -c "nc -l 60000 | gzip -dfc | '
                     'sudo dd of={0} bs=16M"'
                     .format(get_vol_dev(dst_vol)), pty=False)  # dirty magick
                dst_ip = sudo(
                    'curl http://169.254.169.254/latest/meta-data/public-ipv4')

        with settings(host_string=src_inst.public_dns_name,
                      key_filename=src_key_filename):
            put(key_file, '.ssh/', mirror_local_mode=True)
            dst_key_filename = os.path.split(key_file)[1]
            if encr:
                sudo('(dd if={0} bs=16M | gzip -cf --fast | nc -v {1} 60000)'
                     .format(get_vol_dev(src_vol), dst_ip))
            else:
                cmd = (
                    'rsync -e "ssh -i .ssh/{key_file} -o '
                    'StrictHostKeyChecking=no" -cahHAX --delete --inplace '
                    '--exclude /root/.bash_history '
                    '--exclude /home/*/.bash_history '
                    '--exclude /etc/ssh/moduli --exclude /etc/ssh/ssh_host_* '
                    '--exclude /etc/udev/rules.d/*persistent-net.rules '
                    '--exclude /var/lib/ec2/* --exclude=/mnt/* '
                    '--exclude=/proc/* --exclude=/tmp/* '
                    '{src_mnt}/ root@{rhost}:{dst_mnt}')
                wait_for_sudo(cmd.format(
                    rhost=dst_inst.public_dns_name, dst_mnt=dst_mnt,
                    key_file=dst_key_filename, src_mnt=src_mnt))
                label = sudo('e2label {0}'.format(get_vol_dev(src_vol)))
        with settings(host_string=dst_inst.public_dns_name,
                      key_filename=dst_key_filename):
            if not encr:
                sudo('e2label {0} {1}'.format(get_vol_dev(dst_vol), label))
            wait_for_sudo('mv /root/.ssh/authorized_keys.bak '
                          '/root/.ssh/authorized_keys')
            wait_for_sudo('sync', shell=False)
            wait_for_sudo('for i in {1..20}; do sync; sleep 1; done &')


def update_snap(src_vol, src_mnt, dst_vol, dst_mnt, encr, delete_old=False):

    """Update destination region from `src_vol`.

    Create new snapshot with same description and tags. Delete previous
    snapshot (if exists) of the same volume in destination region if
    ``delete_old`` is True."""

    src_inst = get_inst_by_id(src_vol.region.name,
                              src_vol.attach_data.instance_id)
    dst_inst = get_inst_by_id(dst_vol.region.name,
                              dst_vol.attach_data.instance_id)
    rsync_mountpoints(src_inst, src_vol, src_mnt, dst_inst, dst_vol, dst_mnt,
                     encr)
    src_snap = src_vol.connection.get_all_snapshots([src_vol.snapshot_id])[0]
    create_snapshot(dst_vol, description=src_snap.description,
                                    tags=src_snap.tags, synchronously=False)
    if delete_old and dst_vol.snapshot_id:
        old_snap = dst_vol.connection.get_all_snapshots(
            [dst_vol.snapshot_id])[0]
        logger.info('Deleting previous {0} in {1}'.format(old_snap,
                                                          dst_vol.region))
        old_snap.delete()


@contextmanager
def create_tmp_volume(region, size):
    """Format new filesystem."""
    with create_temp_inst(region) as inst:
        earmarking_tag = config.get(region.name, 'TAG_NAME')
        try:
            vol = get_region_conn(region.name).create_volume(size,
                                                             inst.placement)
            vol.add_tag(earmarking_tag, 'temporary')
            vol.attach(inst.id, get_avail_dev(inst))
            yield vol, mount_volume(vol, mkfs=True)
        finally:
            vol.detach(True)
            wait_for(vol, 'available')
            vol.delete()


def get_relevant_snapshots(
        conn, tag_name=DEFAULT_TAG_NAME, tag_value=DEFAULT_TAG_VALUE,
        native_only=True, filters={'status': SNAP_STATUSES}):
    """Returns snapshots with proper description."""
    if tag_name and tag_value:
        filters.update({'tag:{0}'.format(tag_name): tag_value})
    snaps = conn.get_all_snapshots(owner='self', filters=filters)
    is_described = lambda snap: get_snap_vol(snap) and get_snap_time(snap)
    snaps = [snp for snp in snaps if is_described(snp)]
    if native_only:
        is_native = lambda snp, reg: get_descr_attr(snp, 'Region') == reg.name
        snaps = [snp for snp in snaps if is_native(snp, conn.region)]
    return snaps


def get_oldest_replica(
        src_conn, dst_conn, amount=1, native_only=True,
        tag_name=DEFAULT_TAG_NAME, tag_value=DEFAULT_TAG_VALUE):
    """Return list with (one by default) not yet replicated snapshots."""
    snaps = get_relevant_snapshots(src_conn, tag_name, tag_value, native_only)
    if not snaps:
        return []
    # Separating out all but latest snapshots for every volume.
    latest_snaps = []
    snaps = sorted(snaps, key=get_snap_vol)
    for vol_id, vol_snaps in groupby(snaps, key=get_snap_vol):
        latest_snaps.append(sorted(vol_snaps, key=get_snap_time)[-1])
    # Seeking for latests replicas in dst region for every new snapshot.
    # Treating volumes with proper description as replicas.
    latest_descriptions = [snp.description for snp in latest_snaps]
    # Replicated snapshots.
    snap_desc = [snp.description for snp in dst_conn.get_all_snapshots(
        owner='self', filters={'status': SNAP_STATUSES,
                               'description': latest_descriptions})]
    # Temporary volumes used by in-process replication.
    vol_desc = [vol.tags[DESCRIPTION_TAG] for vol in dst_conn.get_all_volumes(
        filters={'tag:{0}'.format(DESCRIPTION_TAG): latest_descriptions,
                 'status': VOL_STATUSES})]
    # Seeking for snaps wihtout replicas.
    snaps_to_replicate = [snp for snp in latest_snaps if
        snp.description not in set(snap_desc + vol_desc)]
    return sorted(snaps_to_replicate, key=get_snap_time)[:amount]


@task
def rsync_snapshot(src_region_name, snapshot_id, dst_region_name,
                   src_inst=None, dst_inst=None, force=False):

    """Duplicate the snapshot into dst_region.

    src_region_name, dst_region_name
        Amazon region names. Allowed to be contracted, e.g.
        `ap-southeast-1` will be recognized in `ap-south` or even
        `ap-s`;
    snapshot_id
        snapshot to duplicate;
    src_inst, dst_inst
        will be used instead of creating new for temporary;
    force
        rsync snapshot even if newer version exist.

    You'll need to open port 60000 for encrypted instances replication."""
    src_conn = get_region_conn(src_region_name)
    src_snap = src_conn.get_all_snapshots([snapshot_id])[0]
    dst_conn = get_region_conn(dst_region_name)
    _src_device = get_snap_device(src_snap)
    _src_dev = re.match(r'^/dev/sda$', _src_device)  # check for encryption
    if _src_dev:
        encr = True
        logger.info('Found traces of encryption')
    else:
        encr = None

    info = 'Going to transmit {snap.volume_size} GiB {snap} {snap.description}'
    if src_snap.tags.get('Name'):
        info += ' of {name}'
    info += ' from {snap.region} to {dst}'
    logger.info(info.format(snap=src_snap, dst=dst_conn.region,
                            name=src_snap.tags.get('Name')))

    src_vol = get_snap_vol(src_snap)
    dst_snaps = get_relevant_snapshots(dst_conn, native_only=False)
    vol_snaps = [snp for snp in dst_snaps if get_snap_vol(snp) == src_vol]
    if not force:
        tmp_vol = dst_conn.get_all_volumes(     # Indicates running replication.
            filters={'tag:{0}'.format(DESCRIPTION_TAG): src_snap.description,
                     'status': VOL_STATUSES})
        if tmp_vol:
            logger.info('Stepping over {src} - it\'s already replicating to '
                        '{vol} in {reg}'.format(src=src_snap, vol=tmp_vol,
                                                reg=dst_conn.region))
            return

    def sync_mountpoints(src_vol, src_mnt, dst_vol, dst_mnt):
        # Marking temporary volume with snapshot's description.
        dst_vol.add_tag(DESCRIPTION_TAG, src_snap.description)
        update_snap(src_vol, src_mnt, dst_vol, dst_mnt, encr)

    if vol_snaps:
        dst_snap = sorted(vol_snaps, key=get_snap_time)[-1]
        if not force and get_snap_time(dst_snap) >= get_snap_time(src_snap):
            kwargs = dict(src=src_snap, dst=dst_snap, dst_reg=dst_conn.region)
            logger.info('Stepping over {src} - it\'s not newer than {dst} '
                        '{dst.description} in {dst_reg}'.format(**kwargs))
            return
        else:
            with nested(
                    attach_snapshot(src_snap, inst=src_inst, encr=encr),
                    attach_snapshot(dst_snap, inst=dst_inst, encr=encr)) as (
                        (src_vol, src_mnt), (dst_vol, dst_mnt)):
                sync_mountpoints(src_vol, src_mnt, dst_vol, dst_mnt)
    else:
        with nested(
                attach_snapshot(src_snap, inst=src_inst, encr=encr),
                create_tmp_volume(dst_conn.region, src_snap.volume_size)) as (
                    (src_vol, src_mnt), (dst_vol, dst_mnt)):
            sync_mountpoints(src_vol, src_mnt, dst_vol, dst_mnt)


@task
def rsync_region(
        src_region_name, dst_region_name, tag_name=DEFAULT_TAG_NAME,
        tag_value=DEFAULT_TAG_VALUE, native_only=True):
    """Duplicates latest snapshots with given tag into dst_region.

    src_region_name, dst_region_name
        every latest volume snapshot from src_region will be rsynced
        to the dst_region;
    tag_name, tag_value
        snapshots will be filtered by tag. Tag will be fetched from
        config by default;
    native_only
        sync only snapshots, created in the src_region_name. True by
        default."""
    src_conn = get_region_conn(src_region_name)
    dst_conn = get_region_conn(dst_region_name)
    snaps = get_relevant_snapshots(src_conn, tag_name, tag_value, native_only)
    if not snaps:
        return
    with nested(create_temp_inst(src_conn.region),
                create_temp_inst(dst_conn.region)) as (src_inst, dst_inst):
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
def rsync_all_regions(primary_backup_region, secondary_backup_region):
    """
    Replicates snapshots across all regions.

    Sync snapshots from all but primary regions into primary. And then
    snapshots from primary regions will be replicated into secondary
    backup region.

    :param primary_backup_region: AWS region name that keeps all
        snapshots clones;
    :type primary_backup_region: str
    :param secondary_backup_region: AWS region name that keeps clones of
        snapshots from `primary_backup_region`.
    :type secondary_backup_region: str
    """
    pri_name = get_region_conn(primary_backup_region).region.name
    all_regs = get_region_conn().get_all_regions()
    for reg in (reg for reg in all_regs if reg.name != pri_name):
        rsync_region(reg.name, primary_backup_region)
    rsync_region(primary_backup_region, secondary_backup_region)

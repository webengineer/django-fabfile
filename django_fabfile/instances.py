"""Check :doc:`README` or :class:`django_fabfile.utils.Config` docstring
for setup instructions."""

from contextlib import contextmanager
from datetime import timedelta, datetime
from json import dumps
import logging
import os
import re
from string import lowercase
from time import sleep
from traceback import format_exc

from boto.ec2 import regions
from boto.ec2.blockdevicemapping import BlockDeviceMapping, EBSBlockDeviceType
from boto.exception import BotoServerError
from fabric.api import env, output, prompt, put, settings, sudo, task
from fabric.context_managers import hide
from pkg_resources import resource_stream

from django_fabfile import __name__ as pkg_name
from django_fabfile.utils import (
    Config, StateNotChangedError, add_tags, config_temp_ssh,
    get_all_instances, get_all_snapshots, get_descr_attr,
    get_inst_by_id, get_region_conn, get_snap_device, get_snap_instance,
    get_snap_time, prompt_to_select, wait_for, wait_for_exists, wait_for_sudo)


config = Config()
username = config.get('DEFAULT', 'USERNAME')
env.update({'user': username, 'disable_known_hosts': True})

logger = logging.getLogger(__name__)

_now = lambda: datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')


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

    conn = get_region_conn(region_name)

    ami_ptrn = config.get('DEFAULT', 'AMI_PTRN')
    architecture = config.get('DEFAULT', 'ARCHITECTURE')
    ubuntu_aws_account = config.get('DEFAULT', 'UBUNTU_AWS_ACCOUNT')
    filters = {'owner_id': ubuntu_aws_account, 'architecture': architecture,
             'name': ami_ptrn, 'image_type': 'machine',
             'root_device_type': 'ebs'}
    images = conn.get_all_images(filters=filters)

    # Filtering by latest version.
    ptrn = re.compile(config.get('DEFAULT', 'AMI_REGEXP'))
    versions = set([ptrn.search(img.name).group('version') for img in images])

    def complement(year_month):
        return '0' + year_month if len(year_month) == 4 else year_month

    latest_version = sorted(set(filter(complement, versions)))[-1]  # XXX Y3K.
    ami_ptrn_with_version = config.get('DEFAULT', 'AMI_PTRN_WITH_VERSION')
    name_with_version = ami_ptrn_with_version.format(version=latest_version)
    filters.update({'name': name_with_version})
    images = conn.get_all_images(filters=filters)
    # Filtering by latest release date.
    dates = set([ptrn.search(img.name).group('released_at') for img in images])
    latest_date = sorted(set(dates))[-1]
    ami_ptrn_with_release_date = config.get('DEFAULT',
                                            'AMI_PTRN_WITH_RELEASE_DATE')
    name_with_version_and_release = ami_ptrn_with_release_date.format(
        version=latest_version, released_at=latest_date)
    filters.update({'name': name_with_version_and_release})
    image = conn.get_all_images(filters=filters)[0]
    zone = zone_name or conn.get_all_zones()[-1].name
    logger.info('Launching new instance in {zone} using {image}'
        .format(image=image, zone=zone))

    key_pair = key_pair or config.get(conn.region.name, 'KEY_PAIR')
    ssh_grp = config.get('DEFAULT', 'SSH_SECURITY_GROUP')
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
        inst.add_tag(config.get(zone.region.name, 'tag_name'), 'TEMPORARY')
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
    """Return OS-specific volume representation as attached device."""
    assert vol.attach_data.instance_id
    inst = get_inst_by_id(vol.region, vol.attach_data.instance_id)
    assert inst.public_dns_name     # Instance is down.
    key_filename = config.get(vol.region.name, 'KEY_FILENAME')
    attached_dev = vol.attach_data.device
    natty_dev = attached_dev.replace('sd', 'xvd')
    with settings(host_string=inst.public_dns_name, key_filename=key_filename):
        logger.debug(env, output)
        for dev in [attached_dev, natty_dev]:
            if wait_for_exists(dev):
                return dev


def mount_volume(vol, mkfs=False):

    """Mount the device by SSH. Return mountpoint on success.

    vol
        volume to be mounted on the instance it is attached to."""

    vol.update()
    assert vol.attach_data
    inst = get_inst_by_id(vol.region, vol.attach_data.instance_id)
    key_filename = config.get(vol.region.name, 'KEY_FILENAME')
    with settings(host_string=inst.public_dns_name, key_filename=key_filename):
        dev = get_vol_dev(vol)
        mountpoint = dev.replace('/dev/', '/media/')
        wait_for_sudo('mkdir -p {0}'.format(mountpoint))
        if mkfs:
            sudo('mkfs.ext3 {dev}'.format(dev=dev))
        sudo('mount {dev} {mnt}'.format(dev=dev, mnt=mountpoint))
        if mkfs:
            sudo('chown -R {user}:{user} {mnt}'.format(user=env.user,
                                                       mnt=mountpoint))
    logger.debug('Mounted {0} to {1} at {2}'.format(vol, inst, mountpoint))
    return mountpoint


@contextmanager
def attach_snapshot(snap, key_pair=None, security_groups=None, inst=None):

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
            vol.add_tag(config.get(inst.region.name, 'tag_name'), 'TEMPORARY')
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
            mnt = mount_volume(vol)
            yield vol, mnt
        except BaseException as err:
            logger.exception(str(err))
        finally:
            key_filename = config.get(inst.region.name, 'KEY_FILENAME')
            with settings(host_string=inst.public_dns_name,
                          key_filename=key_filename):
                try:
                    wait_for_sudo('umount {0}'.format(mnt))
                except:
                    pass
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


@task
def modify_kernel(region, instance_id):
    """
    Modify old kernel for stopped instance (needed for make pv-grub working)

    .. note:: install grub-legacy-ec2 and upgrades before run this.

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
        us-west-1       i386    aki-99a0f1dc"""
    key_filename = config.get(region, 'KEY_FILENAME')
    conn = get_region_conn(region)
    instance = get_inst_by_id(conn.region, instance_id)
    env.update({
        'host_string': instance.public_dns_name,
        'key_filename': key_filename,
    })
    sudo('env DEBIAN_FRONTEND=noninteractive apt-get update && '
         'env DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade && '
         'env DEBIAN_FRONTEND=noninteractive apt-get install grub-legacy-ec2')
    kernel = config.get(conn.region.name,
                        'KERNEL' + instance.architecture.upper())
    instance.stop()
    wait_for(instance, 'stopped')
    instance.modify_attribute('kernel', kernel)
    instance.start()


def make_encrypted_ubuntu(host_string, key_filename, user,
                          architecture, dev, name, release, pw1, pw2):
    with settings(host_string=host_string, user=user,
                  key_filename=key_filename):
        data = '/home/' + user + '/data'
        page = 'https://uec-images.ubuntu.com/releases/' \
               + release + '/release/'
        image = release + '-server-uec-' + architecture + '.img'
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
                logger.exception('Invalid system: {0}'.format(release))
            logger.info('Uploading uecimage.gpg.....')
            encr_root = resource_stream(pkg_name, 'encrypted_root.tar.gz')
            put(encr_root, data + '/encrypted_root.tar.gz', use_sudo=True,
                mirror_local_mode=True)
            sudo('cd {data}; tar -xf {data}/encrypted_root.tar.gz'
                 .format(data=data))
            file = sudo('pattern=\'<a href="([^"]*-{arch}\.tar\.gz)">'
                        '\\1</a>\'; perl -ne "m[$pattern] && "\'print "$1\\n'
                        '"\' "{data}/release.html"'
                        .format(data=data, arch=architecture))
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
            places = {'data': data, 'bozo_target': bozo_target}
            for file_ in ['boot.key', 'boot.crt', 'cryptsetup.sh',
                          'index.html', 'activate.cgi', 'hiding.gif',
                          'make_bozo_dir.sh']:
                sudo('cp {data}/encrypted_root/{file} {bozo_target}/{file}'
                     .format(file=file_, **places))
            logger.info('Modifying scripts to match our volumes.....')
            sudo('sed -i "s/\/dev\/sda2/{root_device}/" '
                 '{work}/root/etc/initramfs-tools/hooks/cryptsetup'.format(
                 root_device=root_device, work=work))
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
                 #'update-initramfs -uk all\n'
                 'mv /etc/resolv.conf.old /etc/resolv.conf\n'
                 'umount /dev/pts\n'
                 'umount /proc\n'
                 'umount /sys\n'
                 'EOT'.format(work=work))
            logger.info('Shutting down temporary instance')
            sudo('shutdown -h now')


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


@task
def modify_instance_termination(region, instance_id):
    """Mark production instnaces as uneligible for termination.

    region
        name of region where instance is located;
    instance_id
        instance to be updated;

    You must change value of preconfigured tag_name and run this command
    before terminating production instance via API."""
    conn = get_region_conn(region)
    inst = get_inst_by_id(conn.region, instance_id)
    prod_tag = config.get(conn.region.name, 'TAG_NAME')
    prod_val = config.get(conn.region.name, 'TAG_VALUE')
    inst_tag_val = inst.tags.get(prod_tag)
    inst.modify_attribute('disableApiTermination', inst_tag_val == prod_val)


@task
def mount_snapshot(region_name=None, snap_id=None, inst_id=None):

    """Mount snapshot to temporary created instance or inst_id.

    region_name, snap_id
        specify snapshot. Will be prompted if `None`.
    inst_id
        attach to existing instance. Will be created temporary if
        None."""

    if not region_name or not snap_id:
        region_name, snap_id = select_snapshot()
    conn = get_region_conn(region_name)
    inst = get_inst_by_id(conn.region, inst_id) if inst_id else None
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id, ])[0]

    info = ('\nYou may now SSH into the {inst} server, using:'
            '\n ssh -i {key} {user}@{inst.public_dns_name}')
    with attach_snapshot(snap, inst) as (vol, mountpoint):
        if mountpoint:
            info += ('\nand browse snapshot, mounted at {mountpoint}.')
        else:
            info += ('\nand mount {device}. NOTE: device name may be '
                     'altered by system.')
        key_file = config.get(conn.region.name, 'KEY_FILENAME')
        inst = get_inst_by_id(conn.region, vol.attach_data.instance_id)
        logger.info(info.format(inst=inst, user=env.user, key=key_file,
            device=vol.attach_data.device, mountpoint=mountpoint))

        info = ('\nEnter FINISHED if you are finished looking at the '
                'backup and would like to cleanup: ')
        while raw_input(info).strip() != 'FINISHED':
            pass


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
        user_data = config.get('user_data', 'USER_DATA')
    except:
        user_data = None
    conn = get_region_conn(region_name)
    image = conn.get_all_images([ami_id])[0]
    inst_type = inst_type or get_descr_attr(image, 'Type') or 't1.micro'

    avail_grps = [sec.name for sec in conn.get_all_security_groups()]
    sec_grps = prompt_to_select(avail_grps, 'Select security groups',
                                multiple=True)
    wait_for(image, 'available')
    reservation = image.run(
        key_name=config.get(conn.region.name, 'KEY_PAIR'),
        security_groups=sec_grps,
        instance_type=inst_type,
        user_data=user_data)
    new_instance = reservation.instances[0]
    wait_for(new_instance, 'running')
    add_tags(new_instance, image.tags)
    modify_instance_termination(conn.region.name, new_instance.id)
    info = ('\nYou may now SSH into the {inst} server, using:'
            '\n ssh -i {key} {user}@{inst.public_dns_name}')
    key_file = config.get(conn.region.name, 'KEY_FILENAME')
    logger.info(info.format(inst=new_instance, user=env.user, key=key_file))
    return new_instance


@task
def create_ami(region=None, snap_id=None, force=None, root_dev='/dev/sda1',
               default_arch='x86_64', default_type='t1.micro'):
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
        Used only if ``force`` is "RUN"."""
    if not region or not snap_id:
        region, snap_id = select_snapshot()
    conn = get_region_conn(region)
    snap = conn.get_all_snapshots(snapshot_ids=[snap_id, ])[0]
    instance_id = get_snap_instance(snap)
    _device = get_snap_device(snap)
    snaps = conn.get_all_snapshots(owner='self')
    snapshots = [snp for snp in snaps if
        get_snap_instance(snp) == instance_id and
        get_snap_device(snp) != _device and
        abs(get_snap_time(snap) - get_snap_time(snp)) <= timedelta(minutes=10)]
    snapshot = sorted(snapshots, key=get_snap_time,
                      reverse=True) if snapshots else None
    # setup for building an EBS boot snapshot
    arch = get_descr_attr(snap, 'Arch') or default_arch
    kernel = config.get(conn.region.name, 'KERNEL' + arch.upper())
    dev = re.match(r'^/dev/sda$', _device)  # if our instance encrypted
    if dev:
        kernel = config.get(conn.region.name, 'KERNEL_ENCR_' + arch.upper())
    ebs = EBSBlockDeviceType()
    ebs.snapshot_id = snap_id
    ebs.delete_on_termination = True
    block_map = BlockDeviceMapping()
    block_map[_device] = ebs
    if snapshot:
        for s in snapshot:
            s_dev = get_snap_device(s)
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
        new_instance = launch_instance_from_ami(region, image.id,
                                                inst_type=instance_type)
    return image, new_instance


@task
def create_encrypted_instance(region_name, release='lucid', volume_size='8',
                             architecture='x86_64', type='t1.micro',
                             name='encr_root', pw1=None, pw2=None):
    """
    Creates ubuntu instance with luks-encryted root volume.

    .. note:: Snapshot replication to backup region with `rsync_region`
              and `rsync_snapshot` doesn't function for encrypted
              volumes. Due to this reason encrypted instance could be
              restored only within its region.

    region_name
        Region where you want to create instance;
    release
        Ubuntu release name (lucid or natty). "lucid" by default;
    volume_size
        Size of volume in Gb (always remember, that script creates boot volume
        with size 1Gb, so minimal size of whole volume is 3Gb (1Gb for /boot
        2Gb for /)). 8 by default;
    type
        Type of instance. 't1.micro' by default;
    name
        Name of luks encrypted volume. 'encr_root' by default;
    pw1, pw2
        You can specify passwords in parameters to suppress password prompt;

    To unlock go to https://ip_address_of_instance (only after reboot
    or shutdown).
    You can set up to 8 passwords. Defaut boot.key and boot.crt created
    for .amazonaws.com so must work for all instances. Process of
    creation is about 20 minutes long."""
    assert volume_size >= 3, '1 GiB for /boot and 2 GiB for /'
    conn = get_region_conn(region_name)

    with config_temp_ssh(conn) as key_filename:
        key_pair = os.path.splitext(os.path.split(key_filename)[1])[0]
        zn = conn.get_all_zones()[-1]
        ssh_grp = config.get('DEFAULT', 'SSH_SECURITY_GROUP')
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
                                  arch, dev, name, release, pw1, pw2)
            description = dumps({
            'Volume': vol.id,
            'Region': vol.region.name,
            'Device': '/dev/sda',
            'Type': type,
            'Arch': architecture,
            'Root_dev_name': '/dev/sda1',
            'Time': _now(),
            })
            snap = vol.create_snapshot(description)
            wait_for(snap, '100%', limit=20 * 60)
            vol.detach(force=True)
            wait_for(vol, 'available')
            vol.delete()
            img, new_instance = create_ami(region_name, snap.id, 'RUN')
            logger.info('\nTo unlock go to:\n   https://{0}\n'
                        .format(new_instance.public_dns_name))
            img.deregister()
            snap.delete()

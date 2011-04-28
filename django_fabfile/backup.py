'''
Use configuration file ~/.boto for storing your credentials as described
at http://code.google.com/p/boto/wiki/BotoConfig#Credentials

All other options will be saved in ./fabfile.cfg file.
'''

from time import sleep as _sleep

from boto.ec2 import connect_to_region as _connect_to_region


config_file = 'fabfile.cfg'


def _get_instance_by_id(region, instance_id):
    conn = _connect_to_region(region)
    res = conn.get_all_instances([instance_id,])
    assert len(res) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(res, instance_id))
    instances = res[0].instances
    assert len(instances) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(instances,
                                                              instance_id))
    return instances[0]


def create_snapshot(region, instance_id=None, instance=None, dev='/dev/sda1'):
    """Return newly created snapshot of specified instance device.

    Either `instance_id` or `instance` should be specified."""
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
    print 'Waiting for the {snap} to be completed...'.format(snap=snapshot)
    while snapshot.status != 'completed':
        print 'still {snap.status}...'.format(snap=snapshot)
        _sleep(5)
        snapshot.update()
    print 'done.'
    return snapshot


def backup_instance(region, instance_id):
    """Return list of created snapshots for specified instance."""
    instance = _get_instance_by_id(region, instance_id)
    for dev in instance.block_device_mapping:
        return create_snapshot(region, instance=instance, dev=dev)


def upload_snapshot_to_s3(region, snapshot_id, bucket):
    """`snapshot_id` will be used as filename in the `bucket`."""
    pass


def fetch_snapshot_from_s3(region, snapshot_id, bucket):
    """`snapshot_id` name will be searched withit the the `bucket`."""
    pass

'''
Use configuration file ~/.boto for storing your credentials as described
at http://code.google.com/p/boto/wiki/BotoConfig#Credentials

All other options will be saved in ./fabfile.cfg file.
'''

from time import sleep as _sleep

from boto.ec2 import connect_to_region as _connect_to_region


config_file = 'fabfile.cfg'


def create_snapshot(region, instance_id, dev='/dev/sda1'):
    """Return newly created snapshot of specified `instance_id`."""
    conn = _connect_to_region(region)
    res = conn.get_all_instances([instance_id,])
    assert len(res) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(res, instance_id))
    instances = res[0].instances
    assert len(instances) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(instances,
                                                              instance_id))
    inst = instances[0]
    vol_id = inst.block_device_mapping[dev].volume_id
    description = 'Created from {0} of {1}'.format(dev, inst.id)
    if inst.tags.get('Name'):
        description += ' named as {0}'.format(inst.tags['Name'])
    snapshot = conn.create_snapshot(vol_id, description)
    print 'Waiting for the {snap} to be completed...'.format(snap=snapshot)
    while snapshot.status != 'completed':
        print 'still {snap.status}...'.format(snap=snapshot)
        _sleep(4)
        snapshot.update()
    print 'done.'
    return snapshot


def upload_snapshot_to_s3(region, snapshot_id, bucket):
    """`snapshot_id` will be used as filename in the `bucket`."""
    pass


def fetch_snapshot_from_s3(region, snapshot_id, bucket):
    """`snapshot_id` name will be searched withit the the `bucket`."""
    pass

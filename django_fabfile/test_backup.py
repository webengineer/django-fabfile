import fudge
from backup import backup_instance, trim_snapshots, rsync_snapshot
#import datetime
#import utils
#import boto
#from boto import connect_ec2
#from utils import get_region_conn

#from boto.ec2.instance import Instance
#from boto.ec2.blockdevicemapping import BlockDeviceMapping
from boto.sqs import regions


#------------------------------------------------------------------------------
# Fake classes to isolate test method from outer space
# TODO replace following fakes to another file
#------------------------------------------------------------------------------
class Connection():
    def __init__(self, region_name):
        print '>>> Connection.__init__({0})'.format(region_name)
        self.region = region_name
        return
    

    def get_all_volumes(self, vol_id):
        """
        Fake - replacement for Connection.get_all_volumes()
        """
        _ret_val = vol_id
        print '>>> Connection.get_all_volumes({0})'.format(vol_id)
        print '... return {0}'.format(vol_id)
        return _ret_val


    def get_all_regions(self):
        """
        Fake - replacement for Connection.get_all_regions
        Returns list of AWS regions, using regular method 'boto.sqs.regions()'
        """
        _ret_val = regions()
        print '>>> Connection.get_all_regions()'
        print '... return {0}'.format(_ret_val)
        return _ret_val


    def get_all_snapshots(self, snapshot_ids=None,
                          owner=None, restorable_by=None,
                          filters=None):
        """
        Fake - replacement for Connection.get_all_snapshots
        """
        #_ret_val = fudge.Fake('Snapshot').provides('tags').returns(fudge.Fake('tags').has_attr(get='name'))
        #_ret_val = fudge.Fake('Snapshot').provides('tags').returns_fake().provides('get').returns(fudge.Fake('get').has_attr(name='name'))
        #_ret_val = 'Snapshot'
        _ret_val = Snapshot(self.region)
        print '>>> Connection.get_all_snapshots({0}, {1}, {2}, {3})'.format(snapshot_ids, owner, restorable_by, filters)
        print '... return {0}'.format(_ret_val)
        return [_ret_val,]


class Instance():
    """
    Fake - replacement for class 'boto.ec2.instance.Instance'
    """
    def __init__(self):
        print '>>> Instance.__init__()'
        self.block_device_mapping = {'dev-1': fudge.Fake('dev-1').has_attr(volume_id=['vol-11', 'vol-12']),
            'dev-2': fudge.Fake('dev-2').has_attr(volume_id=['vol-21', 'vol-22'])}
        return    


class Tags():
    #TODO fill up doc-stirng
    def __init__(self):
        print '>>> Tags.__init__()'
        return

    def get(self, name=None):
        if name == 'Name':
            _ret_val = 'fake:Name'
        else:
            _ret_val = None
            pass
        print '>>> Tags.get({0})'.format(name)
        print '... return {0}'.format(_ret_val)
        return _ret_val

class Snapshot():
    #TODO fill up doc-stirng
    def __init__(self, region):
        print '>>> Snapshot.__init__()'
        self.tags = Tags()
        self.volume_id = 'Volume ID'
        self.volume_size = 8
        self.description = 'Description'
        self.region = region
        self.status = 'not error'
        self.start_time = '2011-09-15T15:18:00.000Z'
        return

    def __getitem__(self, key):
        print '>>> Snapshot.__getitem__({0})'.format(key)
        return


def get_region_conn(region_name=None):
    """
    Fake - replacement for utils.get_region_conn()
    """
    conn = Connection(region_name)
    print '>>> get_region_conn({0})'.format(region_name)
    print '... return {0}'.format(conn)
    return conn#'fake:AWSConnection'


def get_inst_by_id(region_name, instance_id):
    """
    Fake - replacement for utils.get_inst_by_id()
    """
    instance = Instance()
    print '>>> get_inst_by_id({0}, {1})'.format(region_name, instance_id)
    print '... instance = {0}'.format(instance)
    print '... instance.block_device_mapping = {0}'.format(instance.block_device_mapping)
    return instance


def create_snapshot(vol, synchronously):
    """
    Fake - replacement for backup.create_snapshot
    """
    _ret_val = fudge.Fake('snap')
    print '>>> create_snapshot({0}, {1})'.format(vol, synchronously)
    print '... return {0}'.format(_ret_val)
    return _ret_val


def delete_broken_snapshots():
    """
    Fake - replacement for backup.delete_broken_snapshots()
    """
    print '>>> delete_broken_snapshots()'
    return


def _trim_snapshots(reg, dry_run):
    """
    Fake - replacement for backup._trim_snapshots()
    """
    print '>>> _trim_snapshots({0}, {1})'.format(reg, dry_run)
    return


def get_snap_device(snap):
    """
    Fake - replacement for utils.get_snap_device()
    """
    _ret_val = '/dev/sda'
    #_ret_val = '/dev/sdb'
    print '>>> get_snap_device({0})'.format(snap)
    print '... return {0}'.format(_ret_val)
    return _ret_val


#------------------------------------------------------------------------------
# Testing tasks
#------------------------------------------------------------------------------
@fudge.patch('backup.get_region_conn', 'backup.get_inst_by_id',
    'backup.create_snapshot')
def test_backup_instance(fakeMethod1, fakeMethod2, fakeMethod3):
    fakeMethod1.is_callable().calls(get_region_conn)
    fakeMethod2.is_callable().calls(get_inst_by_id)
    fakeMethod3.is_callable().calls(create_snapshot)

    instance = Instance()

    print "\nTEST 1 - backup.backup_instance(region_name)"
    try: backup_instance('us-east-1')
    except Exception, e: print 'STOP: ', e

    print "\nTEST 2 - backup.backup_instance(region_name, instance_id)"
    try: print backup_instance('us-east-1', 'i-12345678')
    except Exception, e: print 'STOP: ', e

    #TODO work out with such test issue
    print "\nTEST 3 - backup.backup_instance(region_name, instance)"
    try: print backup_instance('us-east-1', instance)
    except Exception, e: print 'STOP: ', e

    print "\nTEST 4 - backup.backup_instance(region_name, instance_id, synchronously)"
    try: print backup_instance('us-east-1', 'i-12345678', synchronously=True)
    except Exception, e: print 'STOP: ', e

    print "\nTEST 5 - backup.backup_instance(region_name, instance_id, instance)"
    try: print backup_instance('us-east-1', 'i-12345678', instance)
    except Exception, e: print 'STOP: ', e

    return


@fudge.patch('backup.get_region_conn', 'backup.delete_broken_snapshots', 'backup._trim_snapshots')
def test_trim_snapshots(fakeMethod1, fakeMethod2, fakeMethod3):
    fakeMethod1.is_callable().calls(get_region_conn)
    fakeMethod2.is_callable().calls(delete_broken_snapshots)
    fakeMethod3.is_callable().calls(_trim_snapshots)

    print "\nTEST 1 - backup.trim_snapshots(region_name)"
    try: trim_snapshots('us-east-1')
    except Exception, e: print 'STOP: ', e

    print "\nTEST 2 - backup.trim_snapshots()"
    try: trim_snapshots()
    except Exception, e: print 'STOP: ', e

    return


@fudge.patch('backup.get_region_conn', 'backup.get_snap_device')
def test_rsync_snapshot(fakeMethod1, fakeMethod2):
    #rsync_snapshot(src_region_name, snapshot_id, dst_region_name, src_inst=None, dst_inst=None)
    fakeMethod1.is_callable().calls(get_region_conn)
    fakeMethod2.is_callable().calls(get_snap_device)

    print "\nCALL for backup.rsync_snapshot(src_region_name, snapshot_id, dst_region_name)"
#    try: rsync_snapshot('us-east-1', 'snap-12345678', 'us-west-1')
#    except Exception, e: print 'STOP: ', e
    rsync_snapshot('us-east-1', 'snap-12345678', 'us-west-1')

    return


# Run testing Tasks
print '\nRUN test for backup.backup_instance'
test_backup_instance()

print '\nRUN test for backup.trim_snapshots'
test_trim_snapshots()

print '\nRUN test for backup.rsync_snapshot'
test_rsync_snapshot()


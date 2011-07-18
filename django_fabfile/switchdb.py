from boto.ec2 import (connect_to_region as _connect_to_region,
                      regions as _regions)
from fabric.api import env, settings, sudo, abort, put
from ConfigParser import ConfigParser as _ConfigParser
from re import match as _match

try:
    config_file = 'fabfile.cfg'
    config = _ConfigParser()
    config.read(config_file)
    primary = config.get('RDBMS', 'primary')
    secondary = config.get('RDBMS', 'secondary')
    username = config.get('DEFAULT', 'username')
except:
    primary = None
    secondary = None
    username = None

if username:
    env.update({'disable_known_hosts': True, 'user': username})

def _return(primary, secondary, node_id):
    env.update({'host_string': primary})
    sudo('su postgres -c "pcp_attach_node 60 127.0.0.1 9898 postgres'
          ' GKQUAzldPbsWsweC {node_id}"'.format(node_id=node_id))
    env.update({'host_string': secondary})
    sudo('su postgres -c "pcp_attach_node 60 127.0.0.1 9898 postgres'
          ' GKQUAzldPbsWsweC {node_id}"'.format(node_id=node_id))

def _failover(new_master_host_name, old_master_node, failed_node_id, 
                                                    master_node_id):
    trigger='/var/log/pgpool/trigger/trigger_file1'
    env.update({'host_string': new_master_host_name})
    sudo('su postgres -c "touch {trigger}"'.format(trigger=trigger))
    sudo('su postgres -c "/usr/local/etc/dnsmadeeasy-update.sh {new_master}"'
                                .format(new_master=new_master_host_name))
    sudo('su postgres -c "pcp_detach_node 60 127.0.0.1 9898 postgres'
          ' GKQUAzldPbsWsweC {node_id}"'.format(node_id=failed_node_id))
    sudo('su postgres -c "pcp_attach_node 60 127.0.0.1 9898 postgres'
          ' GKQUAzldPbsWsweC {node_id}"'.format(node_id=master_node_id))
    env.update({'host_string': old_master_node})
    sudo('su postgres -c "pcp_detach_node 60 127.0.0.1 9898 postgres'
          ' GKQUAzldPbsWsweC {node_id}"'.format(node_id=failed_node_id))
    sudo('su postgres -c "pcp_attach_node 60 127.0.0.1 9898 postgres'
          ' GKQUAzldPbsWsweC {node_id}"'.format(node_id=master_node_id))

def _recover(master,standby, recover_node_id):
    if master:
        env.update({'host_string': master})
    out = sudo('/etc/init.d/postgresql status | grep -c Running')
    if out=='1':
        sudo('/etc/init.d/postgresql stop')
    if standby:
        env.update({'host_string': standby})
    sudo('su postgres -c "pcp_recovery_node 60 127.0.0.1 9898 postgres'
          ' GKQUAzldPbsWsweC {node_id}"'.format(node_id=recover_node_id))
    if master:
        env.update({'host_string': master})
    sudo('su postgres -c "pcp_attach_node 60 127.0.0.1 9898 postgres'
          ' GKQUAzldPbsWsweC {node_id}"'.format(node_id=recover_node_id))

def switch_to_secondary():
    """
    If primary server failed, run this to promote secondary server
    from slave to master-role and detach failed node from pgpool
    """
    _failover(secondary, primary, 0, 1)

def recover_primary():
    """
    Run this to start recovery of failing primary node
    (run this strictly after switching_to_secondary)
    """
    _recover(primary, secondary, 0)
    
def return_primary():
    """
    Run this after temporary network issues in us-east-1
    It will reattach primary node to pgpool server
    """
    _return(primary, secondary, 0)
        
def switch_to_primary():
    """
    If secondary server failed, run this to promote primary server
    from slave to master-role and detach failed node from pgpool
    """
    _failover(primary, secondary, 1, 0)
          
def recover_secondary():
    """
    Run this to start recovery of failing secondary node
    (run this strictly after switching_to_primary)
    """
    _recover(secondary, primary, 1)
    
def return_secondary():
    """
    Run this after temporary network issues in eu-west-1
    It will reattach secondary node to pgpool server
    """
    _return(primary, secondary, 1)
    
    
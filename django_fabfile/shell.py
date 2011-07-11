"""
Add sudo shell access
"""
#!/usr/bin/python2.6

from __future__ import with_statement

import os.path
import sys

from StringIO import StringIO
from time import sleep

from fabric import utils as fabric_utils
from fabric.api import run, local, settings, env, cd, sudo
from fabric.contrib import files
from fab_deploy import utils

import boto
from boto.ec2 import connect_to_region


# Some global variables. May be changed
AWS_ACCESS_KEY = "AKIAI4XIBE35OJNLMY7A"
AWS_SECURITY_ACCESS_KEY = "/+SbXWz+0Qt2Oi3SOLBUiLh8/7nPSW0RJB3Run8o"
key_filename = "/home/copycat/.ssh/ec2-keypair"
host_username = "ubuntu"
#mount_point = ""
mount_point = "/mnt/snapshot"

log_output = StringIO()


def log(data, newline=True):
    """
    Simple logger
    """
    sys.stdout.write(data)
    log_output.write(data)

    if newline:
        sys.stdout.write('\n')
        log_output.write('\n')


@utils.run_as('root')
def add_sudo_shell_access(region='us-east-1', instance_id='i-efbc9181',
    pub_key_file='/home/copycat/.ssh/ec2-keypair', username='copycat'):
    """
    Creates Fabric command, which should add specified public key
    into authorized_keys file.
    Run as fab -f shell.py add_sudo_shell_access:[arg_list,...]
    """

    # Check the username
    log('>>>> Check the username...')
    username = username or env.conf.USER
    if username == 'root':
        return
    log('.... OK')

    # Create connection to AWS EC2
    log('>>>> Connecting to ec2.%s.amazonaws.com...' % region)
    #conn = boto.connect_ec2(AWS_ACCESS_KEY, AWS_SECURITY_ACCESS_KEY)
    conn = connect_to_region(region, aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECURITY_ACCESS_KEY)
    log('.... OK')

    # Check for volumes and their state
    log('>>>> Check for volumes...')
    vol = conn.get_all_volumes()
    log('.... OK')

    for v in vol:
        log('>>>> %s is %s and %s' % (v, v.update(), v.attachment_state()))
    log('.... OK')

    # Connecting to instance
    # For test purposes we have only one known instance (i-1f146671)
    log('>>>> Check for instances...')
    res = conn.get_all_instances()

    for r in res:
        log(">>>> %s is %s" % (r.instances[0], r.instances[0].update()))
    log('.... OK')

    #@@@@ Connect to instance with instance_id
    log('>>>> Connect to instance %s' % instance_id)
    try:
        res = conn.get_all_instances([instance_id, ])
    except:
        log('>>>> ERROR: The instance %s not found. Program will be terminated'
            % instance_id)
    else:
        inst = res[0].instances[0]
        log('.... OK')

    # Copying public key file
    log('>>>> Copying public key file...')
    with open(os.path.normpath(pub_key_file), 'rt') as f:
        ssh_key = f.read()
    log('.... OK')

    # Add user to groups (see adduser.py)
    # Copy adduser.py to the instance
    local('scp -i %s adduser.py %s@%s:/home/%s/' %
        (key_filename, host_username, inst.public_dns_name, host_username))

    # Adjust environment variables
    log('>>>> Open SSH connection to the instance %s' % inst)
    env.update({
        'host_string': inst.public_dns_name,
        'key_filename': key_filename,
        'load_known_hosts': False,
        'user': host_username,
    })

    # Open SSH connection to the instance
    while True:
        try:
            sudo('pwd')
            break
        except:
            log('>>>> sshd still launching, waiting to try again...')
            sleep(1)

    # Implement adduser.py in the instance
    # (username and mount_point as argument)
    sudo('python /home/%s/adduser.py %s %s/etc' %
        (host_username, username, mount_point))

    # Check out message, left by adduser.py in the instance:/tmp/adduser.msg
    if files.contains('/tmp/adduser.msg', 'already exists',
        exact=False, use_sudo=True):
        print('>>>> The user %s already exits. Terminating program' % username)
        # Clean up
        sudo('rm -rf /home/%s/adduser.py' % host_username)
        sudo('rm -f /tmp/adduser.msg')
        return
    else:
        UID = 1000
        print('>>>> Obtain UID')

        # Obtain assigned to username UID
        while not files.contains('/tmp/adduser.msg', str(UID),
            exact=False, use_sudo=True):
            print('>>>>')
            UID = UID + 1
        print ('.... OK %s') % UID

    # Create ../home/username in the instance
    home_dir = '%s/home/%s' % (mount_point, username)
    sudo('mkdir -p %s' % home_dir)

    # TODO Copy /../etc/skel to /../home/username

    with (settings(warn_only=True)):
        with cd(home_dir):
            sudo('mkdir -p .ssh')
            files.append('.ssh/authorized_keys', ssh_key, use_sudo=True)

            sudo('chown -R %s:%s %s' % (UID, UID, home_dir))
            sudo('chmod 700 .ssh')
            sudo('chmod -R 600 .ssh/authorized_keys')

        line = '%s ALL=(ALL) NOPASSWD: ALL' % username
        files.append('%s/etc/sudoers' % mount_point, line, use_sudo=True)

    # Clean up
    sudo('rm -rf /home/%s/adduser.py' % host_username)
    sudo('rm -f /tmp/adduser.msg')

    return

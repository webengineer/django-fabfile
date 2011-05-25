import os.path
from fabric.api import run, settings, env, cd, sudo

#@@@@ 
from backup import _get_instance_by_id

#@@@@ Added by asuvorov
def add_sudo_shell_access(region, instance_id=None, pub_key_file, username='ubuntu'):
    """ 
    Creates Fabric command, which should add specified public key into authorized_keys file.
    """
    
    #@@@@ Is this necessary?
    if username == 'root':
        return
    
#    if instance_id:
        instance = _get_instance_by_id(region, instance_id)

    if not instance:
        with open(os.path.normpath(pub_key_file), 'rt') as f:
            ssh_key = f.read()

        home_dir = '/home/%s' % username

        #@@@@ Ubuntu 10.04 Lucid, 32-bit, EBS boot in that region
        amiid=ami-cb97c68e

        with (settings(warn_only=True)):
            #@@@@ Add user to groups
            run('adduser %s ' % username)
            run('adduser %s adm' % username)
            run('adduser %s admin' % username)
            run('adduser %s staff' % username)

            #@@@@ Create an ext3 file system for the device attached at /dev/sdh
            run('mke2fs -F -j /dev/sdh')
            run('mkdir %s') % home_dir
            
            #@@@@ Mount the new file system at this directory            
            run('mount /dev/sdh %s' % home_dir)
        
            with cd(home_dir):
                run('mkdir .ssh')
                files.append('.ssh/authorized_keys', ssh_key)
                run('chown -R %s:%s .ssh' % (username, username))
                run('chmod 700 .ssh')
                run('chmod -R 600 .ssh/authorized_keys')

                #@@@@ some useful variables
                zone = '%sa' % region
                ssh_key='.ssh/authorized_keys'
                i_type = ''

                run('ec2-run-instances --key %s --availability-zone % %s' % (ssh_key, zone, amiid))
                #run('ec2-run-instances --key %s --region %s --availability-zone % --instance-type %s %s' % (ssh_key, region, zone, i_type, amiid))
    
            #line = '%s ALL=(ALL) NOPASSWD: ALL' % username
            #files.append('/etc/sudoers', line)

"""
#2470
Object of request for change:
> Amazon EC2 Ubuntu instances without available shell access can’t be used. Actually it’s a server without available login credentials. But filesystem still can be mounted to another instance. 

Goal of the request for change:
> ^mount volume to another instance
> ^modify filesystem of the mounted volume
> ^launch original instance with modified volume, where provided credentials can be applied.

Implementation details:
> Clone repository git clone gitolite@repo.odeskps.com:bootcamp/django-fabfile
> Create Fabric command django_fabfile.shell.add_sudo_shell_access with arguments region_name, instance_id, pub_key_file, username='ubuntu', which should add specified public key into authorized_keys file.

Command should create admin account with sudo privileges if no such user exists at the instance. Just FYI – admin accounts are created with following commands in generic case (cat addadmin.sh):

#!/bin/bash

username=${1?"Usage: $0 username"}

sudo adduser $username
sudo adduser $username adm
sudo adduser $username admin
sudo adduser $username staff

sudo mkdir /home/$username/.ssh
sudo vi /home/$username/.ssh/authorized_keys
sudo chown -R $username\: /home/$username/.ssh/
sudo chmod 700 /home/$username/.ssh
sudo chmod -R 600 /home/$username/.ssh/authorized_keys
"""

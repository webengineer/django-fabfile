#!/usr/bin/python2.6
"""
Script creates a new user in mounted FS
"""

import os
import sys
import time


# List of groups where to add username. May be changed
_group = ['adm', 'admin', 'staff']


def adduser(username='copycat', _etc_path='/mnt/snapshot/etc'):
    """
    Make changes into mounted FS - creates a new user.
    """

    def is_exist():
        """
        Look through the mounted file /etc/passwd to find
        if user already exists
        """
        global UID

        # First reasonable number for UID. Will be useful
        # to assign UID to username
        UID = 1000

        f = open('%s/passwd' % _etc_path, 'r')
        for _line in f.readlines():
            _list = _line.rsplit(':')

            if _list[0] == username:
                return True
            if UID == int(_list[2]):
                UID = UID + 1

        f.close()
        return False


    # Leave the message
    fd = os.open('/tmp/adduser.msg', os.O_CREAT | os.O_WRONLY)
    if is_exist():
        print ">>>> User %s already exists" % username
        os.write(fd, 'User %s already exists' % username)
        return
    else:
        print ('>>>> UID is %s' % str(UID))
        os.write(fd, '%s' % str(UID))
    os.close(fd)

    # Add username to /etc/passwd
    _list = [
        username, 'x', str(UID), str(UID), '', '/home/%s' % username,
        '/bin/bash']
    _line = ':'.join(_list)
    print _line

    f = open('%s/passwd' % _etc_path, 'a')
    f.write(_line)
    f.close()

    # Add username to /etc/group
    # Assume, that all groups in _group list already exist
    f1 = open('%s/group' % _etc_path, 'r')
    f2 = open('%s/group.tmp' % _etc_path, 'w+')

    for _line in f1.readlines():
        _list = _line.rsplit(':')

        if _list[0] in _group:
            if len(_list[3]) > 1:
                _list[3] = _list[3].rsplit('\n')[0] + ',' + username + '\n'
            else:
                _list[3] = _list[3].rsplit('\n')[0] + username + '\n'

        _line = ':'.join(_list)
        f2.write(_line)

    # Create new group 'username', GID = UID
    _list = [username, 'x', str(UID), '']
    _line = ':'.join(_list)
    print _line.rsplit('\n')[0]

    f2.write(_line)

    f2.close()
    f1.close()

    os.remove('%s/group' % _etc_path)
    os.rename('%s/group.tmp' % _etc_path, '%s/group' % _etc_path)

    # Add username to /etc/shadow
    # obtain number of days since 01.01.1970 till current date
    _TIME_STAMP = int(time.time() / 86400)

    _list = [username, '*', str(_TIME_STAMP), '0', '99999', '7', '', '', '']
    _line = ':'.join(_list)
    print _line

    f = open('%s/shadow' % _etc_path, 'a')
    f.write(_line)
    f.close()

    return

#Passing args, obtained from arguments list, to function
adduser(sys.argv[1], sys.argv[2])

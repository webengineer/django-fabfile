from fabric.api import env, settings, sudo, abort, put


def _sudo(cmd):
    """ Shows output of cmd and allows interaction """
    sudo(cmd, shell=False, pty=False)


def deluser(name):
    """ removes user <name> with deluser """
    env.username = name
    _sudo('deluser %(username)s' % env)


def adduser(ssh_key):
    """ :<username>.pub creates new <username> with public SSH key """
    from os.path import basename, splitext, isfile

    if not isfile(ssh_key):
        abort("%s does not exist" % ssh_key)
    env.ssh_key = ssh_key
    path, ext = splitext(ssh_key)
    env.username = basename(path)
    _sudo('adduser --disabled-password %(username)s' % env)
    for group in ['adm', 'admin', 'staff']:
        with settings(group=group):
            _sudo('adduser %(username)s %(group)s' % env)
    _sudo('echo -e "\n%(username)s ALL=(ALL) NOPASSWD:ALL\n" | sudo tee -a /etc/sudoers' % env)
    _sudo('mkdir -p /home/%(username)s/.ssh' % env)
    _sudo('touch /home/%(username)s/.ssh/authorized_keys' % env)
    _sudo('chown -R %(username)s: /home/%(username)s/.ssh' % env)
    _sudo('chmod 700 /home/%(username)s/.ssh' % env)
    put(ssh_key, '/home/%(username)s/.ssh/authorized_keys' % env, use_sudo=True)
    _sudo('chown -R %(username)s: /home/%(username)s/.ssh/authorized_keys' % env)
    _sudo('chmod 600 /home/%(username)s/.ssh/authorized_keys' % env)
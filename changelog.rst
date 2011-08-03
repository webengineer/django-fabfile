Version 2011.08.03.4
--------------------

Added `minutes_for_snap` option to `DEFAULT` section of config.

Version 2011.08.01.2
--------------------

Added `django_fabfile.backup.update_volumes_tags` for cloning tags from
instances.

Version 2011.08.01.1
--------------------

*XXX* Requirements updated with patched version of Fabric - please
install it from http://pypi.odeskps.com/simple/odeskps-fabric/ using::

    pip install odeskps-Fabric

Version 2011.07.26.1
--------------------

Added logging to file with rotation. Note: logging to a single file from
multiple processes is not supported.

Version 2011.07.24.1
--------------------

Added configuration option `username` in new `odesk` section.

Version 2011.07.21.1
--------------------

Added `django_fabfile.switchdb` module with commands for switching current
primary DB server.

Version 2011.07.18.1
--------------------

Added workaround with kernels for AMI creation to fix problems at instance boot
stage.

Fixed wrongly removed statement in `django_fabfile.backup.trim_snapshots`.

Version 2011.07.16.2
--------------------

Added `django_fabfile.backup.modify_kernel` command for make pv-grub working.

Version 2011.07.16.1
--------------------

Enabled volume deletion after termination for AMI, created by
`django_fabfile.backup.create_ami`.

Version 2011.06.28.1
--------------------

Added `adduser` and `deluser` commands to `django_fabfile.useradd` module.

Version 2011.06.25.2
--------------------

* Added `native_only` argument to the `django_fabfile.backup.rsync_region`
  function. With default value `True` it synchronze only locally created
  snapshots.

Version 2011.06.25.1
--------------------

* Added AMI creation

Please update your local version of fabfile.cfg:

* add `aki_ptrn` to `DEFAULT` section
* move `architecture`, `ami_ptrn`, `ami_ptrn_with_version`,
  `ami_ptrn_with_release_date`, `ami_regexp`, `ubuntu_aws_account`, `username`
  to `DEFAULT` section

Version 2011.06.19.1
--------------------

* Added configuration options `ssh_timeout_attempts` and
`ssh_timeout_interval`, responsible for iterations of sudo command.

Please update your local version of fabfile.cfg.

Version 0.9.6.5
---------------
**2011-05-17**
* *resolved #2269* - merged backup fabric scripts and added
`readme.rtf`.

Version 0.9.5.4
---------------

**2011-04-13**

* *resolved #616* - added backups mounting commands in separate fabfile
`mount_backup.py`.

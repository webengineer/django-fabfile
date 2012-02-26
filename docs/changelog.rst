Change Log
**********

Version 2012.02.26.1
--------------------

Added explicit exception for preventing `generator didn't yield`
`RuntimeError`.

Version 2012.02.23.1
--------------------

Increased new instance launch awaiting time in
:func:`django_fabfile.instances.launch_instance_from_ami` and
temporary volume availability awaiting time before deleting in
:func:`django_fabfile.instances.attach_snapshot`.

Version 2012.02.23.1
--------------------

Increased snapshot awaiting time in
:func:`django_fabfile.instances.attach_snapshot`.

Version 2012.02.20.1
--------------------

Fixed replication of existing snapshots that led to non-incremental snapshots.

Version 2012.02.19.1
--------------------

Preventing snapshot replication if it's already in process of
replication.

Version 2012.02.18.1
--------------------

Added :func:`django_fabfile.backup.get_oldest_replica` for filtering
snapshots without replicas with ordering by creation time.

:mod:`django_fabfile.backup` expects to locate `TAG_NAME` and `TAG_VALUE`
in the 'DEFAULT' section of `fabfile.cfg`.

Added `force` option to :func:`django_fabfile.backup.rsync_snapshot` for
enforcing old snapshots replication.

Version 2012.02.10.1
--------------------

Added :func:`django_fabfile.backup.rsync_all_regions` for replicating snapshots
across all available regions.

Version 2011.11.04.1
--------------------

Fixed error in :func:`django_fabfile.utils.get_inst_by_id` caused by distinct
responses from `boto` :func:`get_all_instances`.

Version 2011.10.19.1
--------------------

Introduced consistent snapshots (disabled by default) for preventing files
corruption that was caused by not synced and not locked filesystem. Filesystem
lock lasts 10 seconds.

Version 2011.09.27.1
--------------------

Added security groups replication
:func:`django_fabfile.security_groups.replicate_security_groups`.

Version 2011.09.21.1
--------------------

Added security groups synchronization
:func:`django_fabfile.security_groups.sync_rules` and corresponding
Fabric task :func:`django_fabfile.security_groups.sync_rules_by_id`.

Version 2011.09.15.1
--------------------

Accept ``security_groups`` argument only formatted as string separated
with semicolons ';'.

Version 2011.09.14.1
--------------------

* Updated default architecture (for newly created instances) from x86_64
  to i386 for economy reasons - them could be upgraded to less expensive
  types.
* Added per-instance security groups. List of instance's security groups
  will be kept in tag "Security Groups" and copied into snapshot during
  backup.
* Added :func:`django_fabfile.utils.cleanup_security_groups` Fabric task
  for deleting unused Security Groups.

Version 2011.09.06.2
--------------------

Fixed snapshots trimming on 31st days of months.

Version 2011.09.02.1
--------------------

Added support for transferring encrypted snapshots between regions.

Version 2011.08.26.1
--------------------

Updated package and configuration layout.

.. note:: sporadical errors "SSH session not active" (see
   https://github.com/fabric/fabric/issues/402 for more details) could
   be fixed by using patched version of Fabric
   https://github.com/webengineer/fabric/tree/fix-ssh-session-not-active.

Version 2011.08.25.2
--------------------

Updated config file parsing - only options to override should be mentioned in
local `fabfile.cfg` for `django_fabfile.backup` module.

Version 2011.08.25.1
--------------------

Added instance encryption support with `create_encrypted_instance` task.
Encrypted instance could not be replicated to backup region yet - it could be
restored from snapshots only within its region. Support for encrypted instance
replication to backup region could be added in future.

Version 2011.08.23.1
--------------------

Added AMI assembling from two or more snapshots (see
http://redmine.odeskps.com/issues/2843 for details).

Version 2011.08.10.1
--------------------

Changed snapshots creation with function
``django_fabfile.backup.backup_instances_by_tag`` to wait for successful
completion in order to avoid snapshots with status "error".

Version 2011.08.08.1
--------------------

Updated logging setup with option ``logging_folder``.

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

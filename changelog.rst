Version 2011.06.25.1
--------------------

* Added AMI creation

Please update your local version of fabfile.cfg:

* add `aki_ptrn` to `DEFAULT` section
* move `architecture`, `ami_ptrn`, `ami_ptrn_with_version`,
`ami_ptrn_with_release_date`, `ami_regexp`, `tag_name`, `tag_value`,
`ubuntu_aws_account`, `username` to `DEFAULT` section

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

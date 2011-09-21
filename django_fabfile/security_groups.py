from collections import defaultdict
from contextlib import contextmanager
from itertools import chain
import logging

from boto.exception import EC2ResponseError
from fabric.api import task

from django_fabfile.utils import get_region_conn, timestamp


logger = logging.getLogger(__name__)


def new_security_group(region, name=None, description=None):
    """Create Security Groups with SSH access."""
    s_g = get_region_conn(region.name).create_security_group(
        name or 'Created on {0}'.format(timestamp()),
        description or 'Created for using with specific instance')
    s_g.authorize('tcp', 22, 22, '0.0.0.0/0')
    return s_g


@task
def cleanup_security_groups(delete=False):
    """
    Delete unused AWS Security Groups.

    :type delete: boolean
    :param delete: notify only (i.e. False) by default.

    If security group with the same name is used at least in one region,
    it is treated as used.
    """
    groups = defaultdict(lambda: {})
    used_groups = set(['default', ])
    regions = get_region_conn().get_all_regions()
    for reg in regions:
        for s_g in get_region_conn(reg.name).get_all_security_groups():
            groups[s_g.name][reg] = s_g
            if s_g.instances():     # Security Group is used by instance.
                used_groups.add(s_g.name)
            for rule in s_g.rules:
                for grant in rule.grants:
                    if grant.name and grant.owner_id == s_g.owner_id:
                        used_groups.add(s_g.name)   # SG is used by group.
    for grp in used_groups:
        del groups[grp]

    for grp in sorted(groups):
        if delete:
            for reg in groups[grp]:
                s_g = groups[grp][reg]
                logger.info('Deleting {0} in {1}'.format(s_g, reg))
                s_g.delete()
        else:
            msg = '"SecurityGroup:{grp}" should be removed from {regs}'
            logger.info(msg.format(grp=grp, regs=groups[grp].keys()))


def regroup_rules(security_group):
    grouped_rules = defaultdict(lambda: [])
    for rule in security_group.rules:
        ports = rule.ip_protocol, rule.from_port, rule.to_port
        for grant in rule.grants:
            grouped_rules[ports].append(grant)
    return grouped_rules


def sync_rules(src_grp, dst_grp):
    """
    Copy Security Group rules.

    Works across regions as well. The sole exception is granted groups,
    owned by another user - such groups can't be copied recursively.
    """

    def is_group_in(region, group_name):
        try:
            get_region_conn(region.name).get_all_security_groups([group_name])
        except EC2ResponseError:
            return False
        else:
            return True

    src_rules = regroup_rules(src_grp)
    # Assure granted group represented in destination region.
    src_grants = chain(*src_rules.values())
    for grant in dict((grant.name, grant) for grant in src_grants).values():
        if grant.name and grant.owner_id == src_grp.owner_id:
            if not is_group_in(dst_grp.region, grant.name):
                src_conn = get_region_conn(src_grp.region.name)
                grant_grp = src_conn.get_all_security_groups([grant.name])[0]
                dst_conn = get_region_conn(dst_grp.region.name)
                grant_copy = dst_conn.create_security_group(
                    grant_grp.name, grant_grp.description)
                sync_rules(grant_grp, grant_copy)
    dst_rules = regroup_rules(dst_grp)
    # Remove rules absent in src_grp.
    for ports in set(dst_rules.keys()) - set(src_rules.keys()):
        for grant in dst_rules[ports]:
            args = ports + ((None, grant) if grant.name else (grant, None))
            dst_grp.revoke(*args)
    # Add rules absent in dst_grp.
    for ports in set(src_rules.keys()) - set(dst_rules.keys()):
        for grant in src_rules[ports]:
            if grant.name and not is_group_in(dst_grp.region, grant.name):
                continue    # Absent other's granted group.
            args = ports + ((None, grant) if grant.name else (grant, None))
            dst_grp.authorize(*args)
    # Refresh `dst_rules` from updated `dst_grp`.
    dst_rules = regroup_rules(dst_grp)

    @contextmanager
    def patch_grouporcidr():
        """XXX Patching `boto.ec2.securitygroup.GroupOrCIDR` cmp and hash."""
        from boto.ec2.securitygroup import GroupOrCIDR
        original_cmp = getattr(GroupOrCIDR, '__cmp__', None)
        GroupOrCIDR.__cmp__ = lambda self, other: cmp(str(self), str(other))
        original_hash = GroupOrCIDR.__hash__
        GroupOrCIDR.__hash__ = lambda self: hash(str(self))
        try:
            yield
        finally:
            if original_cmp:
                GroupOrCIDR.__cmp__ = original_cmp
            else:
                del GroupOrCIDR.__cmp__
            GroupOrCIDR.__hash__ = original_hash

    # Sync grants in common rules.
    with patch_grouporcidr():
        for ports in src_rules:
            # Remove grants absent in src_grp rules.
            for grant in set(dst_rules[ports]) - set(src_rules[ports]):
                args = ports + ((None, grant) if grant.name else (grant, None))
                dst_grp.revoke(*args)
            # Add grants absent in dst_grp rules.
            for grant in set(src_rules[ports]) - set(dst_rules[ports]):
                if grant.name and not is_group_in(dst_grp.region, grant.name):
                    continue    # Absent other's granted group.
                args = ports + ((None, grant) if grant.name else (grant, None))
                dst_grp.authorize(*args)


@task
def sync_ryles_by_id(src_reg_name, src_grp_id, dst_reg_name, dst_grp_id):
    """Update Security Group rules from other Security Group.

    Works across regions as well. The sole exception is granted groups,
    owned by another user - such groups can't be copied.

    :param src_reg_name: region name
    :type src_reg_name: str
    :param src_grp_id: group ID
    :type src_grp_id: str
    :param dst_reg_name: region name
    :type dst_reg_name: str
    :param dst_grp_id: group ID
    :type dst_grp_id: str"""
    src_grp = get_region_conn(src_reg_name).get_all_security_groups(
        filters={'group-id': src_grp_id})
    dst_grp = get_region_conn(dst_reg_name).get_all_security_groups(
        filters={'group-id': dst_grp_id})
    sync_rules(src_grp, dst_grp)

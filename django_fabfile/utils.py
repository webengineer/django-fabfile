"""Check :doc:`README` or :class:`django_fabfile.utils.Config` docstring
for setup instructions."""

from ConfigParser import SafeConfigParser
from contextlib import contextmanager
from datetime import datetime
from json import loads
import logging
import os
from pprint import PrettyPrinter
from pydoc import pager
import re
from time import sleep
from traceback import format_exc

from boto import BotoConfigLocations, connect_ec2
from boto.ec2 import regions
from fabric.api import prompt, sudo, task
from fabric.contrib.files import exists
from pkg_resources import resource_stream

from django_fabfile import __name__ as pkg_name


logger = logging.getLogger(__name__)


def timestamp():
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')


class Config(object):

    """Make use from Django settings or local config file.

    Django settings will be checked out if environment variable
    `DJANGO_SETTINGS_MODULE` configured properly. If not configured
    within Django settings, then options will be taken from
    ./fabfile.cfg file - copy-paste rows that should be overriden from
    :download:`django_fabfile/fabfile.cfg.def
    <../django_fabfile/fabfile.cfg.def>`."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Config, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        self.fabfile = SafeConfigParser()
        self.fabfile.read(BotoConfigLocations)
        self.fabfile.readfp(resource_stream(pkg_name, 'fabfile.cfg.def'))
        self.fabfile.read('fabfile.cfg')

    def get_from_django(self, option, section='DEFAULT'):
        if os.environ.get('DJANGO_SETTINGS_MODULE'):
            try:
                from django.conf import settings
                return settings.FABFILE[section][option]
            except:
                pass

    def get(self, section, option):
        return (self.get_from_django(option, section) or
                self.fabfile.get(section, option))

    def getboolean(self, section, option):
        return (self.get_from_django(option, section) or
                self.fabfile.getboolean(section, option))

    def getfloat(self, section, option):
        return (self.get_from_django(option, section) or
                self.fabfile.getfloat(section, option))

    def getint(self, section, option):
        return (self.get_from_django(option, section) or
                self.fabfile.getint(section, option))

    def get_creds(self):
        return dict(
            aws_access_key_id=self.get('Credentials', 'AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=self.get('Credentials',
                                           'AWS_SECRET_ACCESS_KEY'))


config = Config()


def get_region_conn(region_name=None):
    """Connect to partially spelled `region_name`.

    Return connection to default boto region if called without
    arguments.

    :param region_name: may be spelled partially."""
    creds = config.get_creds()
    if region_name:
        matched = [reg for reg in regions(**creds) if re.match(region_name,
                                                               reg.name)]
        assert len(matched) > 0, 'No region matches {0}'.format(region_name)
        assert len(matched) == 1, 'Several regions matches {0}'.format(
            region_name)
        return matched[0].connect(**creds)
    else:
        return connect_ec2(**creds)


def prompt_to_select(choices, query='Select from', paging=False,
                     multiple=False):
    """Prompt to select an option from provided choices.

    Return solely possible value instantly without prompting.

    :param choices: if dict, then choice will be made among keys.
    :type choices: list or dict
    :param paging: render long list with pagination."""
    keys = list(choices)
    while keys.count(None):
        keys.pop(choices.index(None))    # Remove empty values.
    assert len(keys), 'No choices provided'
    if len(keys) == 1:
        return keys[0]

    def in_list(input_, avail_list, multiple=False):
        selected_list = re.split('[\s,]+', input_)
        if not multiple:
            assert len(selected_list) == 1, 'Only one item allowed'
        for item in selected_list:
            if not item in avail_list:
                raise ValueError('{0} not in {1}'.format(item, avail_list))
        return selected_list if multiple else selected_list[0]

    if paging:
        pp = PrettyPrinter()
        pager(query + '\n' + pp.pformat(choices))
        text = 'Enter your choice or press Return to view options again'
    else:
        text = '{query} {choices}'.format(query=query, choices=choices)
    input_in_list = lambda input_: in_list(input_, choices, multiple)
    return prompt(text, validate=input_in_list)


class StateNotChangedError(Exception):

    def __init__(self, state):
        self.state = state

    def __str__(self):
        return 'State remain {0} after limited time gone'.format(self.state)


def wait_for(obj, state, attrs=None, max_sleep=30, limit=5 * 60):
    """Wait for attribute to go into state.

    :param attrs: nested attribute names.
    :type attrs: list"""

    def get_state(obj, attrs=None):
        obj_state = obj.update()
        if not attrs:
            return obj_state
        else:
            attr = obj
            for attr_name in attrs:
                attr = getattr(attr, attr_name)
            return attr
    logger.debug('Calling {0} updates'.format(obj))
    for i in range(10):     # Resource may be reported as "not exists"
        try:                # right after creation.
            obj_state = get_state(obj, attrs)
        except Exception as err:
            logger.debug(str(err))
        else:
            break
    logger.debug('Called {0} update'.format(obj))
    obj_region = getattr(obj, 'region', None)
    logger.debug('State fetched from {0} in {1}'.format(obj, obj_region))
    if obj_state != state:
        if obj_region:
            info = 'Waiting for the {obj} in {obj.region} to be {state}...'
        else:
            info = 'Waiting for the {obj} to be {state}...'
        logger.info(info.format(obj=obj, state=state))
        slept, sleep_for = 0, 3
        while obj_state != state and slept < limit:
            logger.info('still {0}...'.format(obj_state))
            sleep_for = sleep_for + 5 if sleep_for < max_sleep else max_sleep
            sleep(sleep_for)
            slept += sleep_for
            obj_state = get_state(obj, attrs)
        if obj_state == state:
            logger.info('done.')
        else:
            raise StateNotChangedError(obj_state)


class WaitForProper(object):

    """Decorate consecutive exceptions eating.

    >>> @WaitForProper(attempts=3, pause=5)
    ... def test():
    ...     1 / 0
    ...
    >>> test()
    ZeroDivisionError('integer division or modulo by zero',)
     waiting next 5 sec (2 times left)
    ZeroDivisionError('integer division or modulo by zero',)
     waiting next 5 sec (1 times left)
    ZeroDivisionError('integer division or modulo by zero',)
    """

    def __init__(self, attempts=10, pause=10):
        self.attempts = attempts
        self.pause = pause

    def __call__(self, func):

        def wrapper(*args, **kwargs):
            attempts = self.attempts
            while attempts > 0:
                attempts -= 1
                try:
                    return func(*args, **kwargs)
                except BaseException as err:
                    logger.debug(format_exc())
                    logger.error(repr(err))

                    if attempts > 0:
                        logger.info('waiting next {0} sec ({1} times left)'
                            .format(self.pause, attempts))
                        sleep(self.pause)
                else:
                    break
        return wrapper

ssh_timeout_attempts = config.getint('DEFAULT', 'SSH_TIMEOUT_ATTEMPTS')
ssh_timeout_interval = config.getint('DEFAULT', 'SSH_TIMEOUT_INTERVAL')
wait_for_exists = WaitForProper(attempts=ssh_timeout_attempts,
                                pause=ssh_timeout_interval)(exists)
wait_for_sudo = WaitForProper(attempts=ssh_timeout_attempts,
                              pause=ssh_timeout_interval)(sudo)


def add_tags(res, tags):
    for tag in tags:
        if tags[tag]:
            res.add_tag(tag, tags[tag])
    logger.debug('Tags added to {0}'.format(res))


def get_descr_attr(resource, attr):
    try:
        return loads(resource.description)[attr]
    except:
        pass


def get_snap_vol(snap):
    return get_descr_attr(snap, 'Volume') or snap.volume_id


def get_snap_instance(snap):
    return get_descr_attr(snap, 'Instance')


def get_snap_device(snap):
    return get_descr_attr(snap, 'Device')


def get_snap_time(snap):
    for format_ in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f'):
        try:
            return datetime.strptime(get_descr_attr(snap, 'Time'), format_)
        except (TypeError, ValueError):
            continue
    # Use attribute if can't parse description.
    return datetime.strptime(snap.start_time, '%Y-%m-%dT%H:%M:%S.000Z')


def get_inst_by_id(region, instance_id):
    res = get_region_conn(region.name).get_all_instances([instance_id, ])
    assert len(res) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(res,
                                                      instance_id))
    instances = res[0].instances
    assert len(instances) == 1, (
        'Returned more than 1 {0} for instance_id {1}'.format(instances,
                                                              instance_id))
    return instances[0]


@task
def update_volumes_tags(filters=None):
    """Clone tags from instances to volumes.

    :param filters: apply optional filtering for the
                    :func:`django_fabfile.utils.get_all_instances`.
    """
    for region in regions():
        reservations = get_region_conn(region.name).get_all_instances(
            filters=filters)
        for res in reservations:
            inst = res.instances[0]
            for bdm in inst.block_device_mapping.keys():
                vol_id = inst.block_device_mapping[bdm].volume_id
                vol = inst.connection.get_all_volumes([vol_id])[0]
                add_tags(vol, inst.tags)


@contextmanager
def config_temp_ssh(conn):
    config_name = '{region}-temp-ssh-{now}'.format(
        region=conn.region.name, now=timestamp())
    key_pair = conn.create_key_pair(config_name)
    key_filename = key_pair.name + '.pem'
    key_pair.save('./')
    os.chmod(key_filename, 0600)
    try:
        yield os.path.realpath(key_filename)
    finally:
        key_pair.delete()
        os.remove(key_filename)


def new_security_group(region, name=None, description=None):
    return get_region_conn(region.name).create_security_group(
        name or 'Created on {0}'.format(timestamp()),
        description or 'Created for using with specific instance')

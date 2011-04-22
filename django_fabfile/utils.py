from ConfigParser import ConfigParser as _ConfigParser

from fabric.api import prompt


def _config_get_or_set(filename, option, section='DEFAULT', default_value=None,
                       info=None):

    """Open config `filename` and try to get `option` value.

    If no `default_value` provided, prompt user with `info`."""

    config = _ConfigParser()
    config.read(filename)
    if not config.has_option(section, option):
        if (not section in config.sections() and
            not section.lower() == 'default'):
            config.add_section(section)
        if default_value is not None:
            config.set(section, option, default_value)
        else:
            value = prompt(info or 'Please enter {0} for {1}'.format(option,
                                                                     section))
            config.set(section, option, value)
        with open(filename, 'w') as f_p:
            config.write(f_p)
    return config.get(section, option)

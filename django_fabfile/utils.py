from re import match as _match

from boto.ec2 import (regions as _regions)


def get_region_by_name(region_name):
    """Allow to specify boto region name fuzzyly."""
    matched = [reg for reg in _regions() if _match(region_name, reg.name)]
    assert len(matched) > 0, 'No region matches {0}'.format(region_name)
    assert len(matched) == 1, 'Several regions matches {0}'.format(region_name)
    return matched[0]

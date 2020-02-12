import requests
from dateutil.parser import parse


def check_url_filters(url):
    """
    Checks if the url has any filters i.e. query parameter
    :param url:
    :return:
    """
    _filters = requests.utils.urlparse(url).query
    if not _filters.strip():
        return False
    else:
        return True


def check_ga_query_res(data):
    """
    GA query results is dictionary containing rows
    If the res
    check if rows in the result and rows is not emtpty
    :param data: dictionary
    :return: boolean
    """
    if "rows" in data and data.get("rows"):
        return True
    else:
        return False


def check_resource_views_filters(query):
    """
    Function:
        - Do not consider
    :param query: query striong
    :return: Boolean
    """
    _query_items = (
        'resource_view',  # This is for resource view do not consider this
        'filters'  # This is from profiles filter do not consider this
    )

    qs = query.split("=")
    for _q in _query_items:
        if _q in qs:
            return True
    return False


def validate_date(key, data_dict):
    """
    Validate the date format
    :param key: from_dt or to_dt
    :param data_dict: dict
    :param errors: dict
    :return: raises error
    """
    if not data_dict.get(key):
        raise ValueError
    parse(data_dict.get(key))


def check_date_period(data_dict, errors, limit=184):
    """
    Check the dates from and two dates. Allowed period is only for 6 months period.
    :param data_dict: dict
    :param errors: dict
    :param limit: no of days (6 months nearly 182 days)
    :return: errors
    """

    from_dt = parse(data_dict.get('from_dt'))
    to_dt = parse(data_dict.get('to_dt'))

    if from_dt >= to_dt:
        errors['from_dt'] = ["From date greater than to date"]
        return errors

    _period = (from_dt - to_dt).days

    if _period > limit:
        errors['from_dt'] = ["Exceeded the limit"]
        errors['to_dt'] = ["Exceeded the limit"]
        return errors

    return errors

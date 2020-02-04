import requests


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


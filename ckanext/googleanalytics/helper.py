import ckan.model as model
from ckan.common import config
import logging

log = logging.getLogger(__name__)


def get_recent_runs(limit=10, offset=0):
    """
    Get all unique run ids. The data should inculde from and to date
    If the site code is given extract suitable runs for the current site
    Also can be utilised this for pagination with limit and offset.
    :param limit: limit
    :param offset: offset
    :return:
    """
    query = """
            SELECT * FROM (SELECT run_id, from_date, to_date, 
            time_created, site_code, ROW_NUMBER() OVER(PARTITION BY run_id ORDER BY time_created DESC) AS row_num 
            from ga_report_package) as ga_pkg  where ga_pkg.row_num = 1 
            ORDER BY ga_pkg.time_created DESC
            LIMIT {} OFFSET {};
            """.format(limit, offset)
    conn = model.Session.connection()
    res = conn.execute(query).fetchall()

    return res


def all_sites_label_mapping():
    """
    Get all the sites and its title mapping
    :return: dict
    """
    mapping = {
        "odm": "OpenDevelopment Mekong",
        "odmy": "OpenDevelopment Myanmar",
        "odt": "OpenDevelopment Thailand",
        "odv": "OpenDevelopment Vietnam",
        "odc": "OpenDevelopment Cambodia",
        "odl": "OpenDevelopment Laos",
    }

    return mapping


def get_ga_select_form_options():
    """
    Prepare select form for the GA page
    :return: list
    """
    _all_sites = all_sites_label_mapping()
    options = []
    for site_code in _all_sites:
        options.append({"value": site_code, "text": _all_sites.get(site_code)})

    options.append({"value": "all_sites", "text": "All sites"})

    return options

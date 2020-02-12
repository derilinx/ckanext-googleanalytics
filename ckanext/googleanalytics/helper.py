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
    _site_code = config.get('ckanext.odm.site_code', '')
    if _site_code:
        query = """
                SELECT * FROM (SELECT run_id, from_date, to_date, 
                time_created, site_code, ROW_NUMBER() OVER(PARTITION BY run_id ORDER BY time_created DESC) AS row_num 
                from ga_report_package) as ga_pkg  where ga_pkg.row_num = 1 and site_code={} 
                ORDER BY ga_pkg.time_created DESC 
                LIMIT {} OFFSET {};
                """.format("'"+_site_code+"'", limit, offset)
    else:
        query = """
                SELECT * FROM (SELECT run_id, from_date, to_date, 
                time_created, site_code, ROW_NUMBER() OVER(PARTITION BY run_id ORDER BY time_created DESC) AS row_num 
                from ga_report_package) as ga_pkg  where ga_pkg.row_num = 1 
                ORDER BY ga_pkg.time_created 
                LIMIT {} OFFSET {};
                """.format(limit, offset)
    conn = model.Session.connection()
    res = conn.execute(query).fetchall()

    return res

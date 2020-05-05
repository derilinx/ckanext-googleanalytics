from ckanext.googleanalytics.ga_auth import GoogleAnalyticsCredentialsObject
from ckanext.googleanalytics.ga.commands import ga_report
from ckanext.googleanalytics import helper as ga_h
import logging

log = logging.getLogger(__name__)


def _run_ga(st_dt, ed_dt, site_code):
    """
    pass
    :return:
    """
    try:
        log.info("Running GA report for site: {}".format(site_code))
        service = GoogleAnalyticsCredentialsObject(site_code=site_code)
        service()
        ga_report(service, start_date=st_dt, end_date=ed_dt, site_code=site_code)
    except Exception as e:
        # ODC GA credentials not working
        log.error("Error for fetching GA data for site: {}".format(site_code))
        pass


def run_ga(st_dt, ed_dt, site_code):
    """
    This is to wrap the class with function. Jobs cannot pickle the class object
    :param st_dt: str
    :param ed_dt: str
    :param site_code: str
    :return: None
    """
    if site_code == "all_sites":
        for site_code in ga_h.all_sites_label_mapping():
            _run_ga(st_dt, ed_dt, site_code)
    else:
        _run_ga(st_dt, ed_dt, site_code)

    return None

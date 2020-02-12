from ckanext.googleanalytics.ga_auth import GoogleAnalyticsCredentialsObject
from ckanext.googleanalytics.ga.commands import ga_report


def run_ga(st_dt, ed_dt):
    """
    This is to wrap the class with function. Jobs cannot pickle the class object
    :param st_dt: str
    :param ed_dt: str
    :return: None
    """
    service = GoogleAnalyticsCredentialsObject()
    service()
    ga_report(service, start_date=st_dt, end_date=ed_dt)

    return None

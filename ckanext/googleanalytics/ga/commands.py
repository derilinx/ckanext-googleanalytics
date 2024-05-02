import json
from dateutil.parser import parse as date_time_parse
from ckanext.googleanalytics.ga import stats
import datetime
import re

from ckan.plugins import toolkit

import logging

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

log = logging.getLogger(__name__)

# This also should go to config.
# TODO: Don't know how...??
DATASET_URL_REGEX = re.compile('(.*)/{}/([a-z0-9-_]+)/resource/([a-z0-9-_]+)')
MAPS_URL_REGEX = re.compile('.*/map/([a-z0-9-_]+)')
PROFILES_URL_REGEX = re.compile('.*/profile/([a-z0-9-_]+)')


# This goes to config
EVENT_CATEGORIES_ACTION = "Resource:Download"

# This should go to config
try:
    from ckanext import odm_profile
    DATASET_TYPES = "dataset laws_record agreement library_record map profile"
except:
    DATASET_TYPES = "dataset"


def get_data_for_events(client, property_id, event_category, event_action, from_date, to_date):
    date_range = DateRange(start_date=from_date, end_date=to_date)
    
    metrics = [Metric(name="eventCount")]
    dimensions = [Dimension(name="eventName"), Dimension(name="deviceCategory")]
    
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[date_range],
        metrics=metrics,
        dimensions=dimensions
    )
    
    response = client.run_report(request)
    return response.rows


def get_page_view_query_data(client, property_id, from_date, to_date):
    date_range = DateRange(start_date=from_date, end_date=to_date)
    
    metrics = [Metric(name="eventCount"), Metric(name="totalUsers"),
               Metric(name="newUsers"), Metric(name="screenPageViews")]
    dimensions = [Dimension(name="pagePath")]
    
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[date_range],
        metrics=metrics,
        dimensions=dimensions
    )
    
    response = client.run_report(request)
    return response.rows

def ga_report(client, property_id, start_date, end_date, site_code=None):
    if not (end_date >= start_date):
        raise Exception("End date must be greater than or equal to start date")
    
    data_types = ["dataset", "api_record"]
    log.info(data_types)
    ga_views = stats.GoogleAnalyticsViews(toolkit.config, start_date, end_date, site_code=site_code)
    
    for _type in data_types:
        page_view_data = get_page_view_query_data(client, property_id, start_date, end_date)
        ga_views.ga_pageviews(_type, page_view_data)
    
    ga_views.save_to_db("ga_report_package", "dataset_views")
    ga_views.save_to_db("ga_report_resource", "resource_views")
    ga_views.save_to_db("ga_report_resource", "downloads")


    if EVENT_CATEGORIES_ACTION:
        events = EVENT_CATEGORIES_ACTION.split(" ")

        for event in events:
            _category, _action = event.split(":")
        ga_evt_res = get_data_for_events(client, property_id, _category, _action, start_date, end_date)
        if _action.lower() == "download":

            ga_views.ga_download_event(_action, ga_evt_res)
        else:
            log.error("Parser is not defined for the event: {}, hence ignoring".format(_action))
            log.info("Parser is not defined for the event: {}, hence ignoring".format(_action))
        log.info("Finished %s info", ga_evt_res)
        ga_views.save_to_db("ga_report_events", "events")
        log.info(ga_views)

    log.info("GA report generation completed.")

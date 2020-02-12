import json
from dateutil.parser import parse as date_time_parse
from ckanext.googleanalytics.ga import stats
import datetime
import re

import logging

log = logging.getLogger(__name__)

# This also should go to config.
# TODO: Don't know how...??
DATASET_URL_REGEX = re.compile('(.*)/{}/([a-z0-9-_]+)/resource/([a-z0-9-_]+)')
MAPS_URL_REGEX = re.compile('.*/map/([a-z0-9-_]+)')
PROFILES_URL_REGEX = re.compile('.*/profile/([a-z0-9-_]+)')


# This goes to config
EVENT_CATEGORIES_ACTION = "Resource:Download"

# This should go to config
DATASET_TYPES = "dataset laws_record agreement library_record map profile"


def get_data_for_events(self, event_category, event_action, from_date=None, to_date=None,
                        start_index=1, max_results=10000, metrics=None, dimensions=None):
    """
    Gets the event data given category and action

    :param self: CKAN command instance
    :param event_category: str category
    :param event_action: str action
    :param from_date: str date
    :param to_date: str date
    :param start_index: default
    :param max_results: default
    :param metrics: default
    :param dimensions: default
    :return: dict GA data
    """

    if not metrics:
        metrics = "ga:uniqueEvents"

    if not dimensions:
        dimensions = "ga:eventCategory,ga:eventAction,ga:eventLabel"

    query = 'ga:eventCategory=={};ga:eventAction=={}'.format(event_category, event_action)

    result = self.service.data().ga().get(ids='ga:%s' % self.profile_id,
                                          filters=query,
                                          dimensions=dimensions,
                                          metrics=metrics,
                                          start_date=from_date,
                                          start_index=start_index,
                                          max_results=max_results,
                                          end_date=to_date).execute()

    return result


def get_page_view_query_data(self, query_filter=None, from_date=None, to_date=None,
                             start_index=1, max_results=10000, metrics=None, sort=None):
        """
        Gets all unique page views and corresponding dimensions

        :param self: CKAN command instance
        :param query_filter: str GA query
        :param from_date: str date
        :param to_date: str date
        :param start_index: default
        :param max_results: default
        :param metrics: default
        :param sort: default
        :return: dict GA data
        """

        if not to_date:
            now = datetime.datetime.now()
            to_date = now.strftime("%Y-%m-%d")
        if isinstance(from_date, datetime.date):
            from_date = from_date.strftime("%Y-%m-%d")
        if isinstance(to_date, datetime.date):
            to_date = to_date.strftime("%Y-%m-%d")
        if not metrics:
            metrics = 'ga:visits,ga:visitors,ga:newVisits,ga:uniquePageviews'
        if not sort:
            sort = '-ga:uniquePageviews'

        print '%s -> %s' % (from_date, to_date)

        results = self.service.data().ga().get(ids='ga:%s' % self.profile_id,
                                               filters=query_filter,
                                               dimensions='ga:pagePath',
                                               start_date=from_date,
                                               start_index=start_index,
                                               max_results=max_results,
                                               metrics=metrics,
                                               sort=sort,
                                               end_date=to_date).execute()
        return results


def ga_report(self, start_date=None, end_date=None):

        """
        Google Analytics report given from date and end date.

        Functionality:
            - Get visits to dataset page, resource page
            - Get CKAN resource downloads stats

        :param self: CKAN command instance
        :param start_date: str date
        :param end_date: str date
        :return:
        """

        try:
            start_date = date_time_parse(start_date)
            end_date = date_time_parse(end_date)
        except Exception:
            raise Exception("From date and to date cannot be parsed")

        if not (end_date >= start_date):
            raise Exception("End date is not greater or equal to start date")

        # Date time object to string
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        # Initialise the parser
        ga_views = stats.GoogleAnalyticsViews(self.CONFIG, start_date, end_date)

        # parse from all the page views of dataset types
        if DATASET_TYPES:
            data_types = DATASET_TYPES.split(" ")
        else:
            # CKAN default dataset type
            data_types = ['dataset']

        # Get page views for each data types

        for _type in data_types:
            print("Gathering data for data type: {}".format(_type))
            _ga_path = "/{}/".format(_type.strip())
            _query = 'ga:pagePath=~{}'.format(_ga_path)
            ga_dt_res = get_page_view_query_data(self, _query, from_date=start_date, to_date=end_date)

            print("processing data for data type :{}".format(_type))
            # call the dataset/resource views parser
            ga_views.ga_pageviews(_type, ga_dt_res)
            print("Finished processing for the data type :{}".format(_type))

        # Store GA Report for page views (dataset, resource and url type download)
        ga_views.save_to_db("ga_report_package", "dataset_views")
        ga_views.save_to_db("ga_report_resource", "resource_views")
        ga_views.save_to_db("ga_report_resource", "downloads")

        # process for all the events
        if EVENT_CATEGORIES_ACTION:
            events = EVENT_CATEGORIES_ACTION.split(" ")

            # Loop over all the events
            for _event in events:
                _category, _action = _event.strip().split(":")
                ga_evt_res = get_data_for_events(self, _category, _action, from_date=start_date, to_date=end_date)
                if _action.lower() == "download":
                    # Currently supports download event
                    # Call the events parser
                    ga_views.ga_download_event(_action, ga_evt_res)
                else:
                    log.error("Parser is not defined for the event: {}, hence ignoring".format(_action))
                    print("Parser is not defined for the event: {}, hence ignoring".format(_action))

        # Save all the events
        ga_views.save_to_db("ga_report_events", "events")
        # Print the summary
        print(ga_views)

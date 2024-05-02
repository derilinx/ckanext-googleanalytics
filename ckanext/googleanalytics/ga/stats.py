from ckanext.googleanalytics.ga import validators
from ckanext.googleanalytics.dbutil import insert_ga_report, insert_ga_events
import ckan.model as model
from sqlalchemy import or_
from ckan.plugins import toolkit
import uuid
import requests
import re

import logging
log = logging.getLogger(__name__)


class GoogleAnalyticsViews:
    """
        Parse all events from Google Analytics
            -   Currently support download event
            - This stats also considers 3rd party resource URL

        Google Analytics page views (dataset, resource and download) may contain the following information
            -  /dataset/
            - /dataset/<>/resource/<>
            - /dataset/<>/resource/<>/download/<>

        Note:
            - The url may also contains query parameters for views/geospatial and more.
            - These types are considered as resource views
    """

    # This takes zero parameter
    download_regex = '.*/.*/[a-z0-9-_]+/resource/([a-z0-9-_]+)/download/[a-z0-9-_]+'

    # This takes one param i.e. dataset type
    resource_view_regex = '.*/{}/[a-z0-9-_]+/resource/([a-z0-9-_]+)'
    package_view_regex = '.*/{}/([a-z0-9-_]+)'

    # Define features for dataset

    def __init__(self, config, start_date, end_date, site_code=None):
        self._request_id = str(uuid.uuid4())
        self._config = config
        self.start_date = start_date
        self.end_date = end_date
        self._update_views_containing_filters = False
        self._site_code = site_code or self._config.get("ckanext.odm.site_code") or "default"
        self._capture_download = self._config.get("ckanext.googleanalytics.capture_download", True)
        self.downloads = dict()
        self.dataset_views = dict()
        self.resource_views = dict()
        self.events = dict()

    def __repr__(self):
        """
        This is base representation
        :return:
        """
        return GoogleAnalyticsViews.__doc__

    def __str__(self):
        """
        Prints the summary
        :return:
        """
        return self.summary()

    def summary(self):
        """
        Print summary of the extracted data
        :return: None
        """
        r = [
            "\nSUMMARY FOR THE GIVEN PERIOD : ",
            "\nGiven Site code: {}".format(self._site_code),
            "\nCount of datasets: {}".format(len(self.dataset_views)),
            "\nCount of resources: {}".format(len(self.resource_views)),
            "\nCount of download resources from url: {}".format(len(self.downloads)),
            "\n"
        ]
        for _act in self.events:
            r.append("\nCount of datasets/resource for the event action - {}: {}".format(_act, len(self.events[_act])))
        return " ".join(r)

    def save_to_db(self, table_name=None, attribute_name=None):
        log.info("Attribute name: {}".format(attribute_name))
        if table_name and attribute_name:
            if hasattr(self, attribute_name):
                if attribute_name == "events":
                    # Insert event data
                    insert_ga_events(getattr(self, attribute_name), self.start_date, self.end_date)
                else:
                    # Insert page views data
                    insert_ga_report(table_name, getattr(self, attribute_name), self.start_date, self.end_date)
            else:
                log.error("No attribute named {} associated with the class GoogleAnalyticsViews".format(attribute_name))
        else:
            log.error("No table name or attribute name is specified")

    def _get_resource_from_url(self, resource_url):
        """
        Get resource object given resource url. Get the latest created url
        :return: resource
        """
        if resource_url.strip():
            resource = model.Session.query(model.Resource).autoflush(True).filter(
                model.Resource.url == resource_url.strip()).order_by(
                model.Resource.created.desc()).first()

            # What if the multiple resource has same 3rd part url??
            return resource
        else:
            return None

    def _get_package(self, dataset_id):
        """
        Get package data given package id
        :param dataset_id:
        :return:
        """
        package = model.Session.query(model.Package).autoflush(True).filter(or_(
            model.Package.id == dataset_id.strip(), model.Package.name == dataset_id)).first()

        return package

    def _get_resource(self, resource_id):
        """
        Get resource data given resource id or name
        :param resource_id:
        :return:
        """

        resource = model.Session.query(model.Resource).autoflush(True).filter(or_(
            model.Resource.id == resource_id, model.Resource.name == resource_id)).first()
        return resource

    def capture_resource_view(self, resource_id=None, views=None, label=''):
        """
        Check if the resource exists and create a data to insert to database.
        :param resource_id: download resource id
        :param views: Views count
        :param label: GA label
        :return: None
        """
        # Prepare the download data
        rsc = self._get_resource(resource_id)
        if rsc:
            resource_id = rsc.id
            if resource_id in self.resource_views:
                self.resource_views[resource_id]['visits'] += int(views[0])
                self.resource_views[resource_id]['visitors'] += int(views[1])
                self.resource_views[resource_id]['new_visits'] += int(views[2])
                self.resource_views[resource_id]['unique_pageviews'] += int(views[3])
            else:

                # Note: Any additional field is to be added in extras
                self.resource_views[resource_id] = {
                    'run_id': self._request_id,
                    'metric_type': "resource views",
                    'metrics': 'resource',
                    'site_code': self._site_code,
                    'label': label,
                    'resource_id': resource_id,
                    'resource_title': rsc.name or rsc.id,
                    'visits': int(views[0]),
                    'visitors': int(views[1]),
                    'new_visits': int(views[1]),
                    'unique_pageviews': int(views[3]),
                    "state": rsc.state
                }
        else:
            log.warning("Given resource id: {} doesnt exists".format(resource_id))

    def capture_dataset_view(self, dataset_id=None, views=None, label=''):
        """
        Check if the resource exists and create a data to insert to database.
        :param dataset_id: dataset_id
        :param views: Views count
        :param label: GA label
        :return: None
        """
        # Prepare the download data
        pkg = self._get_package(dataset_id)
        if pkg:
            dataset_id = pkg.id
            if dataset_id in self.dataset_views:
                self.dataset_views[dataset_id]['visits'] += int(views[0])
                self.dataset_views[dataset_id]['visitors'] += int(views[1])
                self.dataset_views[dataset_id]['new_visits'] += int(views[2])
                self.dataset_views[dataset_id]['unique_pageviews'] += int(views[3])
            else:

                # Note: Any additional field is to be added in extras
                self.dataset_views[dataset_id] = {
                    'run_id': self._request_id,
                    'metric_type': "package views",
                    'metrics': 'package',
                    'site_code': self._site_code,
                    'label': label,
                    'package_id': dataset_id,
                    "package_title": pkg.title or pkg.name,
                    'visits': int(views[0]),
                    'visitors': int(views[1]),
                    'new_visits': int(views[2]),
                    'unique_pageviews': int(views[3]),
                    'type': pkg.type,
                    'private': pkg.private,
                    'state': pkg.state
                }
        else:
            log.warning("Given dataset id: {} doesnt exists".format(dataset_id))

    def capture_resource_download(self, resource_id=None, views=None, label=''):
        """
        Check if the resource exists and create a data to insert to database.
        :param resource_id: download resource id
        :param views: Views count
        :param label: GA label
        :return: None
        """
        # Prepare the download data
        rsc = self._get_resource(resource_id)
        if rsc:
            resource_id = rsc.id
            if resource_id in self.downloads:
                self.downloads[resource_id]['metric_value'] += int(views)
            else:

                # Note: Any additional field is to be added in extras
                self.downloads[resource_id] = {
                    'run_id': self._request_id,
                    'metric_type': "download",
                    'metrics': 'resource',
                    'site_code': self._site_code,
                    'label': label,
                    'resource_id': resource_id,
                    'resource_title': rsc.name or rsc.id,
                    'visits': int(views[0]),
                    'visitors': int(views[1]),
                    'newVisits': int(views[1]),
                    'uniquePageviews': int(views[3]),
                    "state": rsc.state
                }
        else:
            log.warning("Given resource id: {} doesnt exists".format(resource_id))

    def ga_download_event(self, event_action, data):
        """
        TODO: How to generalise the events??

        Consider only these events for now.....

        Current Events:
            -  download
            - dataset_views
            - resource_views

        :param event_action: type of the event
        :param data: events data
        :return:
        """
        self.events[event_action] = dict()

        for row in data:
            if len(row.dimension_values) > 2:
                event_label = row.dimension_values[2].value
                _event_count = int(row.metric_values[0].value)
            else:
                log.error("Insufficient dimension values in GA4 'Row' object.")
                continue
            
            _process = event_label.split("|")
            if len(_process) > 1:
                _resource_id = _process[0].strip()
                label = _process[1]
            else:
                is_download = re.match(self.download_regex, event_label)
                if is_download:
                    _resource_id = is_download.groups()[0]
                else:
                    resource = self._get_resource_from_url(event_label)
                    if resource:
                        _resource_id = resource.id
                        label = event_label

            if _resource_id:
                if _resource_id in self.events[event_action]:
                    self.events[event_action][_resource_id]["metric_value"] += _event_count
                else:
                    _data = {
                        'run_id': self._request_id,
                        "metric_type": "events",
                        "metric_value": _event_count,
                        "site_code": self._site_code,
                        "category": event_action,
                        "id": _resource_id,
                        "title": label,
                    }
                    self.events[event_action][_resource_id] = _data

    def ga_pageviews(self, data_type, data):

        """
        Google Analytics page views may contain the following information
            -  /dataset/
            - /dataset/<>/resource/<>
            - /dataset/<>/resource/<>/download/<>

        :param data_type: str data type as given in the config (dataset, laws, library_record etc.
        :param data: rows (list)
        :return: Store the result in the class instance to update to the db.
        """

        if not validators.check_ga_query_res(data):
            print("GA query for data type: {} resulted in empty result".format(data_type))
 
        for row in data:
            if row.dimension_values:
                _url = row.dimension_values[0].value
            else:
                log.error(f"Missing dimension values in GA4 row for data type: {data_type}")
                continue

            url_object = requests.utils.urlparse(_url)
            _url_query = url_object.query
            _url_path = url_object.path

            if row.metric_values:
                _views = [int(metric.value) for metric in row.metric_values]
            else:
                log.error(f"Missing metric values in GA4 row for data type: {data_type}")
                continue

            if not validators.check_resource_views_filters(_url_query):
                is_download = re.compile(self.download_regex).match(_url_path)

                if is_download and self._capture_download:
                    _download_id = is_download.groups()[0]
                    self.capture_resource_download(
                        resource_id=_download_id,
                        views=_views,
                        label=_url
                    )
                else:
                    is_res_view = re.compile(self.resource_view_regex.format(data_type)).match(_url_path)
                    is_pkg_view = re.compile(self.package_view_regex.format(data_type)).match(_url_path)
                    if is_res_view:
                        res_id = is_res_view.groups()[0]
                        self.capture_resource_view(
                            resource_id=res_id,
                            views=_views,
                            label=_url
                        )
                    elif is_pkg_view:
                        pkg_id = is_pkg_view.groups()[0]
                        self.capture_dataset_view(
                            dataset_id=pkg_id,
                            views=_views,
                            label=_url
                        )

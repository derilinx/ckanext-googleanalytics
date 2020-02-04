from ckanext.googleanalytics.ga import validators
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

    def __init__(self, config, start_date, end_date):
        self._request_id = str(uuid.uuid4())
        self._config = config
        self.start_date = start_date
        self.end_date = end_date
        self._update_views_containing_filters = False
        self._site_code = self._config.get("ckanext.odm.site_code")
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
            "\nCount of datasets: {}".format(len(self.dataset_views)),
            "\nCount of resources: {}".format(len(self.resource_views)),
            "\nCount of download resources from url: {}".format(len(self.downloads)),
            "\n"
        ]
        for _act in self.events:
            r.append("\nCount of datasets/resource for the event action - {}: {}".format(_act, len(self.events[_act])))
        return " ".join(r)

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
                self.resource_views[resource_id]['metric_value'] += int(views)
            else:

                # Note: Any additional field is to be added in extras
                self.resource_views[resource_id] = {
                    'run_id': self._request_id,
                    'metric_type': "resource views",
                    'metrics': 'resource',
                    'site_code': self._site_code,
                    'label': label,
                    'id': resource_id,
                    'metric_value': int(views),
                    'extras': {
                        "state": rsc.state
                    }
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
                self.dataset_views[dataset_id]['metric_value'] += int(views)
            else:

                # Note: Any additional field is to be added in extras
                self.dataset_views[dataset_id] = {
                    'run_id': self._request_id,
                    'metric_type': "package views",
                    'metrics': 'package',
                    'site_code': self._site_code,
                    'label': label,
                    'id': dataset_id,
                    'metric_value': int(views),
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
                    'id': resource_id,
                    'metric_value': int(views),
                    'extras': {
                        "state": rsc.state
                    }
                }
        else:
            log.warning("Given resource id: {} doesnt exists".format(resource_id))

    def ga_download_event(self, event_action, event_data):
        """
        TODO: How to generalise the events??

        Consider only these events for now.....

        Current Events:
            -  download
            - dataset_views
            - resource_views

        :param event_type: type of the event
        :param events_data: events data
        :return:
        """
        self.events[event_action] = dict()
        events_data = data.get('rows', [])

        for _event in events_data:
            # This is the event label
            _url = _event[2]
            _event_count = int(_event[3])
            _event_category = _event[0]
            _event_action = _event[1]
            _resource_id = None

            # Download event label is separated by pipe (recently made changes - consider the previous type as well)
            _process = _url.split("|")
            if len(_process) > 1:
                # first element is resource id
                label = _process[1]
                _resource_id = _process[0].strip()
            else:
                # if not separated by pipe - check if the url type is of ckan /download/
                label = _url
                is_download = re.compile(self.download_regex).match(_url)
                if is_download:
                    _resource_id = is_download.groups()[0]
                else:
                    # URl is not from ckan, hence get id from database given url
                    # Try to get the resource id from the database given resource url
                    # This scenario occurs when the resource url is 3rd part url
                    resource = self._get_resource_from_url(label)
                    if resource:
                        _resource_id = resource.id

            if _resource_id:
                if _resource_id in self.events:
                    self.events[event_action][_resource_id]['metrics_value'] += _event_count
                else:
                    # Create a new event data
                    _data = {
                        'run_id': self._request_id,
                        "metric_type": "events",
                        "metric_value": _event_count,
                        "site_code": self._site_code,
                        "category": _event_category,
                        "action": _event_action,
                        "id": _resource_id,
                        "label": label
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
            raise Exception("GA query for data type: {} resulted in empty result".format(data_type))

        rows = data.get('rows')
        for _item in rows:
            _url = _item[0]
            url_object = requests.utils.urlparse(_url)
            _url_query = url_object.query
            _url_path = url_object.path
            _views = _item[1]

            if not validators.check_resource_views_filters(_url_query):

                # Check if the link is of download?
                is_download = re.compile(self.download_regex).match(_url_path)

                # Capture the downloads of the resource,
                # should not contain query
                # /download/ should be in url path
                if is_download and self._capture_download:
                    # Capture the download from the url
                    # Note: this is used only when there is not download event present
                    # in ODM there is a resource download event exists which is more accurate
                    _download_id = is_download.groups()[0]
                    self.capture_resource_download(resource_id=_download_id, views=_views, label=_url)

                else:
                    # Check for resource and package match regex
                    # If the group 2 is empty then its dataset views else its resource view
                    is_res_view = re.compile(self.resource_view_regex.format(data_type)).match(_url_path)
                    is_pkg_view = re.compile(self.package_view_regex.format(data_type)).match(_url_path)

                    if is_res_view:
                        res_id = is_res_view.groups()[0]
                        # Capture resource views details
                        self.capture_resource_view(resource_id=res_id, views=_views, label=_url)
                    elif is_pkg_view:
                        pkg_id = is_pkg_view.groups()[0]
                        # This is package view details
                        self.capture_dataset_view(dataset_id=pkg_id, views=_views, label=_url)

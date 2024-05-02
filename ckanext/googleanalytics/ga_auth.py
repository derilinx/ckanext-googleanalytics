from ckan import logic
import httplib2
# from apiclient.discovery import build
# from oauth2client.service_account import ServiceAccountCredentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.auth import load_credentials_from_file
from google.api_core.exceptions import NotFound

import os
import json

NotFound = logic.NotFound

from ckan.exceptions import CkanVersionException
import ckan.plugins.toolkit as tk

try:
    tk.requires_ckan_version("2.9")
except CkanVersionException:
    from pylons import config
else:
    config = tk.config

import logging

log = logging.getLogger(__file__)


def _prepare_credentials(credentials_filename):
    credentials, project = load_credentials_from_file(credentials_filename)
    return credentials


def init_client(credentials_file):
    credentials = _prepare_credentials(credentials_file)
    client = BetaAnalyticsDataClient(credentials=credentials)
    return client


def get_property_id(client, site_code=None):
    return config.get("googleanalytics.id", )


class GoogleAnalyticsCredentialsObject:

    _default_path = "/usr/lib/ckan/default/src/ckan/"

    def __init__(self, site_code=None):
        self.property_id = None
        self.client = None
        self._site_code = site_code or config.get("ckanext.odm.site_code")
        self._cred_file = self._get_file()
        self.set_credentials()

    def __call__(self, *args, **kwargs):
        self.set_credentials()

    def _get_file(self):
        _file_name = config.get("googleanalytics.credential.file.name", "ga4-service-account.json")

        path = os.path.join(self._default_path, _file_name)
        if not os.path.isfile(path):
            raise NotFound(f"Google Analytics 4 credentials not found: {path}. Contact system admin.")

        return path

    def set_credentials(self):
        self.client = init_client(self._cred_file)
        self.property_id = get_property_id(self.client, site_code=self._site_code)

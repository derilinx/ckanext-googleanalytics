from ckan import logic
import httplib2
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

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
    """
    Either returns the user's oauth credentials or uses the credentials
    file to generate a token (by forcing the user to login in the browser)
    """
    scope = ["https://www.googleapis.com/auth/analytics.readonly"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        credentials_filename, scopes=scope
    )
    return credentials


def init_service(credentials_file):
    """
    Given a file containing the user's oauth token (and another with
    credentials in case we need to generate the token) will return a
    service object representing the analytics API.
    """
    http = httplib2.Http()

    credentials = _prepare_credentials(credentials_file)
    http = credentials.authorize(http)  # authorize the http object

    return build("analytics", "v3", http=http)


def get_profile_id(service, site_code=None):
    """
    Get the profile ID for this user and the service specified by the
    'googleanalytics.id' configuration option. This function iterates
    over all of the accounts available to the user who invoked the
    service to find one where the account name matches (in case the
    user has several).
    """
    accounts = service.management().accounts().list().execute()

    if not accounts.get("items"):
        return None

    if not site_code:
        accountName = config.get('googleanalytics.account')
        webPropertyId = config.get('googleanalytics.id')
    else:
        accountName = config.get('googleanalytics.account_{}'.format(site_code))
        webPropertyId = config.get('googleanalytics.id_{}'.format(site_code))

    for acc in accounts.get('items'):
        if acc.get('name') == accountName:
            accountId = acc.get('id')

            # TODO: check, whether next line is doing something useful.
            webproperties = (
                service.management()
                .webproperties()
                .list(accountId=accountId)
                .execute()
            )

            profiles = (
                service.management()
                .profiles()
                .list(accountId=accountId, webPropertyId=webPropertyId)
                .execute()
            )

            if profiles.get("items"):
                return profiles.get("items")[0].get("id")

        log.error("get_profile_id: Didn't find the account name")
    return None


class GoogleAnalyticsCredentialsObject:
    """
    This is to register google analytics credentials for the UI. CLI command registers credentials from the file
    But UI we need to register service from the config
    """

    _default_path = "/usr/lib/ckan/default/src/ckan/"

    def __init__(self, site_code=None):

        self.service = None
        self.profile_id = None
        self.CONFIG = config
        self._site_code = site_code or config.get("ckanext.odm.site_code")
        self._cred_file = self._get_file()

    def __repr__(self):
        return "This is required to call GA from UI"

    def __str__(self):
        return GoogleAnalyticsCredentialsObject.__doc__

    def __call__(self, *args, **kwargs):
        self.set_credentials()

    def _get_file(self):
        """
        Validates the availability of required credentials
        :return: raises error
        """
        _file_name = config.get("googleanalytics.credential.file.name",
                                "opendevelopmentmekong2-6cd98ea6d5d9.json")

        file = "{}{}".format(self._default_path, _file_name)
        if not os.path.isfile(file):
            raise NotFound("Google Analytics credentials error. Contact system admin")

        return file

    def set_credentials(self):
        """
        Call init service and profile id
        :return:
        """
        self.service = init_service(self._cred_file)
        self.profile_id = get_profile_id(self.service, site_code=self._site_code)


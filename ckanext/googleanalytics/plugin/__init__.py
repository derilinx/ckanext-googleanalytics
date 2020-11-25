# -*- coding: utf-8 -*-
from __future__ import absolute_import

from six.moves.urllib.parse import urlencode
import ast
import logging
import threading

from builtins import str, range

import requests
import json

import ckan.lib.helpers as h
import ckan.plugins as p
import ckan.plugins.toolkit as tk

from ckan.exceptions import CkanVersionException

DEFAULT_RESOURCE_URL_TAG = "/downloads/"

log = logging.getLogger(__name__)

try:
    tk.requires_ckan_version("2.9")
except CkanVersionException:
    from ckanext.googleanalytics.plugin.pylons_plugin import GAMixinPlugin
else:
    from ckanext.googleanalytics.plugin.flask_plugin import GAMixinPlugin

from ckanext.googleanalytics import action

class GoogleAnalyticsException(Exception):
    pass


class AnalyticsPostThread(threading.Thread):
    """Threaded Url POST"""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # grabs host from queue
            data_dict = self.queue.get()

            data = urlencode(data_dict)
            log.debug("Sending API event to Google Analytics: " + data)
            # send analytics
            res = requests.post(
                "http://www.google-analytics.com/collect", data, timeout=10,
            )
            # signals to queue job is done
            self.queue.task_done()


class GoogleAnalyticsPlugin(GAMixinPlugin, p.SingletonPlugin):
    p.implements(p.IConfigurable, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IActions)

    def configure(self, config):
        """Load config settings for this extension from config file.

        See IConfigurable.

        """
        if "googleanalytics.id" not in config:
            msg = "Missing googleanalytics.id in config"
            raise GoogleAnalyticsException(msg)
        self.googleanalytics_id = config["googleanalytics.id"]
        self.googleanalytics_domain = config.get(
            "googleanalytics.domain", "auto"
        )
        self.googleanalytics_fields = ast.literal_eval(
            config.get("googleanalytics.fields", "{}")
        )

        googleanalytics_linked_domains = config.get(
            "googleanalytics.linked_domains", ""
        )
        self.googleanalytics_linked_domains = [
            x.strip() for x in googleanalytics_linked_domains.split(",") if x
        ]

        if self.googleanalytics_linked_domains:
            self.googleanalytics_fields["allowLinker"] = "true"

        # If resource_prefix is not in config file then write the default value
        # to the config dict, otherwise templates seem to get 'true' when they
        # try to read resource_prefix from config.
        if "googleanalytics_resource_prefix" not in config:
            config[
                "googleanalytics_resource_prefix"
            ] = DEFAULT_RESOURCE_URL_TAG
        self.googleanalytics_resource_prefix = config[
            "googleanalytics_resource_prefix"
        ]

        self.show_downloads = tk.asbool(
            config.get("googleanalytics.show_downloads", True)
        )
        self.track_events = tk.asbool(
            config.get("googleanalytics.track_events", False)
        )
        self.enable_user_id = tk.asbool(
            config.get("googleanalytics.enable_user_id", False)
        )

        p.toolkit.add_resource("../assets", "ckanext-googleanalytics")

        # spawn a pool of 5 threads, and pass them queue instance
        for i in range(5):
            t = AnalyticsPostThread(self.analytics_queue)
            t.setDaemon(True)
            t.start()

    def update_config(self, config):
        """Change the CKAN (Pylons) environment configuration.

        See IConfigurer.

        """
        p.toolkit.add_template_directory(config, "../templates")

    def get_helpers(self):
        """Return the CKAN 2.0 template helper functions this plugin provides.

        See ITemplateHelpers.

        """
        return {"googleanalytics_header": self.googleanalytics_header}

    def get_actions(self):
        """
        Register all API actions
        :return: dict
        """
        return {
            "run_ga_report": action.ga_report_run
        }

    def googleanalytics_header(self):
        """Render the googleanalytics_header snippet for CKAN 2.0 templates.

        This is a template helper function that renders the
        googleanalytics_header jinja snippet. To be called from the jinja
        templates in this extension, see ITemplateHelpers.

        """

        config = {'anonymizeIp': True,
                  'debug_mode': True,
        }

        if self.enable_user_id and tk.c.user:
            config['user_id'] = str(tk.c.userobj.id)

        IS_GA4 = False
        #
        # custom OGCIO dimensions:
        #
        user_properties = { 'user_type': 'public' }
        custom_data = {}

        if tk.c.is_psb_user:
            user_properties['user_type'] = 'psb'
        elif tk.c.userobj and tk.c.userobj.sysadmin:
            user_properties['user_type'] = 'admin'
        elif tk.c.userobj:
            user_properties['user_type'] = 'psbadmin'

        pkg_dict = getattr(tk.c, 'pkg_dict', {})
        log.debug('ga_header: pkg_dict: %s', pkg_dict)
        if pkg_dict and pkg_dict.get('organization'):
            config['org'] = pkg_dict['organization']['name']
            config['dataset'] = pkg_dict['name']

        if IS_GA4:
            config['user_properties'] = user_properties
        else:
            config['custom_map'] = {
                'dimension1': 'org',
                'dimension2': 'dataset',
                'dimension3': 'user_type',
                }
            config.update(user_properties)

        # end custom dimensions

        data = {
            "ga4": IS_GA4,
            "gtm": True,
            "googleanalytics_id": self.googleanalytics_id,
            "googleanalytics_config": json.dumps(config),
            #### undone -- these aren't ported to GTM yet, use gtm=False
            "googleanalytics_domain": self.googleanalytics_domain,
            "googleanalytics_fields": str(self.googleanalytics_fields),
            "googleanalytics_linked_domains": self.googleanalytics_linked_domains,
        }
        return p.toolkit.render_snippet(
            "googleanalytics/snippets/googleanalytics_header.html", data
        )

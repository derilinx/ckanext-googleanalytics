# encoding: utf-8
from __future__ import print_function
from __future__ import absolute_import
import datetime
import time
import re
import logging
import click
import ckan.model as model
from . import dbutil
from google.analytics.data_v1beta import RunReportRequest, DateRange, Metric, Dimension, OrderBy


from ckan.cli import tracking
import ckan.plugins.toolkit as tk

log = logging.getLogger(__name__)
PACKAGE_URLS = ["/dataset/", "/api_record/"]  # XXX get from routes...
DEFAULT_RESOURCE_URL_TAG = "/downloads/"

RESOURCE_URL_REGEX = re.compile("/dataset/[a-z0-9-_]+/resource/([a-z0-9-_]+)")
DATASET_EDIT_REGEX = re.compile("/dataset/edit/([a-z0-9-_]+)")

try:
    from ckanext import odm_profile
    LIBRARY_URL = '/library_record/'
    LAWS_URL = '/laws_record/'
    AGREEMENT_URL = '/agreement/'
    MAPS = "/maps/"
    PROFILES = "/profile/"

    URL_MAP = PACKAGE_URLS + [LIBRARY_URL, LAWS_URL, AGREEMENT_URL, MAPS, PROFILES]
except ImportError:
    URL_MAP = PACKAGE_URLS


def get_commands():
    return [
        googleanalytics
    ]


@click.group(short_help=u"GoogleAnalytics commands")
def googleanalytics():
    pass

@googleanalytics.command()
def init():
    """Initialise the local stats database tables
    """
    model.Session.remove()
    model.Session.configure(bind=model.meta.engine)
    dbutil.init_tables()
    log.info("Set up statistics tables in main database")


@googleanalytics.command(short_help=u"Load data from Google Analytics API")
@click.argument("credentials", type=click.Path(exists=True))
@click.option("-s", "--start-date", required=False)
def load(credentials, start_date):
    """Parse data from Google Analytics API and store it
    in a local database
    """
    from .ga_auth import init_client, get_property_id

    try:
        client = init_client(credentials)
    except TypeError as e:
        raise Exception("Unable to create a service: {0}".format(e))
    property_id = get_property_id(client)

    if start_date:
        bulk_import(client, property_id, start_date)
    else:
        now = datetime.datetime.now()
        floor_date = datetime.date(2015, 8, 14).strftime("%Y-%m-%d")
        recent_date_start = (now - datetime.timedelta(14)).strftime("%Y-%m-%d")
        end_date=now.strftime("%Y-%m-%d")

        dates = {"recent": recent_date_start, "ever": floor_date}
        metrics = [Metric(name="screenPageViews"), Metric(name="totalUsers"),]
        dimensions = [Dimension(name="pagePath")]


        packages = {}
        for date_name, date in list(dates.items()):
            request = RunReportRequest(
                property=f"properties/{property_id}",
                date_ranges=[DateRange(start_date=date, end_date=end_date)],
                metrics=metrics,
                dimensions=dimensions,
            )
            response = client.run_report(request)
            for row in response.rows:
                package = row.dimension_values[0].value
                count = row.metric_values[0].value
                val = 0 
                if package in packages and date_name in packages[package]:
                    val += packages[package][date_name]
                packages.setdefault(package, {})[date_name] = (
                    val + int(count)
                )
        save_ga_data(packages)
        log.info("Saved %s records from google" % len(packages))


@googleanalytics.command(short_help=u"Generate Report from Google Analytics API")
@click.argument("credentials", type=click.Path(exists=True))
@click.option("-s", "--start-date", required=True)
@click.option("-e", "--end-date", required=True)
def report(credentials, start_date, end_date):
    """Parse data from Google Analytics API and store it
    in a local database
    """
    from .ga_auth import init_client, get_property_id
    from .ga import commands

    try:
        client = init_client(credentials)
    except TypeError as e:
        raise Exception("Unable to create a service: {0}".format(e))
    property_id = get_property_id(client)

    commands.ga_report(client, property_id, start_date=start_date, end_date=end_date)


def _resource_url_tag():
    return tk.config.get(
        "googleanalytics_resource_prefix", DEFAULT_RESOURCE_URL_TAG
    )


###############################################################################
#                                     xxx                                     #
###############################################################################


def internal_save(packages_data, summary_date):
    engine = model.meta.engine
    # clear out existing data before adding new
    sql = (
        """DELETE FROM tracking_summary
             WHERE tracking_date=%s; """
    )
    engine.execute(sql, summary_date)

    for url, count in list(packages_data.items()):
        # If it matches the resource then we should mark it as a resource.
        # For resources we don't currently find the package ID.
        if RESOURCE_URL_REGEX.match(url):
            tracking_type = "resource"
        else:
            tracking_type = "page"

        sql = """INSERT INTO tracking_summary
                 (url, count, tracking_date, tracking_type)
                 VALUES (%s, %s, %s, %s);"""
        engine.execute(sql, url, count, summary_date, tracking_type)

    # get ids for dataset urls
    sql = """UPDATE tracking_summary t
                SET package_id = COALESCE(
                    (SELECT id FROM package p WHERE t.url = ANY (%s) || p.name), '~~not~found~~')
                WHERE t.package_id IS NULL AND tracking_type = 'page';"""
    url_patterns = [f"{url}%" for url in PACKAGE_URLS]
    engine.execute(sql, [url_patterns])

    # get ids for dataset edit urls which aren't captured otherwise
    sql = """UPDATE tracking_summary t
              SET package_id = COALESCE(
                  (SELECT id FROM package p WHERE t.url = ANY (%s) || p.name),
                  '~~not~found~~')
              WHERE t.package_id = '~~not~found~~' AND tracking_type = 'page';"""
    edit_patterns = [f"{url}edit/%" for url in PACKAGE_URLS]
    engine.execute(sql, edit_patterns)

    # update summary totals for resources
    sql = """UPDATE tracking_summary t1
             SET running_total = (
                SELECT sum(count)
                FROM tracking_summary t2
                WHERE t1.url = t2.url
                AND t2.tracking_date <= t1.tracking_date
             ) + t1.count
             ,recent_views = (
                SELECT sum(count)
                FROM tracking_summary t2
                WHERE t1.url = t2.url
                AND t2.tracking_date <= t1.tracking_date AND t2.tracking_date >= t1.tracking_date - 14
             ) + t1.count
             WHERE t1.running_total = 0 AND tracking_type = 'resource';"""
    engine.execute(sql)

    # update summary totals for pages
    sql = """UPDATE tracking_summary t1
             SET running_total = (
                SELECT sum(count)
                FROM tracking_summary t2
                WHERE t1.package_id = t2.package_id
                AND t2.tracking_date <= t1.tracking_date
             ) + t1.count
             ,recent_views = (
                SELECT sum(count)
                FROM tracking_summary t2
                WHERE t1.package_id = t2.package_id
                AND t2.tracking_date <= t1.tracking_date AND t2.tracking_date >= t1.tracking_date - 14
             ) + t1.count
             WHERE t1.running_total = 0 AND tracking_type = 'page'
             AND t1.package_id IS NOT NULL
             AND t1.package_id != '~~not~found~~';"""
    engine.execute(sql)


def bulk_import(service, profile_id, start_date=None):
    if start_date:
        # Get summeries from specified date
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    else:
        # No date given. See when we last have data for and get data
        # from 2 days before then in case new data is available.
        # If no date here then use 2010-01-01 as the start date
        engine = model.meta.engine
        sql = """SELECT tracking_date from tracking_summary
                 ORDER BY tracking_date DESC LIMIT 1;"""
        result = engine.execute(sql).fetchall()
        if result:
            start_date = result[0]["tracking_date"]
            start_date += datetime.timedelta(-2)
            # convert date to datetime
            combine = datetime.datetime.combine
            start_date = combine(start_date, datetime.time(0))
        else:
            start_date = datetime.datetime(2011, 1, 1)
    end_date = datetime.datetime.now()
    original_start_date = start_date
    while start_date < end_date:
        stop_date = start_date + datetime.timedelta(1)
        packages_data = get_ga_data_new(
            service, profile_id, start_date=start_date, end_date=stop_date
        )
        internal_save(packages_data, start_date)
        # sleep to rate limit requests
        time.sleep(0.25)
        start_date = stop_date
        log.info("%s received %s" % (len(packages_data), start_date))
        print("%s received %s" % (len(packages_data), start_date))
    tracking.update_tracking_solr(model.meta.engine, original_start_date)

def get_ga_data_new(client, property_id, start_date=None, end_date=None):
    """Get raw data from Google Analtyics for packages and
    resources.

    Returns a dictionary like::

       {'identifier': 3}
    """
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")

    packages = {}
    date_range = DateRange(start_date=start_date, end_date=end_date)

    metrics = [Metric(name="screenPageViews")]
    dimensions = [Dimension(name="pagePath")]

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[date_range],
        metrics=metrics,
        dimensions=dimensions
    )

    response = client.run_report(request)

    for result in response.rows:
        package = result.dimension_values[0].value
        count = int(result.metric_values[0].value)
        packages[package] = count

    return packages

def save_ga_data(packages_data):
    """Save tuples of packages_data to the database
    """
    for identifier, visits in list(packages_data.items()):
        recently = visits.get("recent", 0)
        ever = visits.get("ever", 0)
        matches = RESOURCE_URL_REGEX.match(identifier)
        if matches:
            resource_url = identifier[len(_resource_url_tag()) :]
            resource = (
                model.Session.query(model.Resource)
                .autoflush(True)
                .filter_by(id=matches.group(1))
                .first()
            )
            if not resource:
                log.warning("Couldn't find resource %s" % resource_url)
                continue
            dbutil.update_resource_visits(resource.id, recently, ever)
            log.info("Updated %s with %s visits" % (resource.id, visits))
        else:
            # package_name = identifier[len(PACKAGE_URL) :]
            package_name = None
            for url in PACKAGE_URLS:
                if url in identifier:
                    package_name = identifier[len(url) :]
            if not package_name:
                continue
            if "/" in package_name:
                log.warning("%s not a valid package name" % package_name)
                continue
            item = model.Package.by_name(package_name)
            if not item:
                log.warning("Couldn't find package %s" % package_name)
                continue
            dbutil.update_package_visits(item.id, recently, ever)
            log.info("Updated %s with %s visits" % (item.id, visits))
    model.Session.commit()


def ga_query(client, property_id, query_filter=None, from_date=None, metrics=None):
    """Execute a query against Google Analytics
    """
    now = datetime.datetime.now()
    to_date = now.strftime("%Y-%m-%d")
    if isinstance(from_date, datetime.date):
        from_date = from_date.strftime("%Y-%m-%d")

    if not metrics:
        metrics = [
            Metric(name="screenPageViews"),
            Metric(name="totalUsers"),
            Metric(name="newUsers"),
        ]

    date_range = DateRange(start_date=from_date, end_date=to_date)
    sort = OrderBy(metric=OrderBy.MetricOrderBy(name="screenPageViews", order="DESCENDING"))
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[date_range],
        metrics=metrics,
        dimensions=[Dimension(name="pagePath")],
        order_bys=[sort]
    )

    response = client.run_report(request)
    return response.rows

def get_ga_data(client, property_id, query_filter):
    """Get raw data from Google Analtyics for packages and
    resources, and for both the last two weeks and ever.

    Returns a dictionary like::

       {'identifier': {'recent':3, 'ever':6}}
    """
    now = datetime.datetime.now()
    recent_date = (now - datetime.timedelta(14)).strftime("%Y-%m-%d")
    floor_date = datetime.date(2005, 1, 1).strftime("%Y-%m-%d")
    packages = {}
    dates = {"recent": recent_date, "ever": floor_date}

    metrics = [Metric(name="screenPageViews")]

    for date_name, date in dates.items():
        date_range = DateRange(start_date=date, end_date=recent_date)

        request = RunReportRequest(
            property=f"properties/{property_id}",
            date_ranges=[date_range],
            metrics=metrics,
            dimensions=[Dimension(name="pagePath")]
        )

        response = client.run_report(request)

        if "rows" in response:
            for result in response.rows:
                package = result.dimension_values[0].value
                count = int(result.metric_values[0].value)

                if not package.startswith("/"):
                    package = "/" + "/".join(package.split("/")[2:])

                val = 0
                if package in packages and date_name in packages[package]:
                    val += packages[package][date_name]
                packages.setdefault(package, {})[date_name] = int(count) + val

    return packages
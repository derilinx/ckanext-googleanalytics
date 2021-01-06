from __future__ import absolute_import
from builtins import str
import logging
import csv
import io

import ckan.model as model
from ckan.plugins import toolkit
from ckan.plugins.toolkit import _

from . import dbutil

import ckan.logic as logic
from ckanext.googleanalytics import dbutil
from ckanext.googleanalytics import helper as ga_h
from ckanext.googleanalytics import action as ga_action
from datetime import datetime



log = logging.getLogger('ckanext.googleanalytics')


def _clear_tables():
    """
    Clears all the GA report table i.e. ga_report_package/ga_report_resource/ga_report_event
    Any any error rollback the session.
    :return: Nothing
    """
    try:
        pkg = model.Session.query(dbutil.GAReportPackage)
        rsc = model.Session.query(dbutil.GAReportResource)
        evn = model.Session.query(dbutil.GAReportEvents)

        pkg.delete(synchronize_session=False)
        rsc.delete(synchronize_session=False)
        evn.delete(synchronize_session=False)
        model.Session.commit()
    except Exception as e:
        log.error(e)
        model.Session.rollback()


def _get_db_data( db_model, run_id):
    """
    Get the data from the database for download.
    :return: ORM object
    """
    data = model.Session.query(db_model).filter(db_model.run_id == run_id).all()
    fieldnames = [column.key for column in db_model.__table__.columns if not column.key.startswith("_")]
    return data, fieldnames


def report(id=None):
    context = {
        'model': model, 'session': model.Session,
        'user': toolkit.c.user, 'auth_user_obj': toolkit.c.userobj,
        'for_view': True
    }
    data_dict = {
        'id': id,
        'user_obj': toolkit.c.userobj,
        'include_datasets': True,
        'include_num_followers': True
    }

    #self._setup_template_variables(context, data_dict)
    table_data = ga_h.get_recent_runs()

    vars = {
        "user_dict": toolkit.c.userobj.as_dict(),
        "table_data": table_data,
        "errors": {},
        "error_summary": {},
        'form_options': [],
        'data_dict': dict()
    }

    site_code = toolkit.config.get("ckanext.odm.site_code", None)
    if site_code:
        vars.update({'form_options': ga_h.get_ga_select_form_options(),
                     'default_selected': site_code})

    vars['user_dict']['num_followers'] = 0
    vars['user_dict']['number_created_packages'] = 0

    log.debug(vars)

    try:
        logic.check_access('sysadmin', context, data_dict)
    except logic.NotAuthorized:
        toolkit.abort(403, _('Unauthorized to view or run this. Only sysadmin can run or view this'))


    if toolkit.request.method == "GET":
        vars['data_dict']['to_dt'] = datetime.now().strftime('%Y-%m-%d')
        return toolkit.render('user/ga_report.html', extra_vars=vars)

    if toolkit.request.method == "POST":
        _parms = request.params

        # This on press of button Generate Report
        if "run" in _parms:
            data_dict['from_dt'] = _parms.get('from_dt')
            data_dict['to_dt'] = _parms.get('to_dt')
            data_dict['site_code'] = _parms.get('site_code')
            try:
                res = ga_action.ga_report_run(context, data_dict)
                toolkit.h.flash_success(_('Background job has been triggered. '
                                  'Please visit this page after sometime. Id: {}'.format(res.get("job_id", ''))))
            except logic.NotAuthorized as e:
                vars["errors"] = e.error_dict
                vars["error_summary"] = e.error_summary
                toolkit.h.flash_error(_("Not authorized to run this. Only sysadmin can run this."))
                toolkit.abort(403, _('Unauthorized to view or run this.'))
            except logic.ValidationError as e:
                vars["errors"] = e.error_dict
                vars["error_summary"] = e.error_summary
                vars['data_dict'] = data_dict
                toolkit.h.flash_error(_("Form validation error. Please check the given dates"))
                return toolkit.render('user/ga_report.html', extra_vars=vars)

        # This on press of button Clear
        elif "clear" in _parms:
            _clear_tables()
            toolkit.h.flash_success(_('Cleared all Google Analytics report table'))

        report_page = toolkit.h.url_for(controller='ckanext.googleanalytics.controller:GAReport', action='report', id=id)
        toolkit.h.redirect_to(report_page)


def download(id=None, run_id=None, action_name=None):
    """
    Download the file object given run id and action
    :param run_id: run id
    :param action_name: str package_report/resource_report/event_report
    :return: download object
    """
    context = {
        'model': model, 'session': model.Session,
        'user': toolkit.c.user, 'auth_user_obj': toolkit.c.userobj,
        'for_view': True
    }
    data_dict = {
        'id': id,
        'user_obj': toolkit.c.userobj,
        'include_datasets': True,
        'include_num_followers': True
    }

    try:
        logic.check_access('user_update', context, data_dict)
    except NotAuthorized:
        toolkit.abort(403, _('Unauthorized to view or run this.'))
    data = None
    fieldnames = None
    _replace_labels = {
        "metric_value": ""
    }

    if action_name == "package_report":
        data, fieldnames = _get_db_data(dbutil.GAReportPackage, run_id)
        _replace_labels["metric_value"] = "dataset_view_count"
    elif action_name == "resource_report":
        data, fieldnames = _get_db_data(dbutil.GAReportResource, run_id)
        _replace_labels["metric_value"] = "resource_view_count"
    elif action_name == "event_report":
        data, fieldnames = _get_db_data(dbutil.GAReportEvents, run_id)
        _replace_labels["metric_value"] = "resource_download_count"
    else:
        toolkit.abort(403, _('This should not occur.'))

    if data and fieldnames:

        file_object = io.StringIO()
        try:
            writer = csv.DictWriter(file_object, fieldnames=[_replace_labels.get(x, x) for x in fieldnames],
                                    quoting=csv.QUOTE_ALL)
            writer.writeheader()

            # For each row in a data
            for row in data:
                csv_dict = dict()
                # For each column in a row
                for _field in fieldnames:
                    csv_dict[_replace_labels.get(_field, _field)] = getattr(row, _field)

                # Write to csv
                writer.writerow(csv_dict)
        except Exception as e:
            log.error(e)
            pass
        finally:
            result = file_object.getvalue()
            file_object.close()

        if result:
            file_name = "{}_{}".format(run_id, action_name)
            toolkit.response.headers['Content-type'] = 'text/csv'
            toolkit.response.headers['Content-disposition'] = 'attachment;filename=%s.csv' % str(file_name)
            return result
        else:
            toolkit.h.flash_success(_("The download event is empty for the given period"))

    # If not data available. This should not occur.
    report_page = toolkit.h.url_for('google_analytics.report', id=id)
    toolkit.h.redirect_to(report_page)


def organization(id=None):
    context = {
        'model': model, 'session': model.Session,
        'user': toolkit.c.user, 'auth_user_obj': toolkit.c.userobj,
        'for_view': True
    }

    data_dict = {'id': id}

    try:
        logic.check_access('organization_update', context, data_dict)
    except logic.NotAuthorized:
        toolkit.abort(403, _('Unauthorized to view.'))

    try:
        org = toolkit.get_action('organization_show')(context, data_dict)
    except:
        toolkit.abort(404, _('Not Found.'))

    sql = """select name, title, visits_recently, visits_ever
             from package
             inner join package_stats on (package.id=package_stats.package_id)
             where
               state='active'
               and owner_org=%s
               and visits_ever > 0
             order by visits_recently desc
             """


    datasets = model.meta.engine.execute(sql, org['id'])

    log.debug("Organization Dataset statistics: %s", datasets)

    extra_vars = { 'datasets': datasets,
                   'group_dict': org,
                   'group_type': "organization" }

    return toolkit.render('organization/dataset_stats.html', extra_vars)

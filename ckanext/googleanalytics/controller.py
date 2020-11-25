from __future__ import absolute_import
from builtins import str
import logging
import csv
import io

import ckan.model as model
from ckan.plugins import toolkit
from ckan.lib.base import BaseController, c, render, request, abort
from . import dbutil

import ckan.logic as logic
import hashlib
from . import plugin
from pylons import config
from ckanext.googleanalytics import dbutil
from ckanext.googleanalytics import helper as ga_h
from ckan.common import _, c, request, response
import ckan.lib.helpers as h
from paste.util.multidict import MultiDict
from ckan.controllers.api import ApiController
from ckan.controllers.user import UserController
from ckanext.googleanalytics import action as ga_action
from datetime import datetime

from . import reports

log = logging.getLogger('ckanext.googleanalytics')
check_access = logic.check_access
ValidationError = logic.ValidationError
NotAuthorized = logic.NotAuthorized


class GAReport(UserController):
    """
    Controller for Google Analytics report generation.
    TODO:
        -  When validation error. The for does not retain value
        - Check the core CKAN controller on how this retention of the form value is handled.
    """
    def _clear_tables(self):
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

    def _get_db_data(self, db_model, run_id):
        """
        Get the data from the database for download.
        :return: ORM object
        """
        data = model.Session.query(db_model).filter(db_model.run_id == run_id).all()
        fieldnames = [column.key for column in db_model.__table__.columns if not column.key.startswith("_")]
        return data, fieldnames

    def report(self, id=None):

        context = {
            'model': model, 'session': model.Session,
            'user': c.user, 'auth_user_obj': c.userobj,
            'for_view': True
        }
        data_dict = {
            'id': id,
            'user_obj': c.userobj,
            'include_datasets': True,
            'include_num_followers': True
        }

        self._setup_template_variables(context, data_dict)
        table_data = ga_h.get_recent_runs()

        vars = {
            "user_dict": c.user_dict,
            "table_data": table_data,
            "errors": {},
            "error_summary": {},
            'form_options': ga_h.get_ga_select_form_options(),
            'default_selected': config.get("ckanext.odm.site_code"),
            'data_dict': dict()
        }

        try:
            check_access('sysadmin', context, data_dict)
        except NotAuthorized:
            abort(403, _('Unauthorized to view or run this. Only sysadmin can run or view this'))

        if request.method == "GET":
            vars['data_dict']['to_dt'] = datetime.now().strftime('%Y-%m-%d')
            return render('user/ga_report.html', extra_vars=vars)

        if request.method == "POST":
            _parms = request.params

            # This on press of button Generate Report
            if "run" in _parms:
                data_dict['from_dt'] = _parms.get('from_dt')
                data_dict['to_dt'] = _parms.get('to_dt')
                data_dict['site_code'] = _parms.get('site_code')
                try:
                    res = ga_action.ga_report_run(context, data_dict)
                    h.flash_success(_('Background job has been triggered. '
                                      'Please visit this page after sometime. Id: {}'.format(res.get("job_id", ''))))
                except logic.NotAuthorized as e:
                    vars["errors"] = e.error_dict
                    vars["error_summary"] = e.error_summary
                    h.flash_error(_("Not authorized to run this. Only sysadmin can run this."))
                    abort(403, _('Unauthorized to view or run this.'))
                except logic.ValidationError as e:
                    vars["errors"] = e.error_dict
                    vars["error_summary"] = e.error_summary
                    vars['data_dict'] = data_dict
                    h.flash_error(_("Form validation error. Please check the given dates"))
                    return render('user/ga_report.html', extra_vars=vars)

            # This on press of button Clear
            elif "clear" in _parms:
                self._clear_tables()
                h.flash_success(_('Cleared all Google Analytics report table'))

            report_page = h.url_for(controller='ckanext.googleanalytics.controller:GAReport', action='report', id=id)
            h.redirect_to(report_page)

    def download(self, id=None, run_id=None, action_name=None):
        return reports.download(id, run_id, action_name)

class GAController(BaseController):

    def view(self):
        # get package objects corresponding to popular GA content
        c.top_resources = dbutil.get_top_resources(limit=10)
        return render("summary.html")


class GAApiController(ApiController):
    # intercept API calls to record via google analytics
    def _post_analytics(
        self, user, request_obj_type, request_function, request_id
    ):
        if config.get("googleanalytics.id"):
            data_dict = {
                "v": 1,
                "tid": config.get("googleanalytics.id"),
                "cid": hashlib.md5(user).hexdigest(),
                # customer id should be obfuscated
                "t": "event",
                "dh": c.environ["HTTP_HOST"],
                "dp": c.environ["PATH_INFO"],
                "dr": c.environ.get("HTTP_REFERER", ""),
                "ec": "CKAN API Request",
                "ea": request_obj_type + request_function,
                "el": request_id,
            }
            plugin.GoogleAnalyticsPlugin.analytics_queue.put(data_dict)

    def action(self, logic_function, ver=None):
        try:
            function = logic.get_action(logic_function)
            side_effect_free = getattr(function, "side_effect_free", False)
            request_data = self._get_request_data(
                try_url_params=side_effect_free
            )
            if isinstance(request_data, dict):
                id = request_data.get("id", "")
                if "q" in request_data:
                    id = request_data["q"]
                if "query" in request_data:
                    id = request_data["query"]
                self._post_analytics(c.user, logic_function, "", id)
        except Exception as e:
            log.debug(e)
            pass
        return ApiController.action(self, logic_function, ver)

    def list(self, ver=None, register=None, subregister=None, id=None):
        self._post_analytics(
            c.user,
            register + ("_" + str(subregister) if subregister else ""),
            "list",
            id,
        )
        return ApiController.list(self, ver, register, subregister, id)

    def show(
        self, ver=None, register=None, subregister=None, id=None, id2=None
    ):
        self._post_analytics(
            c.user,
            register + ("_" + str(subregister) if subregister else ""),
            "show",
            id,
        )
        return ApiController.show(self, ver, register, subregister, id, id2)

    def update(
        self, ver=None, register=None, subregister=None, id=None, id2=None
    ):
        self._post_analytics(
            c.user,
            register + ("_" + str(subregister) if subregister else ""),
            "update",
            id,
        )
        return ApiController.update(self, ver, register, subregister, id, id2)

    def delete(
        self, ver=None, register=None, subregister=None, id=None, id2=None
    ):
        self._post_analytics(
            c.user,
            register + ("_" + str(subregister) if subregister else ""),
            "delete",
            id,
        )
        return ApiController.delete(self, ver, register, subregister, id, id2)

    def search(self, ver=None, register=None):
        id = None
        try:
            params = MultiDict(self._get_search_params(request.params))
            if "q" in list(params.keys()):
                id = params["q"]
            if "query" in list(params.keys()):
                id = params["query"]
        except ValueError as e:
            log.debug(str(e))
            pass
        self._post_analytics(c.user, register, "search", id)

        return ApiController.search(self, ver, register)

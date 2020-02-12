from ckan.common import config
from ckan.plugins import toolkit
from ckan import logic
from ckanext.googleanalytics.ga import validators
from ckanext.googleanalytics.ga import run
import ckan.lib.jobs as jobs
import logging

log = logging.getLogger(__name__)
ValidationError = logic.ValidationError


@toolkit.side_effect_free
def ga_report_run(context, data_dict):
    """
    Run the Google Analytics report for the given time period (from and to date)
    Functionality:
        - Only sysadmin can run this
        - Get allowed period from config (default is 6 months)
        - Initialise the service
    :param context:
    :param data_dict:
    :return:
    """

    # Raises errors if not a sysadmin
    toolkit.check_access('sysadmin', context, data_dict)

    for key in ('from_dt', 'to_dt'):
        try:
            validators.validate_date(key, data_dict)
        except ValueError:
            raise ValidationError({key: ["Not a valid date"]})

    # Validate if the from date is greater than the to date
    #
    errors = validators.check_date_period(data_dict, {})

    if errors:
        raise ValidationError(errors)

    # Initialise the GA service
    start_dt = data_dict.get('from_dt')
    end_dt = data_dict.get('to_dt')

    # run as background job
    job = jobs.enqueue(run.run_ga, args=[start_dt, end_dt])

    log.info("Triggered a background job for Google Analytics")
    log.info("JOB ID: {}".format(job.id))

    return {
        "message": "Triggered the background job",
        "job_id": job.id
    }

import pytest

from ckan.cli.cli import ckan
from ckan.cli import CKANConfigLoader

from ckan import model

def test_group(cli, ckan_config):
    result = cli.invoke(ckan, ['-c', ckan_config['__file__'],
                               'googleanalytics'])
    assert 'load' in result.output
    assert 'init' in result.output
    assert 'report' in result.output


@pytest.mark.usefixtures('clean_db')
def test_init(cli, ckan_config):
    tables = ['ga_report_package', 'ga_report_resource',
              'ga_report_events', 'package_stats', 'resource_stats']

    for table in tables:
        # this should error
        with pytest.raises(Exception):
            model.Session.execute("select * from %s limit 0" % table)

    result = cli.invoke(ckan, ['-c', ckan_config['__file__'],
                               'googleanalytics', 'init'])

    for table in tables:
        # this shouldn't error
        model.Session.execute("select * from %s limit 0" % table)

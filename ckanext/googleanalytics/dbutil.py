from sqlalchemy import Table, Column, Integer, String, MetaData, Boolean, DateTime, Text
from sqlalchemy.sql import select, text
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
import ckan.model as model

# from ckan.model.authz import PSEUDO_USER__VISITOR
from ckan.lib.base import *

cached_tables = {}

Base = declarative_base()


class GAReportPackage(Base):
    """
    All Google Analytics report for package details are stored here.
    """
    __tablename__ = 'ga_report_package'
    _key = Column(Integer, primary_key=True)
    package_id = Column(String(60), nullable=False)
    package_title = Column(Text, nullable=True)
    run_id = Column(String(60), nullable=False)
    metric_type = Column(String(60), nullable=False)
    metrics = Column(String(60), nullable=False)
    site_code = Column(String(20), nullable=False)
    label = Column(Text)
    visits = Column(Integer)
    visitors = Column(Integer)
    new_visits = Column(Integer)
    unique_pageviews = Column(Integer)
    type = Column(String(60))
    state = Column(String(60))
    private = Column(Boolean, nullable=False)
    from_date = Column(DateTime, nullable=False)
    to_date = Column(DateTime, nullable=False)
    time_created = Column(DateTime(timezone=True), server_default=func.now())


class GAReportResource(Base):
    """
    All Google Analytics report for resource details are stored here.
    - Resource views
    - Resource downloads from page views (for accurate results of download created resource download event)
    """
    __tablename__ = 'ga_report_resource'
    _key = Column(Integer, primary_key=True)
    resource_id = Column(String(60), nullable=False)
    resource_title = Column(Text, nullable=True)
    run_id = Column(String(60), nullable=False)
    metric_type = Column(String(60), nullable=False)
    metrics = Column(String(60), nullable=False)
    site_code = Column(String(20), nullable=False)
    label = Column(Text)
    visits = Column(Integer)
    visitors = Column(Integer)
    new_visits = Column(Integer)
    unique_pageviews = Column(Integer)
    state = Column(String(60))
    from_date = Column(DateTime, nullable=False)
    to_date = Column(DateTime, nullable=False)
    time_created = Column(DateTime(timezone=True), server_default=func.now())


class GAReportEvents(Base):
    """
    All Google Analytics events are registered here.
    """
    __tablename__ = 'ga_report_events'
    _key = Column(Integer, primary_key=True)
    id = Column(String(60), nullable=False)
    title = Column(Text, nullable=True)
    run_id = Column(String(60), nullable=False)
    metric_type = Column(String(60), nullable=False)
    metric_value = Column(Integer)
    site_code = Column(String(20), nullable=False)
    label = Column(Text)
    category = Column(String(200), nullable=False)
    action = Column(String(200), nullable=False)
    from_date = Column(DateTime, nullable=False)
    to_date = Column(DateTime, nullable=False)
    time_created = Column(DateTime(timezone=True), server_default=func.now())


def init_tables():
    metadata = MetaData()
    package_stats = Table(
        "package_stats",
        metadata,
        Column("package_id", String(60), primary_key=True),
        Column("visits_recently", Integer),
        Column("visits_ever", Integer),
    )
    resource_stats = Table(
        "resource_stats",
        metadata,
        Column("resource_id", String(60), primary_key=True),
        Column("visits_recently", Integer),
        Column("visits_ever", Integer),
    )
    metadata.create_all(model.meta.engine)
    # This is for ORM tables
    Base.metadata.create_all(model.meta.engine)


def get_table(name):
    if name not in cached_tables:
        meta = MetaData()
        meta.reflect(bind=model.meta.engine)
        table = meta.tables[name]
        cached_tables[name] = table
    return cached_tables[name]


def _update_visits(table_name, item_id, recently, ever):
    stats = get_table(table_name)
    id_col_name = "%s_id" % table_name[: -len("_stats")]
    id_col = getattr(stats.c, id_col_name)
    s = select([func.count(id_col)], id_col == item_id)
    connection = model.Session.connection()
    count = connection.execute(s).fetchone()
    if count and count[0]:
        connection.execute(
            stats.update()
            .where(id_col == item_id)
            .values(visits_recently=recently, visits_ever=ever)
        )
    else:
        values = {
            id_col_name: item_id,
            "visits_recently": recently,
            "visits_ever": ever,
        }
        connection.execute(stats.insert().values(**values))


def update_resource_visits(resource_id, recently, ever):
    return _update_visits("resource_stats", resource_id, recently, ever)


def update_package_visits(package_id, recently, ever):
    return _update_visits("package_stats", package_id, recently, ever)


def get_resource_visits_for_url(url):
    connection = model.Session.connection()
    count = connection.execute(
        text(
            """SELECT visits_ever FROM resource_stats, resource
        WHERE resource_id = resource.id
        AND resource.url = :url"""
        ),
        url=url,
    ).fetchone()
    return count and count[0] or ""


""" get_top_packages is broken, and needs to be rewritten to work with
CKAN 2.*. This is because ckan.authz has been removed in CKAN 2.*

See commit ffa86c010d5d25fa1881c6b915e48f3b44657612
"""


def get_top_packages(limit=20):
    items = []
    # caveat emptor: the query below will not filter out private
    # or deleted datasets (TODO)
    q = model.Session.query(model.Package)
    connection = model.Session.connection()
    package_stats = get_table("package_stats")
    s = select(
        [
            package_stats.c.package_id,
            package_stats.c.visits_recently,
            package_stats.c.visits_ever,
        ]
    ).order_by(package_stats.c.visits_recently.desc())
    res = connection.execute(s).fetchmany(limit)
    for package_id, recent, ever in res:
        item = q.filter("package.id = '%s'" % package_id)
        if not item.count():
            continue
        items.append((item.first(), recent, ever))
    return items


def get_top_resources(limit=20):
    items = []
    connection = model.Session.connection()
    resource_stats = get_table("resource_stats")
    s = select(
        [
            resource_stats.c.resource_id,
            resource_stats.c.visits_recently,
            resource_stats.c.visits_ever,
        ]
    ).order_by(resource_stats.c.visits_recently.desc())
    res = connection.execute(s).fetchmany(limit)
    for resource_id, recent, ever in res:
        item = model.Session.query(model.Resource).filter(
            "resource.id = '%s'" % resource_id
        )
        if not item.count():
            continue
        items.append((item.first(), recent, ever))
    return items


def _set_attributes(ga_model, item, start_date, end_date):
    """
    Set attributes to the given model
    :param ga_model: ORM model
    :param item: dict
    :return: model
    """
    report = ga_model()
    report.from_date = start_date
    report.to_date = end_date
    for _field in item:
        setattr(report, _field, item.get(_field))
    return report


def insert_ga_report(table_name, data, start_date=None, end_date=None):
    """
    Insert values to package and resource views
    :param table_name: ga_report_package/ga_report_resource
    :param data: dict
    :param start_date: str
    :param end_date: str
    :return: None
    """
    ga_model = None
    for tb in (GAReportPackage, GAReportResource):
        if tb.__tablename__ == table_name:
            ga_model = tb

    if ga_model:
        for _key in data:
            item = data.get(_key)
            report = _set_attributes(ga_model, item, start_date, end_date)
            model.Session.add(report)

        try:
            log.info("Committing the insert operation")
            model.Session.commit()
        except Exception as e:
            print("********")
            print(e)
            log.error("Some error while committing. Hence rollback")
            log.error(e)
            model.Session.rollback()


def insert_ga_events(data, start_date=None, end_date=None):
    """
    Insert all the events data to the table
    :param data: dict
    :param start_date: str
    :param end_date: str
    :return: None
    """
    ga_model = GAReportEvents
    for _action in data:
        action = data.get(_action)
        for _item in action:
            item = action.get(_item)
            report = _set_attributes(ga_model, item, start_date, end_date)
            model.Session.add(report)

        try:
            log.info("Committing the insert operation")
            model.Session.commit()
        except Exception as e:
            log.error("Some error while committing. Hence rollback")
            log.error(e)
            model.Session.rollback()

"""Idempotently provision the read-only connection, datasets and three dashboard charts."""

import json
import os
from urllib.parse import quote_plus

from sqlalchemy import text

from superset.app import create_app
from superset.connectors.sqla.models import SqlaTable
from superset.extensions import db
from superset.models.core import Database
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice


def metric(expression, label):
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "label": label,
        "optionName": label,
        "hasCustomLabel": True,
    }


def main():
    app = create_app()
    with app.app_context():
        database = (
            db.session.query(Database).filter_by(database_name="Cosmetics analytics").one_or_none()
        )
        if database is None:
            database = Database(database_name="Cosmetics analytics")
            db.session.add(database)
        database.sqlalchemy_uri = (
            "clickhousedb://dashboard:"
            + quote_plus(os.environ["CLICKHOUSE_DASHBOARD_PASSWORD"])
            + "@clickhouse:8123/cosmetics_analytics"
        )
        database.allow_dml = False
        database.expose_in_sqllab = True
        db.session.commit()
        # The BI service may start before the first release. A later `make dashboard`
        # completes provisioning; application startup must not hide other failures.
        with database.get_sqla_engine() as engine, engine.connect() as connection:
            exists = connection.execute(
                text(
                    "SELECT count() FROM system.tables WHERE database='cosmetics_analytics' "
                    "AND name='daily_sales_by_category'"
                )
            ).scalar()
        if not exists:
            print("No serving release yet. Run the DAG, then make dashboard.")
            return
        specifications = [
            (
                "daily_sales_by_category",
                "Revenue by category",
                "echarts_timeseries_line",
                {
                    "granularity_sqla": "event_date",
                    "time_grain_sqla": "P1D",
                    "metrics": [metric("sum(revenue)", "Revenue")],
                    "groupby": ["category_code"],
                },
            ),
            (
                "funnel_steps_daily",
                "Ordered session funnel",
                "echarts_timeseries_line",
                {
                    "granularity_sqla": "event_date",
                    "time_grain_sqla": "P1D",
                    "groupby": [],
                    "metrics": [
                        metric(f"sum({column})", label)
                        for column, label in [
                            ("sessions_view", "Viewed"),
                            ("sessions_cart_after_view", "Cart after view"),
                            ("sessions_purchase_after_cart", "Purchased after cart"),
                        ]
                    ],
                },
            ),
            (
                "rfm_segments",
                "Customer segments",
                "pie",
                {
                    "groupby": ["segment"],
                    "metric": metric("count(*)", "Customers"),
                },
            ),
        ]
        charts = []
        for name, title, viz, params in specifications:
            dataset = (
                db.session.query(SqlaTable)
                .filter_by(
                    database_id=database.id,
                    schema="cosmetics_analytics",
                    table_name=name,
                )
                .one_or_none()
            )
            if dataset is None:
                dataset = SqlaTable(
                    database=database, schema="cosmetics_analytics", table_name=name
                )
                db.session.add(dataset)
                db.session.flush()
            dataset.fetch_metadata()
            chart = db.session.query(Slice).filter_by(slice_name=title).one_or_none()
            if chart is None:
                chart = Slice(slice_name=title)
                db.session.add(chart)
            chart.datasource_id = dataset.id
            chart.datasource_type = "table"
            chart.viz_type = viz
            chart.params = json.dumps(
                {
                    **params,
                    "viz_type": viz,
                    "datasource": f"{dataset.id}__table",
                    "time_range": "No filter",
                    "row_limit": 10000,
                    "adhoc_filters": [],
                }
            )
            charts.append(chart)
        db.session.flush()
        dashboard = db.session.query(Dashboard).filter_by(slug="cosmetics-commerce").one_or_none()
        if dashboard is None:
            dashboard = Dashboard(
                dashboard_title="Cosmetics · Commerce analytics", slug="cosmetics-commerce"
            )
            db.session.add(dashboard)
        dashboard.slices = charts
        dashboard.published = True
        layout = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
            "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": []},
        }
        for i, chart in enumerate(charts):
            row, card = f"ROW-{i}", f"CHART-{chart.id}"
            layout["GRID_ID"]["children"].append(row)
            layout[row] = {
                "id": row,
                "type": "ROW",
                "parents": ["ROOT_ID", "GRID_ID"],
                "children": [card],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            layout[card] = {
                "id": card,
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row],
                "meta": {
                    "chartId": chart.id,
                    "width": 12,
                    "height": 50,
                    "sliceName": chart.slice_name,
                },
            }
        dashboard.position_json = json.dumps(layout)
        sales_dataset = (
            db.session.query(SqlaTable)
            .filter_by(
                database_id=database.id,
                schema="cosmetics_analytics",
                table_name="daily_sales_by_category",
            )
            .one()
        )
        filters = []
        for column in ("category_code", "brand"):
            filters.append(
                {
                    "id": f"NATIVE_FILTER-{column}",
                    "type": "NATIVE_FILTER",
                    "name": column,
                    "filterType": "filter_select",
                    "targets": [{"datasetId": sales_dataset.id, "column": {"name": column}}],
                    "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
                    "cascadeParentIds": [],
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": [c.id for c in charts[1:]]},
                    "controlValues": {
                        "enableEmptyFilter": False,
                        "multiSelect": True,
                        "searchAllOptions": False,
                    },
                }
            )
        dashboard.json_metadata = json.dumps({"native_filter_configuration": filters})
        db.session.commit()
        print("Provisioned /superset/dashboard/cosmetics-commerce/")


if __name__ == "__main__":
    main()

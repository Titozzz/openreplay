import random
from datetime import datetime, timedelta

from chalicelib.utils.metrics_helper import __get_step_size
from chalicelib.utils import pg_client, helper
from schemas import AssistStatsAverage, AssistStatsSessionsRequest, schemas, AssistStatsSessionsResponse, \
    AssistStatsTopMembersResponse


def get_averages(
        project_id: int,
        start_timestamp: int,
        end_timestamp: int,
        user_id: int = None,
):
    step_size = __get_step_size(start_timestamp, end_timestamp, 100, decimal=True)

    constraints = [
        "project_id = %(project_id)s",
        "timestamp BETWEEN %(start_timestamp)s AND %(end_timestamp)s",
    ]

    params = {
        "project_id": project_id,
        "limit": 5,
        "offset": 0,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "step_size": f"{60} seconds",
    }

    if user_id is not None:
        constraints.append("agent_id = %(agent_id)s")
        params["agent_id"] = user_id

    totals = __get_all_events_totals(constraints, params)
    rows = __get_all_events_averages(constraints, params)

    params["start_timestamp"] = start_timestamp - (end_timestamp - start_timestamp)
    params["end_timestamp"] = start_timestamp
    previous_totals = __get_all_events_totals(constraints, params)

    return {
        "currentPeriod": totals[0],
        "previousPeriod": previous_totals[0],
        "list": rows,
    }


def __get_all_events_totals(constraints, params):
    sql = f"""
       SELECT ROUND(SUM(CASE WHEN event_type = 'assist' THEN duration ELSE 0 END))  as assist_total,
           ROUND(AVG(CASE WHEN event_type = 'assist' THEN duration ELSE 0 END))     as assist_avg,
           ROUND(SUM(CASE WHEN event_type = 'call' THEN duration ELSE 0 END))       as call_total,
           ROUND(AVG(CASE WHEN event_type = 'call' THEN duration ELSE 0 END))       as call_avg,
           ROUND(SUM(CASE WHEN event_type = 'control' THEN duration ELSE 0 END))    as control_total,
           ROUND(AVG(CASE WHEN event_type = 'control' THEN duration ELSE 0 END))    as control_avg
        FROM assist_events
        WHERE {' AND '.join(f'{constraint}' for constraint in constraints)}
    """
    with pg_client.PostgresClient() as cur:
        query = cur.mogrify(sql, params)
        cur.execute(query)
        rows = cur.fetchall()
    return helper.list_to_camel_case(rows)


def __get_all_events_averages(constraints, params):
    sql = f"""
        WITH time_series AS (
            SELECT
                EXTRACT(epoch FROM generate_series(
                    date_trunc('minute', to_timestamp(%(start_timestamp)s/1000)),
                    date_trunc('minute', to_timestamp(%(end_timestamp)s/1000)),
                    interval %(step_size)s
                ))::bigint as unix_time
        )
        SELECT
            time_series.unix_time as time,
            ROUND(AVG(CASE WHEN event_type = 'assist' THEN duration ELSE 0 END)) as assist_avg,
            ROUND(AVG(CASE WHEN event_type = 'call' THEN duration ELSE 0 END)) as call_avg,
            ROUND(AVG(CASE WHEN event_type = 'control' THEN duration ELSE 0 END)) as control_avg,
            ROUND(SUM(CASE WHEN event_type = 'assist' THEN duration ELSE 0 END)) as assist_total,
            ROUND(SUM(CASE WHEN event_type = 'call' THEN duration ELSE 0 END)) as call_total,
            ROUND(SUM(CASE WHEN event_type = 'control' THEN duration ELSE 0 END)) as control_total
        FROM
            time_series
            LEFT JOIN assist_events ON time_series.unix_time = EXTRACT(epoch FROM DATE_TRUNC('minute', to_timestamp(assist_events.timestamp/1000)))
        WHERE
            {' AND '.join(f'{constraint}' for constraint in constraints)}
        GROUP BY time
        ORDER BY time;

    """
    with pg_client.PostgresClient() as cur:
        query = cur.mogrify(sql, params)
        cur.execute(query)
        rows = cur.fetchall()
    return helper.list_to_camel_case(rows)


def get_top_members(
        project_id: int,
        start_timestamp: int,
        end_timestamp: int,
        sort_by: str,
        sort_order: str,
        user_id: int = None,
) -> AssistStatsTopMembersResponse:
    event_type_mapping = {
        "sessionsAssisted": "assist",
        "assistDuration": "assist",
        "callDuration": "call",
        "controlDuration": "control"
    }

    event_type = event_type_mapping.get(sort_by)
    if event_type is None:
        raise ValueError("Invalid sortBy option")

    constraints = [
        "project_id = %(project_id)s",
        "timestamp BETWEEN %(start_timestamp)s AND %(end_timestamp)s",
        "duration > 0",
        # "event_type = %(event_type)s",
    ]

    params = {
        "project_id": project_id,
        "limit": 5,
        "offset": 0,
        "sort_by": sort_by,
        "sort_order": sort_order.upper(),
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        # "event_type": event_type,
    }

    if user_id is not None:
        constraints.append("agent_id = %(agent_id)s")
        params["agent_id"] = user_id

    sql = f"""
        SELECT
            COUNT(1) OVER () AS total,
            ae.agent_id,
            u.name AS name,
            CASE WHEN '{sort_by}' = 'sessionsAssisted'
                 THEN SUM(CASE WHEN ae.event_type = 'assist' THEN 1 ELSE 0 END)
                 ELSE SUM(CASE WHEN ae.event_type <> 'assist' THEN ae.duration ELSE 0 END)
            END AS count
        FROM assist_events ae
            JOIN users u ON u.user_id = ae.agent_id
        WHERE {' AND '.join(f'ae.{constraint}' for constraint in constraints)}
            AND ae.event_type = '{event_type}'
        GROUP BY ae.agent_id, u.name
        ORDER BY count {params['sort_order']}
        LIMIT %(limit)s OFFSET %(offset)s
    """

    with pg_client.PostgresClient() as cur:
        query = cur.mogrify(sql, params)
        cur.execute(query)
        rows = cur.fetchall()

    if len(rows) == 0:
        return AssistStatsTopMembersResponse(total=0, list=[])

    count = rows[0]["total"]
    rows = helper.list_to_camel_case(rows)
    for row in rows:
        row.pop("total")

    return AssistStatsTopMembersResponse(total=count, list=rows)


def get_sessions(
        project_id: int,
        data: AssistStatsSessionsRequest,
) -> AssistStatsSessionsResponse:
    constraints = [
        "project_id = %(project_id)s",
        "timestamp BETWEEN %(start_timestamp)s AND %(end_timestamp)s",
    ]

    params = {
        "project_id": project_id,
        "limit": data.limit,
        "offset": (data.page - 1) * data.limit,
        "sort_by": data.sort,
        "sort_order": data.order.upper(),
        "start_timestamp": data.startTimestamp,
        "end_timestamp": data.endTimestamp,
    }

    if data.userId is not None:
        constraints.append("agent_id = %(agent_id)s")
        params["agent_id"] = data.userId

    sql = f"""
        SELECT
            COUNT(1) OVER () AS count,
            ae.session_id,
            MIN(ae.timestamp) as timestamp,
            SUM(CASE WHEN ae.event_type = 'call' THEN ae.duration ELSE 0 END) AS call_duration,
            SUM(CASE WHEN ae.event_type = 'control' THEN ae.duration ELSE 0 END) AS control_duration,
            SUM(CASE WHEN ae.event_type = 'assist' THEN ae.duration ELSE 0 END) AS assist_duration,
            (SELECT json_agg(json_build_object('name', u.name, 'id', u.user_id))
                    FROM users u
                    WHERE u.user_id = ANY (array_agg(ae.agent_id)))                     AS team_members
        FROM assist_events ae
        WHERE {' AND '.join(f'ae.{constraint}' for constraint in constraints)}
        GROUP BY ae.session_id
        ORDER BY {params['sort_by']} {params['sort_order']}
        LIMIT %(limit)s OFFSET %(offset)s
    """

    with pg_client.PostgresClient() as cur:
        query = cur.mogrify(sql, params)
        cur.execute(query)
        rows = cur.fetchall()

    if len(rows) == 0:
        return AssistStatsSessionsResponse(total=0, page=1, list=[])

    count = rows[0]["count"]

    rows = helper.list_to_camel_case(rows)
    for row in rows:
        row.pop("count")
    return AssistStatsSessionsResponse(total=count, page=data.page, list=rows)


def export_csv() -> schemas.AssistStatsSessionsResponse:
    data = get_sessions()
    return data

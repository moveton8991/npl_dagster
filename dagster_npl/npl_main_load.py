from dagster import op, job, DynamicOut, DynamicOutput, In, ConfigMapping, multiprocess_executor, Nothing

import boto3
from datetime import datetime
import re
import zipfile
import io
import clickhouse_connect
import json
import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Получение значений переменных
key_id = os.getenv('KEY_ID')
key_value = os.getenv('KEY_VALUE')
endpoint_url = os.getenv('ENDPOINT_URL')
bucket_name = os.getenv('BUCKET_NAME')

clickhouse_user = os.getenv('CLICKHOUSE_USER')
clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD')
clickhouse_database = os.getenv('CLICKHOUSE_DATABASE')
host = os.getenv('HOST')
port = os.getenv('PORT')


def get_click_connect():
    client = clickhouse_connect.get_client(
    host=host,
    port=port,
    username=clickhouse_user,
    password=clickhouse_password,
    database=clickhouse_database
    )
    return client

def get_s3_connection():

    session = boto3.Session(
    aws_access_key_id=key_id,
    aws_secret_access_key=key_value,
    region_name="ru-central1"
    )
    s3 = session.client('s3', endpoint_url=endpoint_url)
    return s3


@op(out=DynamicOut())
def get_filename(context, date_start: str, date_end: str):
    '''Получение списка файлов для загрузки'''

    date_start = datetime.strptime(date_start, "%Y-%m-%d")
    date_end = datetime.strptime(date_end, "%Y-%m-%d")
    s3 = get_s3_connection()
    count = 0
    for key in s3.list_objects(Bucket=bucket_name)['Contents']:
        file_name = key['Key']
        parts = file_name.split('/')
        year_part = month_part = day_part = None

        for part in parts:
            if part.startswith('year='):
                year_part = part.split('=')[1]
            elif part.startswith('month='):
                month_part = part.split('=')[1]
            elif part.startswith('day='):
                day_part = part.split('=')[1]

        if not (year_part and month_part and day_part):
            context.log.info(f"Invalid file format: {file_name}. Expected year, month, and day.")
            continue

        file_date_str = f"{year_part}-{month_part}-{day_part}"
        file_date = datetime.strptime(file_date_str, "%Y-%m-%d")

        if date_start <= file_date <= date_end:
            context.log.info(f"Found file: {file_name}")
            yield DynamicOutput(file_name, mapping_key=re.sub(r'[^A-Za-z0-9]', '_', file_name))
            count += 1
 

@op(ins={'filename': In(str)})
def process_files(context,filename):
    '''Обработка и загрука файлов в Clickhouse'''

    context.log.info(filename)
    source_file_name = filename
    s3 = get_s3_connection()
    client = get_click_connect()
    s3_object = s3.get_object(Bucket=bucket_name, Key=filename)

    # Определение маппинга между файлами и колонками
    # Определение маппинга между файлами и колонками
    columns_mapping = {
        'browser_events': {
            'target_table': 'raw_browser_events',
            'columns': [
                'click_id',                    # UUID
                'event_id',                    # UUID
                'event_timestamp',             # DateTime
                'event_type',                  # String
                'browser_name',                # String
                'browser_language',            # String
                'browser_user_agent',          # String
                'input_file_name'              # String (имя загружаемого файла)
            ]
        },
        'geo_events': {
            'target_table': 'raw_geo_events',
            'columns': [
                'click_id',
                'geo_country',
                'geo_timezone',
                'geo_region_name',
                'ip_address',
                'geo_latitude',
                'geo_longitude',
                'input_file_name'              # String (имя загружаемого файла)
            ]
        },
        'location_events': {
            'target_table': 'raw_location_events',
            'columns': [
                'event_id',                     # UUID
                'page_url',                     # String
                'page_url_path',                # String
                'referer_url',                  # String
                'referer_medium',               # String
                'utm_medium',                   # String
                'utm_source',                   # String
                'utm_content',                  # String
                'utm_campaign',                 # String
                'input_file_name'              # String (имя загружаемого файла)
            ]
        },
        'device_events': {
            'target_table': 'raw_device_events',
            'columns': [
                'click_id',                     # UUID
                'device_type',                  # String
                'device_is_mobile',             # Boolean
                'user_custom_id',               # String
                'user_domain_id',               # UUID
                'os',                            # String
                'os_name',                       # String
                'os_timezone',                   # String
                'input_file_name'              # String (имя загружаемого файла)
            ]
        }
    }

    with zipfile.ZipFile(io.BytesIO(s3_object['Body'].read()), 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            mapping_info = None
            
            for key, value in columns_mapping.items():
                if key in file_name:
                    mapping_info = value
                    break  # Теперь break находится в правильном месте

            if not mapping_info:
                context.log.warning(f'Нет соответствующего маппинга для файла: {file_name}')
                continue
            
            target_table = mapping_info['target_table']
            column_names = mapping_info['columns']
            
            # Удаляем данные по файлу, который грузим
            delete_query = f"DELETE FROM {target_table} WHERE input_file_name = '{source_file_name}';"
            client.query(delete_query)

            with zip_ref.open(file_name) as jsonl_file:
                rows = []
                for line in jsonl_file:
                    data = json.loads(line.decode('utf-8').strip())
                    row = [data.get(col) for col in column_names[:-1]]  # Все, кроме последней колонки
                    row.append(source_file_name)  # Добавление имени файла в последнюю колонку
                    rows.append(row)
                client.insert(target_table, rows, column_names=column_names)
                context.log.info(f"Успешно вставлено {len(rows)} записей из файла {file_name}.")


@op(
    ins={'process_files': In(Nothing)}
)
def browser_device_hourly_events_agg(context):
    '''Расчет витрины browser_device_hourly_events_agg'''

    target_table = 'browser_device_hourly_events_agg'
    truncate_query = f'truncate {target_table};'
    click_query = f'''
    insert into {target_table}
    select toStartOfHour(parseDateTimeBestEffortOrNull(be.event_timestamp)) as hour,
       be.browser_name,
       device_type,
       count(*)                                                         as total_event_count,
       countIf(page_url_path = '/confirmation')                         as buy_cnt
    from raw_browser_events be
            join raw_location_events le on be.event_id = le.event_id
            join (select distinct on (click_id) * from raw_device_events) de on be.click_id = de.click_id
    where 1=1
    group by hour, be.browser_name, device_type;
    '''
    context.log.info(click_query)
    click_connection = get_click_connect()
    click_connection.query(truncate_query)
    click_connection.query(click_query)

@op(
    ins={'process_files': In(Nothing)}
)
def browser_device_hourly_buy_leading_pages_agg(context):
    '''Расчет витрины browser_device_hourly_buy_leading_pages_agg'''

    target_table = 'browser_device_hourly_buy_leading_pages_agg'
    truncate_query = f'truncate {target_table};'
    click_query = f'''
    insert into {target_table}
    select
    toStartOfHour(parseDateTimeBestEffortOrNull(date_time)) as hour, leading_link as page_url, browser_name, device_type, count(*) as cnt
    from (
            select user_custom_id,
                    arrayJoin(arrayFilter(x -> startsWith(x.1, '/product_'), events_in_group)).3 as browser_name,
                    arrayJoin(arrayFilter(x -> startsWith(x.1, '/product_'), events_in_group)).4 as device_type,
                    arrayJoin(arrayFilter(x -> startsWith(x.1, '/product_'), events_in_group)).2 as date_time,
                    arrayJoin(arrayFilter(x -> startsWith(x.1, '/product_'), events_in_group)).1 as leading_link
            from (
                    select user_custom_id,
                            groupArray((page_url_path, event_timestamp, browser_name, device_type))                     AS events_in_group,
                            arrayExists(x -> x = '/confirmation', groupArray(page_url_path)) AS has_confirmation
                    from (
                            SELECT user_custom_id,
                                    page_url_path,
                                    event_timestamp, browser_name, device_type,
                                    SUM(CASE WHEN page_url_path = '/confirmation' THEN 1 ELSE 0 END)
                                        OVER (PARTITION BY user_custom_id ORDER BY event_timestamp ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS confirmation_group
                            FROM raw_browser_events bes
                                        join (select distinct on (click_id) * from raw_device_events) des
                                            ON bes.click_id = des.click_id
                                        join raw_location_events les on bes.event_id = les.event_id
                            )
                    GROUP BY user_custom_id, confirmation_group
                    )
            where has_confirmation = 1
            )
    group by hour, page_url, browser_name, device_type;
    '''
    context.log.info(click_query)
    click_connection = get_click_connect()
    click_connection.query(truncate_query)
    click_connection.query(click_query)

@op(
    ins={'process_files': In(Nothing)}
)
def source_campaign_buy_events_agg(context):
    '''Расчет витрины source_campaign_buy_events_agg'''

    target_table = 'source_campaign_buy_events_agg'
    truncate_query = f'truncate {target_table};'
    click_query = f'''
    insert into {target_table}
    select
    toStartOfHour(parseDateTimeBestEffortOrNull(be.event_timestamp)) AS hour,
    loc.utm_source,
    loc.utm_campaign,
    count(*) as cnt
    from raw_browser_events be
    JOIN location_events loc
    ON be.event_id = loc.event_id
    where
        page_url_path = '/confirmation'
    GROUP BY hour, loc.utm_source, loc.utm_campaign
    order by cnt desc
    '''
    context.log.info(click_query)
    click_connection = get_click_connect()
    click_connection.query(truncate_query)
    click_connection.query(click_query)

@op(
    ins={'process_files': In(Nothing)}
)
def hourly_sources_users_agg(context):
    '''Расчет витрины hourly_sources_users_agg'''

    target_table = 'hourly_sources_users_agg'
    truncate_query = f'truncate {target_table};'
    click_query = f'''
    insert into {target_table}
    select
    toStartOfHour(parseDateTimeBestEffortOrNull(be.event_timestamp)) AS hour,
    loc.utm_source as utm_source,
    count(distinct des.user_custom_id) as cnt_users
    from raw_browser_events be
    JOIN raw_location_events loc
    ON be.event_id = loc.event_id
    JOIN (select distinct on (click_id) * from raw_device_events) des
                        ON be.click_id = des.click_id
    GROUP BY hour, loc.utm_source
    order by hour, utm_source
    '''
    context.log.info(click_query)
    click_connection = get_click_connect()
    click_connection.query(truncate_query)
    click_connection.query(click_query)

@op(
    ins={'process_files': In(Nothing)}
)
def hourly_country_regions_users_agg(context):
    '''Расчет витрины hourly_country_regions_users_agg'''

    target_table = 'hourly_country_regions_users_agg'
    truncate_query = f'truncate {target_table};'
    click_query = f'''
    insert into {target_table}
    select
    toStartOfHour(parseDateTimeBestEffortOrNull(be.event_timestamp)) AS hour,
    geo_country,
    geo_region_name,
    count(distinct des.user_custom_id) as cnt_users
    from raw_browser_events be
    JOIN (select distinct on (click_id) * from raw_device_events) des
                        ON be.click_id = des.click_id
    JOIN (select distinct on (click_id) * from raw_geo_events) geo
                        ON be.click_id = geo.click_id
    JOIN raw_location_events loc
    ON be.event_id = loc.event_id
    where
        page_url_path = '/confirmation'
    GROUP BY hour, geo_country, geo_region_name
    order by hour, geo_country, geo_region_name
    '''
    context.log.info(click_query)
    click_connection = get_click_connect()
    click_connection.query(truncate_query)
    click_connection.query(click_query)


def config_mapping_fn(config):
    return {
        "ops": {
            "get_filename": {
                "inputs": {
                    "date_start": {"value": config["start_date"]},
                    "date_end": {"value": config["end_date"]},
                }
            },
        }
    }

@job(config=ConfigMapping(
        config_schema={"start_date": str, "end_date": str}, 
        config_fn=config_mapping_fn
    ),
    executor_def=multiprocess_executor.configured({
        'max_concurrent': 5
    }),
    )
def npl_job():
    files = get_filename()
    files_load = files.map(lambda filename: process_files(filename=filename))

    load_browser_device_hourly_events_agg = browser_device_hourly_events_agg(files_load.collect())
    load_browser_device_hourly_buy_leading_pages_agg = browser_device_hourly_buy_leading_pages_agg(files_load.collect())
    load_source_campaign_buy_events_agg = source_campaign_buy_events_agg(files_load.collect())
    load_hourly_sources_users_agg = hourly_sources_users_agg(files_load.collect())
    load_hourly_country_regions_users_agg = hourly_country_regions_users_agg(files_load.collect())

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from airflow.config_templates import airflow_local_settings
import airflow
from airflow.configuration import AirflowConfigParser

from pathlib import Path

from datetime import datetime, timedelta

import yaml

from elephantsql_kashin import conn_id

twos_day = datetime.now() - timedelta(days=2)

filtered_list = []

def config_items():
    config_file = Path(airflow_local_settings.__file__).parent / 'config.yml'
    with open(config_file, 'r') as file:
        parsed = yaml.safe_load(file)
        for i in parsed:
            section_name = i['name']
            for f in i['options']:
                entry = {
                    "section_name": section_name,
                    "version": airflow.__version__,
                    "key_name": f['name'],
                    "val_name": f['default'],
                    "type_name": f['type']
                    }
                filtered_list.append(entry)
        data = [( x['version'], x['section_name'], x['key_name'], x['val_name'], x['type_name']) for x in filtered_list]
        return data

#grabs the most 2 recent versions and compares them
#displays in the logs what keys and values the previous version has that the newer version doesn't
#and what keys and values the newest version has that the older version doesn't
#also it displays the deprecated options that airflows config parser checks for and maps to the correct keys and vals
#if the version being ran has different keys and vals
def hook_for_versions():
    request = f"SELECT DISTINCT version FROM config_by_version ORDER BY config_by_version.version DESC LIMIT 2"
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    data2 = cursor.fetchall()
    print(f"The versions being checked are: {data2}")

    #if theres 2 versions do the comparison
    if len(data2) > 1:
        #turns the 2 versions from tuples to strings to chop off the paranthesis and commas
        #to make it so postgresql can search for the respective version 
        latest_version = str(data2[0])
        print(f"This is the latest version being tested: {latest_version}")
        older_version = str(data2[1])
        print(f"This is the older version being tested: {older_version}")

        #grabs everything from the latest version
        request_latest = f"SELECT section, key, value, type FROM config_by_version WHERE version={latest_version[1:-2]}"
        pg_hook_latest = PostgresHook(postgres_conn_id=conn_id)
        connection_latest = pg_hook_latest.get_conn()
        cursor_latest = connection_latest.cursor()
        cursor_latest.execute(request_latest)
        #grabs all the data from the latest version and converts it to a set from a list for easy comparison
        latest_data = set(cursor_latest.fetchall())

        #grabs everything from the older version
        request_older = f"SELECT section, key, value, type FROM config_by_version WHERE version={older_version[1:-2]}"
        pg_hook_older = PostgresHook(postgres_conn_id=conn_id)
        connection_older = pg_hook_older.get_conn()
        cursor_older = connection_older.cursor()
        cursor_older.execute(request_older)
        #grabs all the data from the older version and converts it to a set from a list for easy comparison
        older_data = set(cursor_older.fetchall())

        in_latest_version_but_not_older_version = latest_data - older_data
        in_older_version_but_not_latest_version = older_data - latest_data
        #grabs the deprecated options that airflow's config parser checks for
        deprecated_key_vals = AirflowConfigParser.deprecated_options
        if in_older_version_but_not_latest_version != in_latest_version_but_not_older_version:
            #pretty print the output as pprint wasn't working correctly
            print(f"These keys and values are in {older_version} but not {latest_version} and could potentially break airflow upon upgrading: ") 
            for i in in_older_version_but_not_latest_version:
                print(i)
            print()
            
            print(f"These keys and values are in {latest_version} but not {older_version}: ")
            for i in in_latest_version_but_not_older_version:
                print(i)
            print()
            
            print(f"These are the deprecated options found in the configuration.py: ")
            for i in deprecated_key_vals.items():
                print(i)
            print()
        else:
            #lots of spaces to make the string show nicely in airflows logs
            print("\n                                                        !!! The versions config data is the same !!!\n")

default_args = {
    'owner':'airflow',
    'depends_on_past': True
}

with DAG(
    dag_id="config_parser",
    start_date=twos_day,
    default_args=default_args,
    schedule_interval=None,
    concurrency=3,
    tags=['core'],
) as dag:

    #creates the table if it doesn't already exist
    t3 = PostgresOperator(
       task_id="create_table_define_cols",
       postgres_conn_id=conn_id,
       sql="""
            CREATE TABLE IF NOT EXISTS config_by_version(
            version varchar,
            section varchar,
            key varchar,
            value varchar,
            type varchar,
            primary key(version, section, key));
           """
    )

    #pulls the versions from the database and compares
    t4 = PythonOperator(
            task_id="pull_versions",
            python_callable=hook_for_versions
            )
    #iterates through every version, section_name, key, value and datatype in the config_items() function
    for (version, section, key, val, type_name) in config_items():

        (
            t3 >>
            #if a row in the database already exists that matches what is being iterated through, delete it
            PostgresOperator(
                task_id=f"make_way_{section}_{key}",
                postgres_conn_id=conn_id,
                sql="""
                 DELETE FROM config_by_version
                 WHERE version=%s
                   AND section=%s
                   AND key=%s
                ;
                """,
                parameters=(version, section, key)
            ) >>
            #insert the configs version, sections, keys, values and datatype
            PostgresOperator(
                task_id=f"insert_{section}_{key}",
                postgres_conn_id=conn_id,
                sql="""
                 INSERT INTO config_by_version (version, section, key, value, type)
                 VALUES (%s, %s, %s, %s, %s);
                """,
                parameters=(version, section, key, val, type_name)
            )

        ) >> t4
    
        
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

from textwrap import dedent

twos_day = datetime.now() - timedelta(days=2)

default_args = {
    'owner':'airflow',
    'depends_on_past': True
}

with DAG(
    dag_id="rendered_templates_for_loop",
    start_date=twos_day,
    schedule_interval=None,
    tags=['core'],
) as dag:
    
    templated_command = dedent(
        """
    {% for i in range(3) %}
        echo {{ run_id }}
        sleep 3
        echo {{ params.current_time }}
    {% endfor %}
        """
    )
    
    jinja_for_loop = BashOperator(
        task_id="jinja_for_loop",
        bash_command=templated_command,
        params={'current_time': str(datetime.now())}
    )

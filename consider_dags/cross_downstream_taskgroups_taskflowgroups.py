from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
from airflow.models.baseoperator import cross_downstream, chain
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.dag import DAG
from airflow.utils.dot_renderer import render_dag
from textwrap import indent

from datetime import datetime, timedelta

two_days = datetime.now() - timedelta(days=2)

default_args = {
    'owner':'airflow',
    'depends_on_past': True
}

@task
def assert_homomorphic(task_group_names, **context):
    """
    The structure of all of the task groups above should be the same
    """
    # get the dag in dot notation, focus only on its edges
    dag = context["dag"]
    print(dag)
    #gives string which represents whole dag structure
    graph = render_dag(dag)
    print("Whole DAG:")
    print(indent(str(graph), "    "))
    lines = list(filter(lambda x: "->" in x, str(graph).split("\n")))

    # bin them by task group, then remove the group names
    group_strings = []
    #removes everything thats not a task name
    for name in task_group_names:
        print(name)
        relevant_lines = filter(lambda x: name in x, lines)
        normalized_lines = map(
        lambda x: x.strip().replace(name, ""), sorted(relevant_lines)
        )
        edges_str = "\n".join(normalized_lines)
        group_strings.append(edges_str)
        print(indent(edges_str, "    "))

    # these should be identical
    for xgroup, ygroup in zip(group_strings, group_strings[1:]):
        assert xgroup == ygroup


with DAG(
    dag_id="cross_downstream_group_compare3",
    schedule_interval=None,
    start_date=two_days,
    default_args=default_args,
    tags=['core'],
) as dag:
    
    with TaskGroup(group_id="group1") as tg1:
        t1 = DummyOperator(task_id="dummy1")
        t2 = DummyOperator(task_id="dummy2")
        t3 = DummyOperator(task_id="dummy3")
        t4 = DummyOperator(task_id="dummy4")
        t5 = DummyOperator(task_id="dummy5")
        t6 = DummyOperator(task_id="dummy6")
        t7 = DummyOperator(task_id="dummy7")
        @task
        def dummy0():
            return 0
        d0 = dummy0()
        cross_downstream(from_tasks=[d0, t1, t2, t3], to_tasks=[t4, t5, t6, t7])

    @task_group(group_id="group2")
    def dec_task_grouper():

        @task
        def dummy0():
            return 0

        @task
        def dummy1():
            return 1
        
        @task
        def dummy2():
            return 2

        @task
        def dummy3():
            return 3

        @task
        def dummy4():
            return 4
        
        @task
        def dummy5():
            return 5

        @task
        def dummy6():
            return 6
        
        @task
        def dummy7():
            return 7
        
        d0 = dummy0()
        t1 = dummy1()
        t2 = dummy2()
        t3 = dummy3()
        t4 = dummy4()
        t5 = dummy5()
        t6 = dummy6()
        t7 = dummy7()



        #chain([d0, t1, t2, t3], [t4, t5, t6, t7])
        chain(d0, [t4, t5, t6, t7])
        chain(t1, [t4, t5, t6, t7])
        chain(t2, [t4, t5, t6, t7])
        chain(t3, [t4, t5, t6, t7])

        return t7

    tg2 = dec_task_grouper()
    
chain([tg1, tg2], assert_homomorphic(["group1", "group2"]))
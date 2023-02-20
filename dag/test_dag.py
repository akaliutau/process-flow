from engine.keeper import InstanceKeeper
from engine.dag import DAG
from engine.dcontext import DContext
from engine.operators_impl import PythonOperator, BigQueryInsertJobOperator
from redux.store import create_store
from redux.store_test import simple_state_reducer


def func_1(context: DContext):
    pass


with DAG(store=create_store(reducer=simple_state_reducer)) as dag:
    op1 = PythonOperator(task_id='task_1', python_callable=func_1)
    op2 = BigQueryInsertJobOperator(task_id='task_2', project_id='test_prj', configuration={})

    op1 >> op2

    dag.run()


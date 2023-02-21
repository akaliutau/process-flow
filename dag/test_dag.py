from typing import Dict

from engine.dag import DAG
from engine.dcontext import DContext
from engine.operators_impl import PythonOperator, BigQueryInsertJobOperator
from engine.utils import log
from redux.store import create_store, Store


def operation_state_reducer(state: Dict, action: Dict) -> Dict:
    state = state or {}
    if not action:
        return state
    if action['type'] == 'operation_status_update':
        state['ack'] = True
    return state


def dummy_function(store: Store, context: DContext, **kwargs):
    log.info('inside func_1, context=%s', context)


with DAG(store=create_store(reducer=operation_state_reducer)) as dag:
    task1 = PythonOperator(task_id='task_1', python_callable=dummy_function, allow_to_fail=True)
    task2 = BigQueryInsertJobOperator(task_id='task_2', project_id='process-flow-demo-project', configuration={})

    task1 >> task2

    dag.run()

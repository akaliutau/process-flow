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


def func_1(store: Store, context: DContext, **kwargs):
    log.info('inside func_1, context=%s', context)


with DAG(store=create_store(reducer=operation_state_reducer)) as dag:
    op1 = PythonOperator(task_id='task_1', python_callable=func_1)
    op2 = BigQueryInsertJobOperator(task_id='task_2', project_id='test_prj', configuration={})

    op1 >> op2

    dag.run()

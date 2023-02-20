from dag.test_dag import dag
from engine.dag import DAG
from engine.dcontext import DContext
from engine.operators_impl import PythonOperator, BigQueryInsertJobOperator
from redux.store import create_store
from redux.store_test import simple_state_reducer


def func_1(context: DContext):
    pass


if __name__ == '__main__':
    print('starting test 1')
    store = create_store(reducer=simple_state_reducer)




# About

This is ProcessFlow - an AirFlow-like framework to execute directed acyclic graphs (DAGs) consisting from tasks of generic nature.

Advantages:

* Allows building fully asynchronous DAGs (harnesses `asyncio` library under the hood)
* Natively supports `Jinja2` templates
* Easily extensible
* Convenient to use - includes out-of-the-box python implementation of Redux library to implement in-memory state holder
* Light-weight and can serve as a replacement of heavy AirFlow execution engine
* DAGs are partially compatible with [AirFlow](https://github.com/apache/airflow)

The difference from AirFlow is - this framework is targeting the asynchronous, multi-thread execution of Tasks on one 
physical machine, rather than to orchestrate a swarm of distributed tasks running on multiple instances.

Tasks are generic and should implement `Operator` abstract class. For demonstration purpose we've implemented two of them:

* `PythonOperator` which is designed to invoke Python function
* `BigQueryInsertJobOperator` which is a mock class simulating wrapper around GCP SDK client libraries


# Installation

```shell
python -m venv env
source ./env/bin/activate
pip install -r requirements.txt
```

# Running

Execute the test dag from root directory:

```shell
python3 -m dag.test_dag
```
This DAG consists from two operators: a generic `PythonOperator` and specialized one, `BigQueryInsertJobOperator`

The output should show the sequential execution of tasks in DAG (according to defined graph structure):

```shell
 2023-11-20 12:09:54,766 [dag.py] DEBUG created dag class
 2023-11-20 12:09:54,766 [dag.py] INFO building graph...
 2023-11-20 12:09:54,766 [dag.py] DEBUG task_1
 2023-11-20 12:09:54,766 [dag.py] DEBUG task_2
 2023-11-20 12:09:54,766 [dag.py] INFO collected 2 task(s)
 2023-11-20 12:09:54,766 [dag.py] INFO running dag...
 2023-11-20 12:09:54,766 [execution_service.py] INFO started execution service
 2023-11-20 12:09:54,766 [execution_service.py] INFO adding to the queue task_1
 2023-11-20 12:09:54,766 [execution_service.py] INFO adding to the queue task_2
 2023-11-20 12:09:54,766 [test_dag.py] INFO inside func_1, context={"run_id": "<class 'uuid.UUID'>"}
 2023-11-20 12:09:54,766 [execution_service.py] INFO processed task_1
 2023-11-20 12:09:54,766 [execution_service.py] INFO processed task_2
 2023-11-20 12:09:54,766 [execution_service.py] DEBUG finalising execution service
 2023-11-20 12:09:54,766 [execution_service.py] INFO stopping loop...
 2023-11-20 12:09:54,766 [execution_service.py] DEBUG Shutdown complete
 2023-11-20 12:09:54,766 [dag.py] DEBUG finalising dag class
```
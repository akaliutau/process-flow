import traceback
import uuid
from typing import List

from jinja2 import Environment

from engine.constants import TaskResult, TaskStatus
from engine.dcontext import DContext
from engine.execution_service import ExecutionService
from engine.graph import Graph
from engine.keeper import keeper
from engine.operators import Operator
from engine.utils import log
from redux.store import Store


class DAG:

    def __init__(self, store: Store, context: DContext = None, environment: Environment = None):
        super().__init__()
        self._context = context or DContext()
        self._store = store
        self._context['run_id'] = str(uuid.UUID)
        keeper.register(DAG.__name__, self)
        self._environment = environment  # Environment(loader=FileSystemLoader(context.get('working_dir')))
        self._execution_graph = None

    def _get_task(self, task: Operator) -> Operator:
        task.environment = self._environment
        task.store = self._store
        task.context = self._context
        return task

    def _exec_task(self, task: Operator) -> None:
        try:
            task = self._get_task(task)
            task.set_status(TaskStatus.RUNNING)
            task.exec()
            task.result = TaskResult.SUCCESS
        except Exception as e:
            print(traceback.format_exc())
            self.result = TaskResult.ERROR
            self.error_msg = str(e)
            log.error(e)
        finally:
            task.set_status(TaskStatus.FINISHED)

    def _exec_graph(self):
        pass

    def get_dag_tasks(self) -> List[Operator]:
        tasks = list()
        for inst in keeper.get_instances(Operator.__name__):
            log.info(inst)
            tasks.append(inst)
        return tasks

    def compile(self):
        g = Graph(self.get_dag_tasks())
        g.prepare()
        self._execution_graph = g

    def run(self):
        log.info('running dag class')
        with ExecutionService() as es:
            es.execute_dag(self._execution_graph)

    def __enter__(self):
        print('created dag class')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print('finalising dag class')
        # for file in self.files:
        #    os.unlink(file)

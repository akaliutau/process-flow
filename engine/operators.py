from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict

from jinja2 import Environment

from keeper import keeper
from dcontext import DContext
from engine.constants import TaskStatus, TaskResult
from graph import DNode
from redux.store import Store


class Operator(DNode, ABC):

    def __init__(self, task_id: str, environment: Environment = None, store: Store = None, context: DContext = None):
        super().__init__(task_id)
        self.timestamp = None
        self.status = TaskStatus.PENDING
        self.result = TaskResult.NA
        self.error_msg = None
        self.allow_to_fail = False

        self._environment = environment
        self._store = store
        self._context = context
        dags = keeper.register(Operator.__name__, self)

    @property
    def full_task_id(self) -> str:
        return self._context.get('run_id') + '_' + self._task_id

    @property
    def store(self) -> Store:
        return self._store

    @property
    def environment(self) -> Environment:
        return self._environment

    @property
    def context(self) -> DContext:
        return self._context

    @environment.setter
    def environment(self, value):
        self._environment = value

    @store.setter
    def store(self, value):
        self._store = value

    @context.setter
    def context(self, value):
        self._context = value

    def complete(self) -> bool:
        return self.status in [TaskStatus.CANCELLED, TaskStatus.FINISHED]

    def get_operation_state(self) -> Dict[str, any]:
        return {
            'run_id': self._context.get('run_id'),
            'task_id': self._task_id,
            'timestamp': self.timestamp,
            'status': self.status.name,
            'result': self.result.name,
            'error_msg': self.error_msg
        }

    def set_status(self, status: TaskStatus):
        self.timestamp = datetime.now().isoformat('T', 'milliseconds')
        self.status = status
        if status in [TaskStatus.RUNNING, TaskStatus.CANCELLED, TaskStatus.PENDING]:
            self.result = TaskResult.NA
            self.error_msg = None
        if self.store:
            self.store.dispatch(self.get_operation_state())

    @abstractmethod
    async def exec(self):
        """ This async method is responsible for:
          1. Updating state for underlying Task (inserting starting and finishing timestamps, updating status, etc)
          2. Handling exceptions occurred during execution
          :param context: runtime configuration for the whole DAG
          :param state_holder: the class implementing the actual logic responsible for persisting state
         """
        pass

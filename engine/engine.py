import traceback
from datetime import datetime
from typing import Dict, List

from engine.constants import TaskStatus, TaskResult
from engine.utils import log
from state.state_holder import StateHolder


class Executable:

    def exec(self):
        pass


class Task:

    def __init__(self, task_id: str):
        self.task_id = task_id
        self.timestamp = None
        self.order_num = 0
        self.status = TaskStatus.PENDING
        self.result = TaskResult.NA
        self.error_msg = None

    def as_json(self) -> Dict[str, any]:
        return {
            'task_id': self.task_id,
            'timestamp': self.timestamp,
            'order': self.order_num,
            'status': self.status.name,
            'result': self.result.name,
            'error_msg': self.error_msg
        }

    def exec_task(self, state: StateHolder):
        """ This method is responsible for:
          1. Updating state for underlying Task (inserting starting and finishing timestamps, updating status, etc)
          2. Handling exceptions occurred during execution
          :param state: the class implementing the actual logic responsible for persisting state
        """
        pass


class PythonTask(Task):
    """A wrapper class to handle execution of python code
    This class is responsible for:
    1. Updating state for underlying Task (starting and finishing timestamps)
    2.
    """

    def __init__(self, task_id: str, python_class: type, **kwargs):
        super().__init__(task_id)
        self.kwargs = kwargs
        self.python_class = python_class

    def exec_task(self, state_holder: StateHolder):

        state_holder.pull_state(task_id=self.task_id)
        state_holder.update_state(task_id=self.task_id, state=self.kwargs)
        prev_counter = state_holder.pull_value(namespace=self.task_id, key='counter') or 0
        state_holder.update_state(task_id=self.task_id, state={'counter': prev_counter + 1})

        self.timestamp = datetime.now().isoformat('T', 'milliseconds')
        self.status = TaskStatus.RUNNING
        self.result = TaskResult.NA
        self.error_msg = None
        state_holder.push_state(task=self.as_json())

        instance = self.python_class(self.task_id, state_holder)
        try:
            instance.exec()
            self.result = TaskResult.SUCCESS
        except Exception as e:
            print(traceback.format_exc())
            self.result = TaskResult.ERROR
            self.error_msg = str(e)
            log.error(e)
        finally:
            self.timestamp = datetime.now().isoformat('T', 'milliseconds')
            self.status = TaskStatus.FINISHED

        state_holder.push_state(task=self.as_json())


class Sequence:
    """The top-level class to manage sequence execution.
    Contains an execution hook to correctly handle process interruption
    """

    def __init__(self, seq_id: str, run_id: str, state_holder: StateHolder, config_params: dict):
        self.state_holder = state_holder
        self.state_holder.global_state.update({'seq_id': seq_id})
        self.state_holder.global_state.update({'run_id': run_id})
        self.config_params = config_params
        self.tasks: List[Task] = list()

    def add_task(self, op: Task) -> None:
        self.tasks.append(op)

    def update(self):
        self.state_holder.pull_tasks()

    def exec_all(self):
        order = 1
        for task in self.tasks:
            task.order_num = order
            order += 1
        for task in self.tasks:
            task.exec_task(self.state_holder)

from typing import Callable, Dict

from engine.operators import Operator


class PythonOperator(Operator):

    def __init__(self, task_id: str, python_callable: Callable, allow_to_fail: bool = False):
        super().__init__(task_id)
        self._python_callable = python_callable
        self.allow_to_fail = allow_to_fail

    async def exec(self):
        self._python_callable(self.store, self.context)


class BigQueryJobOperator:
    pass


class BigQueryInsertJobOperator(Operator):

    def __init__(self, task_id: str, project_id: str, configuration: Dict, allow_to_fail: bool = False):
        super().__init__(task_id)
        self.project_id = project_id
        self.configuration = configuration
        self.allow_to_fail = allow_to_fail

    async def exec(self):
        pass

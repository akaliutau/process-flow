from typing import Callable


class ExecutionService:

    def __init__(self, context: dict):
        self.queue = list()
        self.context = context

    def add_task(self, fn: Callable) -> None:
        self.queue.append(fn)


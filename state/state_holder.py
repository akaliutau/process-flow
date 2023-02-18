from typing import Dict, List

from engine.exceptions import InvalidStateException


class StateHolder:
    """An abstract base class for all state holders. Designed to provide access to persistent state
      The implementation should be ACID - complaint by design.
      Subclass this and override 3 methods to implement a custom persisting mechanism.
    """

    def __init__(self):
        """
        Global state is an in-memory cache for such variables as:
        run_id
        seq_id
        """
        self._global_state = dict()
        self._instance_state = dict()

    @property
    def global_state(self) -> Dict[str, any]:
        return self._global_state

    @property
    def instance_state(self) -> Dict[str, dict]:
        return self._instance_state

    def pull_value(self, task_id: str, key: str) -> any:
        """
         :param task_id: could be either one of the reserved namespaces [runtime] or equal to task_id
         :param key: the key of value to pull from
         :return:
        """
        return self.instance_state[task_id].get(key)

    def update_state(self, task_id: str, state: dict):
        if task_id not in self.instance_state:
            self.instance_state[task_id] = dict()
        self.instance_state[task_id].update(state)
        self.instance_state[task_id].update({'synced': False})

    def pull_state(self, task_id: str) -> None:
        """generates a unique key from (seq_id,run_id,task_id), then searches and returns the latest state for this task
        """
        pass

    def push_state(self, task: dict) -> None:
        pass

    def pull_tasks(self) -> List[any]:
        pass


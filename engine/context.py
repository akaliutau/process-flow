from typing import Dict

from state.state_holder import StateHolder


class Context:
    """A proxy class to get access to both local and global context.
    Covers all operations with states
    """

    def __init__(self, task_id: str, initial_state: dict, state_holder: StateHolder):
        self.task_id = task_id
        self._task_state = initial_state or dict()
        self._state_holder = state_holder

    @property
    def task_state(self) -> Dict[str, any]:
        return self._task_state

    def refresh_state(self) -> None:
        self._task_state.update(self._state_holder.pull_state(self.task_id))

    def get_value(self, key: str, task_id: str = None, scope: str = 'task') -> any:
        """Returns the value of key in local or other task state

        :param scope: the scope of quering - [task, sequence]
        :param key: the key of key-value pair in state
        :param task_id: if omitted, the local state will be queried
        :return:
        """
        if scope == 'task':
            if task_id:
                return self._state_holder.pull_state(task_id).get(key)
            return self._task_state.get(key)
        return self._state_holder.global_state.get(key)

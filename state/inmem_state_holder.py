from typing import Dict, List

from engine.utils import log
from state.state_holder import StateHolder


class InMemoryStateHolder(StateHolder):
    """Is used only for unit-testing and as a PoC. Is not performance optimized

    """

    def __init__(self):
        super().__init__()
        self._records = dict()

    @property
    def records(self) -> Dict[str, any]:
        return self._records

    def pull_state(self, task_id: str):
        """generates a unique key from (seq_id,run_id,task_id), then searches and returns the latest state for this task
        """
        seq_id = self.global_state.get('seq_id')
        run_id = self.global_state.get('run_id')
        tgt_key = f'{seq_id}-{run_id}-{task_id}'
        found = list()
        for key, value in self.records.items():
            if key.startswith(tgt_key):
                found.append(value)
        found.sort(key=lambda k: k['timestamp'])
        final_state = found[-1]['state'] if found else dict()
        self.update_state(task_id, final_state)
        self.update_state(task_id, {'synced': True})

    #        log.info('current state of %s is %s', task_id, self.instance_state[task_id])

    def push_state(self, task: dict) -> None:
        seq_id = self.global_state.get('seq_id')
        run_id = self.global_state.get('run_id')
        task_id = task['task_id']
        timestamp = task['timestamp']
        status = task['status']
        counter = self.pull_value(task_id, 'counter')
        key = f'{seq_id}-{run_id}-{task_id}-{timestamp}-{status}-{counter}'
        cur_state = dict(self.instance_state[task_id])  # deep copy is needed
        cur_state.update({'synced': True})
        task['state'] = cur_state
        self._records[key] = task
        self.instance_state[task_id].update({'synced': True}) # TODO limit direct access to instance_state

    def pull_tasks(self) -> List[any]:
        print(self._records)
        seq_id = self.global_state.get('seq_id')
        run_id = self.global_state.get('run_id')
        tgt_key = f'{seq_id}-{run_id}'
        found = dict()
        for key, value in self.records.items():
            if key.startswith(tgt_key):
                task_id = value['task_id']
                if task_id not in found:
                    found[task_id] = list()
                found.get(task_id).append(value)
        ret = list()
        for task_id, tasks in found.items():
            tasks.sort(key=lambda k: k['timestamp'])  # TODO add sorting on the 2nd key (enum) if timestamps are equal
            ret.append(tasks[-1])
        ret.sort(key=lambda k: k['order'])
        return ret

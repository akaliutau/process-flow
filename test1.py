import uuid
from datetime import datetime

from engine.constants import TaskStatus
from state.inmem_state_holder import InMemoryStateHolder

if __name__ == '__main__':
    print('starting test 1')
    state_holder = InMemoryStateHolder()
    seq_id = 'test_seq'
    run_id = str(uuid.uuid1())
    state_holder.global_state.update({'seq_id': seq_id, 'run_id': run_id})
    task_1 = {
        'seq_id': seq_id,
        'run_id': run_id,
        'task_id': 'task_1',
        'order': 1,
        'timestamp': datetime.now().isoformat('T', 'milliseconds'),
        'status': TaskStatus.PENDING.name,
        'result': None,
        'error_message': None
    }
    state_holder.update_state(task_id='task_1', state={'var_1': 123, 'var_2': 'text value'})
    state_holder.push_state(task=task_1)

    task_2 = {
        'seq_id': seq_id,
        'run_id': run_id,
        'task_id': 'task_2',
        'order': 2,
        'timestamp': datetime.now().isoformat('T', 'milliseconds'),
        'status': TaskStatus.PENDING.name,
        'result': None,
        'error_message': None
    }
    state_holder.update_state(task_id='task_2', state={'var_1': 777, 'var_2': 'long text value'})
    state_holder.push_state(task=task_2)

    task_3 = {
        'seq_id': seq_id,
        'run_id': run_id,
        'task_id': 'task_1',
        'order': 1,
        'timestamp': datetime.now().isoformat('T', 'milliseconds'),
        'status': TaskStatus.RUNNING.name,
        'result': None,
        'error_message': None
    }
    state_holder.update_state(task_id='task_3', state={'var_1': 333})
    state_holder.push_state(task=task_3)


    print(state_holder.pull_tasks())
    print(state_holder.pull_state(task_id='task_1'))


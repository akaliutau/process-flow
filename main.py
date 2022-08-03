import uuid

from engine.context import Context
from engine.engine import Executable, Sequence, PythonTask
from engine.utils import log
from state.inmem_state_holder import InMemoryStateHolder
from state.state_holder import StateHolder


class TestA(Executable):

    def __init__(self, task_id: str, context: Context):
        pass

    def exec(self):
        log.info('Test1 is running!')


class TestB(Executable):

    def __init__(self, task_id: str, state_holder: StateHolder):
        self.state_holder = state_holder
        self.task_id = task_id

    def exec(self):
        log.info('Test2 is running! ta_id=%s and context=%s',
                 self.state_holder.pull_value(namespace=self.task_id,key='ta_id'),
                 self.state_holder.global_state)
        #raise Exception('an error')


if __name__ == '__main__':
    print('starting')
    op1 = PythonTask(task_id="task1", python_class=TestA)
    op2 = PythonTask(task_id="task2", python_class=TestB, ta_id='123', extra='ex1')
    params = {'ta_id': '123', 'extra': 'ex1'}
    op3 = PythonTask(task_id="task3", python_class=TestB, **params) # example of restoring of state
    run_id = str(uuid.uuid1())

    seq = Sequence(
        seq_id = 'simple seq',
        run_id = run_id,
        state_holder=InMemoryStateHolder(),
        config_params={}
    )
    seq.add_task(op1)
    seq.add_task(op2)
    seq.add_task(op3)
    seq.exec_all()
    seq.exec_all()
    print(seq.state_holder.pull_tasks())

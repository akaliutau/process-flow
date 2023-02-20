import datetime
import unittest
from typing import Dict

from redux.store import create_store


def simple_state_reducer(state: Dict, action: Dict) -> Dict:
    state = state or {}
    if not action:
        return state
    if action['type'] == 'TASK_STARTED':
        state['status'] = 'STARTED'
        state['timestamp'] = datetime.datetime.now().strftime('%Y-%d-%mT%H:%m:%S.%f')
    return state


class TestingRedux(unittest.TestCase):

    def test_base_functions(self):
        store = create_store(reducer=simple_state_reducer)
        store.subscribe(lambda s: print(s))
        state = store.get_state()
        print(state)
        store.dispatch({'type': 'TASK_STARTED'})
        state = store.get_state()
        print('current state: %s' % state)
        store.dispatch({'type': 'TASK_FINISHED', 'result': 'success'})

    def test_wrong_args(self):
        store = create_store(reducer=simple_state_reducer)
        store.subscribe(lambda s: print(s))
        state = store.get_state()
        self.assertRaises(ValueError, lambda: store.dispatch({'no_type': 'TASK_STARTED'}))


if __name__ == '__main__':
    unittest.main()

from typing import Callable, Dict, Any


class ActionTypes(object):
    INIT = '@@redux/INIT'


class Store(dict):
    def get_state(self):
        return self['get_state']()

    def subscribe(self, listener):
        return self['subscribe'](listener)

    def dispatch(self, action):
        return self['dispatch'](action)


def create_store(reducer: Callable, initial_state: Any = None) -> Store:

    current_reducer = [reducer]
    current_state = [initial_state]
    current_listeners = [[]]
    next_listeners = [current_listeners[0]]
    is_dispatching_error = [False]
    is_subscribed = [False]

    def next_listener():
        if next_listeners[0] == current_listeners[0]:
            next_listeners[0] = current_listeners[0][:]

    def get_state() -> Any:
        return current_state[0]

    def subscribe(listener: Callable) -> None:
        is_subscribed[0] = True

        next_listener()
        next_listeners[0].append(listener)

    def unsubscribe(listener: Callable) -> None:
        if not is_subscribed[0]:
            return
        is_subscribed[0] = False

        next_listener()
        index = next_listeners[0].index(listener)
        next_listeners[0].pop(index)

    def dispatch(action: Dict) -> Dict:

        if 'type' not in action:
            raise ValueError('Actions MUST have a non-None "type" prop')

        if is_dispatching_error[0]:
            raise Exception('State consistency is lost.')

        try:
            is_dispatching_error[0] = True
            current_state[0] = current_reducer[0](current_state[0], action)
        finally:
            is_dispatching_error[0] = False # if not reset, then consistency is lost

        listeners = current_listeners[0] = next_listeners[0]
        for listener in listeners:
            listener(current_state[0])

        return action

    dispatch({'type': ActionTypes.INIT})

    return Store(
        dispatch=dispatch,
        subscribe=subscribe,
        get_state=get_state,
    )

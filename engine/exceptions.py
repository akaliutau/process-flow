class InvalidStateException(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class InvalidArgumentException(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class ExecutionException(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class ValidationException(Exception):
    def __init__(self, message: str, errors):
        super().__init__(message)
        self.errors = errors

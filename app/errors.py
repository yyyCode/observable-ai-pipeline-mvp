class PipelineError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class ConfigError(PipelineError):
    pass


class RetryableError(PipelineError):
    pass


class FatalError(PipelineError):
    pass


class CanceledError(PipelineError):
    pass


class TransitionError(PipelineError):
    pass

from edbyteai_base_module.error_handler import EdbyteAIException


class ContentFileMismatchException(EdbyteAIException):
    def __init__(self, message_data):
        super(ContentFileMismatchException, self).__init__(message_data)


class InvalidContentPathException(EdbyteAIException):
    def __init__(self, message_data):
        super(InvalidContentPathException, self).__init__(message_data)


class InvalidFileTypeException(EdbyteAIException):
    def __init__(self, message_data):
        super(InvalidFileTypeException, self).__init__(message_data)


class MissingConfigurationException(EdbyteAIException):
    def __init__(self, message_data):
        super(MissingConfigurationException, self).__init__(message_data)


class PdfPageCountException(EdbyteAIException):
    def __init__(self, message_data):
        super(PdfPageCountException, self).__init__(message_data)


class SqsMessagePublishException(EdbyteAIException):
    def __init__(self, message_data):
        super(SqsMessagePublishException, self).__init__(message_data)


class UnknownSqsTriggerException(EdbyteAIException):
    def __init__(self, message_data):
        super(UnknownSqsTriggerException, self).__init__(message_data)

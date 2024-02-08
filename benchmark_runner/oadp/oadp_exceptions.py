
class OadpError(Exception):
    """ Base class for all benchmark operator error classes.
        All exceptions raised by the benchmark runner library should inherit from this class. """
    pass


class MissingResultReport(OadpError):
    """
    This class is error for missing oadp report result
    """
    def __init__(self):
        self.message = "Missing oadp result report"
        super(MissingResultReport, self).__init__(self.message)


class MissingElasticSearch(OadpError):
    """
    This class is error for missing ElasticSearch details
    """
    def __init__(self):
        self.message = "Missing ElasticSearch details"
        super(MissingElasticSearch, self).__init__(self.message)
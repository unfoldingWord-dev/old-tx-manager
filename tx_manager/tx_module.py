# TxModule class

from __future__ import print_function

from six import string_types
from six import iteritems


class TxModule(object):
    db_fields = [
        'name',
        'input_format',
        'options',
        'output_format',
        'private_links',
        'public_links',
        'resource_types',
        'type',
        'version'
    ]

    def __init__(self, data, quiet=False):
        # Init attributes
        self.name = None
        self.input_format = None
        self.options = []
        self.output_format = []
        self.private_links = []
        self.public_links = []
        self.resource_types = []
        self.type = None
        self.version = 1
        self.quiet = quiet
        self.log = []
        self.errors = []
        self.warnings = []

        if isinstance(data, dict):
            self.populate(data)
        elif isinstance(data, string_types):
            self.job_id = data

        if not self.job_id or not isinstance(self.job_id, string_types):
            raise Exception('Must create a job with a job_id or data to populate it which includes job_id.')

    def populate(self, data):
        for key, value in iteritems(data):
            if not hasattr(self, key):
                raise Exception('Invalid field given: {0}'.format(key))
            setattr(self, key, value)

    def get_db_data(self):
        data = {}
        for field in self.db_fields:
            if hasattr(self, field):
                data[field] = getattr(self, field)
            else:
                data[field] = None
        return data

    def log_message(self, message):
        if not self.quiet:
            print(message)
        self.log.append(message)

    def error_message(self, message):
        if not self.quiet:
            print(message)
        self.errors.append(message)

    def warning_message(self, message):
        if not self.quiet:
            print(message)
        self.warnings.append(message)

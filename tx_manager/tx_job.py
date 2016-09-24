# TxJob class

from __future__ import print_function

from six import string_types
from six import iteritems


class TxJob(object):
    db_fields = [
        'job_id',
        'user',
        'convert_module',
        'created_at',
        'expires_at',
        'started_at',
        'ended_at',
        'eta',
        'input_format',
        'source',
        'output_format'
        'output',
        'output_expiration',
        'cdn_bucket',
        'cdn_file',
        'callback',
        'links',
        'job_status',
        'log',
        'warnings',
        'errors',
    ]

    def __init__(self, data, quiet=False):
        # Init attributes
        self.job_id = None
        self.user = None
        self.convert_module = None
        self.created_at = None
        self.expires_at = None
        self.started_at = None
        self.ended_at = None
        self.eta = None
        self.resource_type = None
        self.input_format = None
        self.source = None
        self.output_format = None
        self.output = None
        self.output_expiration = None
        self.cdn_bucket = None
        self.cdn_file = None
        self.callback = None
        self.links = []
        self.job_status = None
        self.log = []
        self.errors = []
        self.warnings = []
        self.quiet = quiet
        self.api_base_url = None
        self.cdn_base_url = None

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


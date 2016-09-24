# txManager functions

from __future__ import print_function

import hashlib
from datetime import datetime
from datetime import timedelta

import requests

from aws_tools.lambda_handler import LambdaHandler
from aws_tools.dynamodb_handler import DynamoDBHandler
from gogs_tools.gogs_handler import GogsHandler
from tx_job import TxJob
from tx_module import TxModule


class TxManager(object):
    JOB_TABLE_NAME = 'tx-job'
    MODULE_TABLE_NAME = 'tx-module'

    def __init__(self, **kwargs):
        # DEFAULTS
        self.api_url = None
        self.cdn_url = None
        self.cdn_bucket = None
        self.quiet = False
        self.job_db_handler = None
        self.module_db_handler = None
        self.gogs_url = None
        self.gogs_handler = None
        self.job_table_name = self.JOB_TABLE_NAME
        self.module_table_name = self.MODULE_TABLE_NAME
        self.aws_access_key_id = None
        self.aws_secret_access_key = None

        # Get any passed arguments into existing attributes
        for key, value in kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, value)

        # Post process of arguments
        if self.gogs_url:
            self.gogs_handler = GogsHandler(self.gogs_url)

        self.lambda_handler = LambdaHandler(self.aws_access_key_id, self.aws_secret_access_key)

    def debug_print(self, message):
        if not self.quiet:
            print(message)

    def get_user(self, user_token):
        return self.gogs_handler.get_user(user_token)

    def get_converter_module(self, job):
        modules = self.query_modules()
        for module in modules:
            if job.resource_type in module['resource_types']:
                if job.input_format in module['input_format']:
                    if job.output_format in module['output_format']:
                        return module
        return None

    def setup_job(self, data):
        if 'user_token' not in data:
            raise Exception('"user_token" not given.')

        user = self.get_user(data['user_token'])

        if not user:
            raise Exception('Invalid user_token. User not found.')

        data['user'] = user
        del data['user_token']

        job = TxJob(data, self.quiet)

        if not job.cdn_bucket:
            raise Exception('"cdn_bucket" not given.')
        if job.source:
            raise Exception('"source" url not given.')
        if not job.resource_type:
            raise Exception('"resource_type" not given.')
        if not job.input_format:
            raise Exception('"input_format" not given.')
        if not job.output_format:
            raise Exception('"output_format" not given.')

        module = self.get_converter_module(job)

        if not module:
            raise Exception('No converter was found to convert {0} from {1} to {2}'.format(job.resource_type, job.input_format, job.output_format))

        job.convert_module = module
        output_file = 'tx/job/{0}.zip'.format(job.job_id)  # All conversions must result in a ZIP of the converted file(s)
        job.output = '{0}/{1}'.format(self.cdn_base_url, output_file)

        created_at = datetime.utcnow()
        expires_at = created_at + timedelta(days=1)
        eta = created_at + timedelta(seconds=20)

        job.created_at = created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        job.expires_at = expires_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        job.eta = eta.strftime("%Y-%m-%dT%H:%M:%SZ")
        job.job_status = 'requested'

        job_id = hashlib.sha256('{0}-{1}-{2}'.format(data['user_token'], user, created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))).hexdigest()
        job.job_id = job_id

        job.links = {
            "href": "{0}/tx/job/{1}".format(self.api_url, job_id),
            "rel": "self",
            "method": "GET"
        }

        # Saving this to the DynamoDB will start trigger a DB stream which will call
        # tx-manager again with the job info (see run() function)
        self.insert_job(job)

        return {
            "job": job.get_db_data(),
            "links": [
                {
                    "href": "{0}/tx/job".format(self.api_url),
                    "rel": "list",
                    "method": "GET"
                },
                {
                    "href": "{0}/tx/job".format(self.api_url),
                    "rel": "create",
                    "method": "POST"
                },
            ],
        }

    def list_jobs(self, data, must_be_authenticated=True):
        if must_be_authenticated:
            if 'user_token' not in data:
                raise Exception('"user_token" not given.')
            user = self.get_user(data['user_token'])
            if not user:
                raise Exception('Invalid user_token. User not found.')
            data['user'] = user
            del data['user_token']
        return self.query_jobs(data)

    def list_endpoints(self):
        return {
            "version": "1",
            "links": [
                {
                    "href": "{0}/tx/job".format(self.api_url),
                    "rel": "list",
                    "method": "GET"
                },
                {
                    "href": "{0}/tx/job".format(self.api_url),
                    "rel": "create",
                    "method": "POST"
                },
            ]
        }

    def start_job(self, job_id):
        job = self.get_job(job_id)

        if not job:
            raise Exception('Job not found: {0}'.format(job_id))

        # Only start the job if the status is 'requested' and a started timestamp hasn't been set
        if job.job_status != 'requested' or job.start_at:
            return

        job.start_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        job.status = 'started'
        job.log_message('Started job {0} at {1}'.format(job_id, job.started_at))

        try:
            self.update_job(job)

            module = self.get_converter_module(job)
            if not module:
                raise Exception('No converter was found to convert {0} from {1} to {2}'.format(job.resourse_type, job.import_format, job.output_format))

            job.converter_module = module
            self.update_job(job)

            payload = {
                'data': {
                    'job': job,
                }
            }
            print("Payload to {0}:".format(module['name']))
            print(payload)

            job.log_message('Telling module {0} to convert {1} and put at {2}'.format(job.converter_module, job.source, job.output))
            response = self.aws_handler.lambda_invoke(module['name'], payload)
            print("Response payload from {0}:".format(module['name']))
            print(response)

            if 'errorMessage' in response:
                job.error_message(response['errorMessage'], module['name'])
            else:
                for message in response['log']:
                    job.log_message(message)
                for message in response['errors']:
                    job.error_message(message)
                for message in response['warnings']:
                    job.warning_message(message)

                if response['errors']:
                    job.log_message('{0} function returned with errors.'.format(module['name']))
                elif response['warnings']:
                    job.log_message('{0} function returned with warnings.'.format(module['name']))
                if response['errors']:
                    job.log_message('{0} function returned.'.format(module['name']))
        except Exception as e:
            job.error_message(e.message)

        job.ended_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        if len(job.errors):
            job.success = False
            job.job_status = "failed"
            message = "Conversion failed"
        elif len(job.warnings) > 0:
            job.success = True
            job.job_status = "warnings"
            message = "Conversion successful with warnings"
        else:
            job.success = True
            job.job_status = "success"
            message = "Conversion successful"

        job.log_message(message)
        job.log_message('Finished job {0} at {1}'.format(job.job_id, job.ended_at))

        self.update_job(job)

        callback_payload = {
            "job_id": job.job_id,
            "identifier": job.identifier,
            "success": job.success,
            "status": job.job_status,
            "message": message,
            "output": job.output,
            "output_expiration": job.output_expiration,
            "log": job.log,
            "warnings": job.warnings,
            "errors": job.errors,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "ended_at": job.ended_at
        }

        if job.callback:
            self.do_callback(job.callback, callback_payload)

    def do_callback(self, url, payload):
        if url.startswith('http'):
            headers = {"content-type": "application/json"}
            print('Making callback to {0} with payload:'.format(url))
            print(payload)
            response = requests.post(url, json=payload, headers=headers)
            print('finished.')

    def make_api_gateway_for_module(self, module):
        # lambda_func_name = module['name']
        # AWS_LAMBDA_API_ID = '7X97xCLPDE16Jep5Zv85N6zy28wcQfJz79E2H3ln'
        # # of 'tx-manager_api_key'
        # # or fkcr7r4dz9
        # # or 7X97xCLPDE16Jep5Zv85N6zy28wcQfJz79E2H3ln
        # AWS_REGION = 'us-west-2'
        #
        # api_client = boto3.client('apigateway')
        # aws_lambda = boto3.client('lambda')
        #
        # ## create resource
        # resource_resp = api_client.create_resource(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     parentId='foo', # resource id for the Base API path
        #     pathPart=lambda_func_name
        # )
        #
        # ## create POST method
        # put_method_resp = api_client.put_method(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     resourceId=resource_resp['id'],
        #     httpMethod="POST",
        #     authorizationType="NONE",
        #     apiKeyRequired=True,
        # )
        #
        # lambda_version = aws_lambda.meta.service_model.api_version
        #
        # uri_data = {
        #     "aws-region": AWS_REGION,
        #     "api-version": lambda_version,
        #     "aws-acct-id": "xyzABC",
        #     "lambda-function-name": lambda_func_name,
        # }
        #
        # uri = "arn:aws:apigateway:{aws-region}:lambda:path/{api-version}/functions/arn:aws:lambda:{aws-region}:{aws-acct-id}:function:{lambda-function-name}/invocations".format(**uri_data)
        #
        # ## create integration
        # integration_resp = api_client.put_integration(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     resourceId=resource_resp['id'],
        #     httpMethod="POST",
        #     type="AWS",
        #     integrationHttpMethod="POST",
        #     uri=uri,
        # )
        #
        # api_client.put_integration_response(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     resourceId=resource_resp['id'],
        #     httpMethod="POST",
        #     statusCode="200",
        #     selectionPattern=".*"
        # )
        #
        # ## create POST method response
        # api_client.put_method_response(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     resourceId=resource_resp['id'],
        #     httpMethod="POST",
        #     statusCode="200",
        # )
        #
        # uri_data['aws-api-id'] = AWS_LAMBDA_API_ID
        # source_arn = "arn:aws:execute-api:{aws-region}:{aws-acct-id}:{aws-api-id}/*/POST/{lambda-function-name}".format(**uri_data)
        #
        # aws_lambda.add_permission(
        #     FunctionName=lambda_func_name,
        #     StatementId=uuid.uuid4().hex,
        #     Action="lambda:InvokeFunction",
        #     Principal="apigateway.amazonaws.com",
        #     SourceArn=source_arn
        # )
        #
        # # state 'your stage name' was already created via API Gateway GUI
        # api_client.create_deployment(
        #     restApiId=AWS_LAMBDA_API_ID,
        #     stageName="your stage name",
        # )
        return

    def register_module(self, data):
        module = TxModule(data, self.quiet)

        if not module.name:
            raise Exception('"name" not given.')
        if not module.type:
            raise Exception('"type" not given.')
        if not module.input_format:
            raise Exception('"input_format" not given.')
        if not module.output_format:
            raise Exception('"output_format" not given.')
        if not module.resource_types:
            raise Exception('"resource_types" not given.')

        self.debug_print("module payload:")
        self.debug_print(module)

        self.insert_module(module)
        self.make_api_gateway_for_module(module)  # Todo: develop this function
        return module.get_db_data()

    def insert_job(self, job):
        job_data = job.get_db_data()
        self.job_db_handler.insert_item(job_data)

    def query_jobs(self, data=None):
        return self.job_db_handler.query_items(data)

    def get_job(self, job_id):
        return self.job_db_handler.get_item({'job_id':job_id})

    def update_job(self, job):
        return self.job_db_handler.update_item({'job_id':job.job_id}, job.get_db_data())

    def delete_job(self, job):
        return self.job_db_handler.delete_item({'job_id':job.job_id})

    def insert_module(self, module):
        module_data = module.get_db_data()
        self.module_db_handler.insert_item(module_data)

    def query_modules(self, data=None):
        return self.module_db_handler.query_items(data)

    def get_module(self, name):
        return self.module_db_handler.get_item({'name': name})

    def update_module(self, module):
        return self.module_db_handler.update_item({'name': module.name}, module.get_db_data())

    def delete_module(self, module):
        return self.module_db_handler.delete_item({'name': module.name})

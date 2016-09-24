from unittest import TestCase

from tx_manager.tx_manager import TxManager


class TestTxManager(TestCase):
    def test_get_endpoints(self):
        api_url = 'https://my-api.com'
        manager = TxManager(api_url=api_url)
        self.assertEqual({'version': '1', 'links': [{'href': api_url+'/tx/job', 'method': 'GET', 'rel': 'list'}, {'href': api_url+'/tx/job', 'method': 'POST', 'rel': 'create'}]}, manager.list_endpoints())

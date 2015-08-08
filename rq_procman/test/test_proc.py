import unittest
import mock

import process

class TestProcess(unittest.TestCase):


    def setUp(self):
        self.p = process.Process('echo 123')

    @mock.patch('process.RePrint')
    def test_process_output(self, mock_reprint):
        events = mock.Mock()
        self.p.run(events)
        rp = mock_reprint.return_value
        rp.msg.assert_called_with('123\n')

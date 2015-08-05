#!/usr/bin/python
# coding=utf-8
################################################################################

from mock import Mock
from mock import patch
from mock import call
import configobj

from test import unittest
import diamond.handler.kairosdb as kairos
from diamond.metric import Metric


def fake_connection(self):
    '''
    Fake _connect method from kairosdb.
    '''

    self.socket = Mock()


class TestKairosdbHandler(unittest.TestCase):

    def setUp(self):
        self.__connect_method = kairos.KairosDBHandler
        kairos.KairosDBHandler._connect = fake_connection

    def tearDown(self):
        kairos.KairosDBHandler._connect = self.__connect_method

    def test_single_metric(self):
        config = configobj.ConfigObj()

        tags = {'host': 'test'}

        metric = Metric('server.example.cpu.total.idle',
                        0, timestamp=1234567, tags=tags)

        expected_data = call("put cpu.total.idle 1234567 0 host=test \n")

        handler = kairos.KairosDBHandler(config)

        patch_sock = patch.object(handler, 'socket', True)
        sendmock = Mock()
        patch_send = patch.object(handler, '_send', sendmock)

        patch_sock.start()
        patch_send.start()
        handler.process(metric)
        patch_send.stop()
        patch_sock.stop()
        self.assertEqual(sendmock.call_count, 1)
        self.assertEqual(sendmock.call_args_list[0], expected_data)


if __name__ == "__main__":
    unittest.main()
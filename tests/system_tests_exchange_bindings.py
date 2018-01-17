#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import ast
import unittest2 as unittest
from threading import Thread
from time import sleep
from subprocess import PIPE, STDOUT

try:
    import Queue as Queue   # 2.7
except ImportError:
    import queue as Queue   # 3.x

from system_test import TestCase, Qdrouterd, main_module, TIMEOUT, Process
from proton import Message, Timeout
from proton.reactor import AtMostOnce, AtLeastOnce
from proton.utils import BlockingConnection, SendException

#TIMEOUT=5
_EXCHANGE_TYPE = "org.apache.qpid.dispatch.router.config.exchange"
_BINDING_TYPE  = "org.apache.qpid.dispatch.router.config.binding"


class _AsyncReceiver(object):
    def __init__(self, address, source, credit=100, timeout=0.1,
                 conn_args=None, link_args=None):
        super(_AsyncReceiver, self).__init__()
        kwargs = {'url': address}
        if conn_args:
            kwargs.update(conn_args)
        self.conn = BlockingConnection(**kwargs)
        kwargs = {'address': source,
                  'credit': credit}
        if link_args:
            kwargs.update(link_args)
        self.rcvr = self.conn.create_receiver(**kwargs)
        self.thread = Thread(target=self._poll)
        self.queue = Queue.Queue()
        self._run = True
        self._timeout = timeout
        self.thread.start()

    def _poll(self):
        while self._run:
            try:
                msg = self.rcvr.receive(timeout=self._timeout)
            except Timeout:
                continue
            try:
                self.rcvr.accept()
            except IndexError:
                # PROTON-1743
                pass
            self.queue.put(msg)
        self.rcvr.close()
        self.conn.close()

    def stop(self):
        self._run = False
        self.thread.join(timeout=TIMEOUT)


class ExchangeBindingsTest(TestCase):
    """
    Tests the exchange/bindings of the dispatch router.
    """
    def _create_router(self, name, config):

        config = [
            ('router',   {'mode': 'standalone', 'id': 'QDR.%s'%name}),
            ('listener', {'role': 'normal', 'host': '0.0.0.0',
                          'port': self.tester.get_port(),
                          'saslMechanisms':'ANONYMOUS'})
            ] + config
        return self.tester.qdrouterd(name, Qdrouterd.Config(config))

    def run_qdmanage(self, router, cmd, input=None, expect=Process.EXIT_OK):
        p = self.popen(
            ['qdmanage'] + cmd.split(' ')
            + ['--bus', router.addresses[0], '--indent=-1', '--timeout', str(TIMEOUT)],
            stdin=PIPE, stdout=PIPE, stderr=STDOUT, expect=expect)
        out = p.communicate(input)[0]
        try:
            p.teardown()
        except Exception, e:
            raise Exception("%s\n%s" % (e, out))
        return out

    def _validate_entity(self, name, kind, entities, expected):
        for entity in entities:
            if "name" in entity and entity["name"] == name:
                for k,v in expected.items():
                    self.assertTrue(k in entity)
                    self.assertEqual(v, entity[k])
                return
        raise Exception("Could not find %s named %s" % (kind, name))

    def _validate_exchange(self, router, name, **kwargs):
        _ = self.run_qdmanage(router, "query --type %s" % _EXCHANGE_TYPE)
        self._validate_entity(name, "exchange", ast.literal_eval(_), kwargs)

    def _validate_binding(self, router, name, **kwargs):
        _ = self.run_qdmanage(router, "query --type %s" % _BINDING_TYPE)
        self._validate_entity(name, "binding", ast.literal_eval(_), kwargs)

    def test_qdmanage(self):
        """
        Tests the management API via qdmanage
        """
        router = self._create_router("A", [])

        # create exchanges
        ex_config = [
            ["Exchange1", {"address": "Address1"}],
            ["Exchange2", {"address": "Address2",
                           "phase": 2,
                           "alternate": "Alternate2",
                           "alternatePhase": 1,
                           "matchMethod": "mqtt"}]
        ]

        for cfg in ex_config:
            args = ""
            for k, v in cfg[1].items():
                args += "%s=%s " % (k, v)
            self.run_qdmanage(router,
                              "create --type %s --name %s %s" %
                              (_EXCHANGE_TYPE, cfg[0], args))

        # validate
        _ = self.run_qdmanage(router, "query --type %s" % _EXCHANGE_TYPE)
        query = ast.literal_eval(_)
        self.assertEqual(len(ex_config), len(query))
        for cfg in ex_config:
            self._validate_entity(name=cfg[0],
                                  kind="exchange",
                                  entities=query,
                                  expected=cfg[1])
        for ex in query:
            self.assertEqual(0, ex['bindingCount'])

        # create bindings
        binding_config = [
            ["b11", {"exchange": "Exchange1",
                     "key":      "a.b.*.#",
                     "nextHop":  "nextHop1",
                     "phase":    3}],
            ["b12", {"exchange": "Exchange1",
                     "key":      "a.*.c.#",
                     "nextHop":  "nextHop1",
                     "phase":    3}],
            ["b13", {"exchange": "Exchange1",
                     "key":      "a.b.*.#",
                     "nextHop":  "nextHop2",
                     "phase":    0}],
            ["b14", {"exchange": "Exchange1",
                     "key":      "a.*.c.#",
                     "nextHop":  "nextHop2",
                     "phase":    0}],

            ["b21", {"exchange": "Exchange2",
                     "key":      "a/b/?/#",
                     "nextHop":  "nextHop3"}],
            ["b22", {"exchange": "Exchange2",
                     "key":      "a",
                     "nextHop":  "nextHop4"}],
            ["b23", {"exchange": "Exchange2",
                     "key":      "a/b",
                     "nextHop":  "nextHop4"}],
            ["b24", {"exchange": "Exchange2",
                     "key":      "b",
                     "nextHop":  "nextHop3"}]
        ]

        for cfg in binding_config:
            args = ""
            for k, v in cfg[1].items():
                args += "%s=%s " % (k, v)
            self.run_qdmanage(router,
                              "create --type %s --name %s %s" %
                              (_BINDING_TYPE, cfg[0], args))

        # validate
        _ = self.run_qdmanage(router, "query --type %s" % _BINDING_TYPE)
        bindings = ast.literal_eval(_)
        self.assertEqual(len(binding_config), len(bindings))
        for cfg in binding_config:
            self._validate_entity(name=cfg[0],
                                  kind="binding",
                                  entities=bindings,
                                  expected=cfg[1])

        _ = self.run_qdmanage(router, "query --type %s" % _EXCHANGE_TYPE)
        exchanges = ast.literal_eval(_)
        self.assertEqual(len(ex_config), len(exchanges))
        for ex in exchanges:
            self.assertEqual(4, ex["bindingCount"])

        # verify reads
        _ = self.run_qdmanage(router, "read --type %s --name Exchange2" % _EXCHANGE_TYPE)
        self.assertEqual("Exchange2", ast.literal_eval(_)["name"])
        _ = self.run_qdmanage(router, "read --type %s --name b24" % _BINDING_TYPE)
        self.assertEqual("b24", ast.literal_eval(_)["name"])

        # binding deletion by id:
        bid = bindings[0]["identity"]
        self.run_qdmanage(router, "delete --type " + _BINDING_TYPE +
                              " --identity %s" % bid)
        _ = self.run_qdmanage(router, "query --type %s" % _BINDING_TYPE)
        bindings = ast.literal_eval(_)
        self.assertEqual(len(binding_config) - 1, len(bindings))
        for binding in bindings:
            self.assertFalse(binding["identity"] == bid)

        # binding deletion by name:
        self.run_qdmanage(router, "delete --type " + _BINDING_TYPE +
                              " --name b14")
        _ = self.run_qdmanage(router, "query --type %s" % _BINDING_TYPE)
        bindings = ast.literal_eval(_)
        self.assertEqual(len(binding_config) - 2, len(bindings))
        for binding in bindings:
            self.assertFalse(binding["name"] == "b14")

        # exchange deletion by name:
        self.run_qdmanage(router, "delete --type " + _EXCHANGE_TYPE +
                              " --name Exchange1")
        _ = self.run_qdmanage(router, "query --type %s" % _EXCHANGE_TYPE)
        exchanges = ast.literal_eval(_)
        self.assertEqual(len(ex_config) - 1, len(exchanges))
        self.assertEqual("Exchange2", exchanges[0]["name"])

        # negative testing

        # exchange name is required
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _EXCHANGE_TYPE +
                          " address=Nope")
        # exchange address is required
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _EXCHANGE_TYPE +
                          " --name Nope")
        # duplicate exchange names
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _EXCHANGE_TYPE +
                          " --name Exchange2 address=foo")
        # invalid match method
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _EXCHANGE_TYPE +
                          " --name Exchange3 address=foo"
                          " matchMethod=blinky")
        # duplicate exchange addresses
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _EXCHANGE_TYPE +
                          " --name Nope address=Address2")
        # binding with no exchange name
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _BINDING_TYPE +
                          " --name Nope")
        # binding with bad exchange name
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _BINDING_TYPE +
                          " exchange=Nope")
        # binding with duplicate name
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _BINDING_TYPE +
                          " --name b22 exchange=Exchange2"
                          " key=b nextHop=nextHop3")
        # binding with duplicate pattern & next hop
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _BINDING_TYPE +
                          " --name Nuhuh exchange=Exchange2"
                          " key=b nextHop=nextHop3")
        # binding with no next hop
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _BINDING_TYPE +
                          " --name Nuhuh exchange=Exchange2"
                          " key=x/y/z")

        # invalid mqtt key
        self.assertRaises(Exception, self.run_qdmanage, router,
                          "create --type " + _BINDING_TYPE +
                          " exchange=Exchange2"
                          " key=x/#/z"
                          " nextHop=Nope")

        # delete exchange by identity:
        self.run_qdmanage(router, "delete --type " + _EXCHANGE_TYPE +
                              " --identity %s" % exchanges[0]["identity"])

    def test_forwarding(self):
        """
        Simple forwarding over a single 0-10 exchange
        """
        config = [
            ('exchange', {'address': 'Address1',
                          'name': 'Exchange1'}),
            # two different patterns, same next hop:
            ('binding', {'name': 'binding1',
                         'exchange': 'Exchange1',
                         'key': 'a.*',
                         'nextHop': 'nextHop1'}),
            ('binding', {'name': 'binding2',
                         'exchange': 'Exchange1',
                         'key': 'a.b',
                         'nextHop': 'nextHop1'}),
            # duplicate patterns, different next hops:
            ('binding', {'name': 'binding3',
                         'exchange': 'Exchange1',
                         'key': 'a.c.#',
                         'nextHop': 'nextHop1'}),
            ('binding', {'name': 'binding4',
                         'exchange': 'Exchange1',
                         'key': 'a.c.#',
                         'nextHop': 'nextHop2'}),
            # match for nextHop2 only
            ('binding', {'name': 'binding5',
                         'exchange': 'Exchange1',
                         'key': 'a.b.c',
                         'nextHop': 'nextHop2'})
        ]
        router = self._create_router('A', config)

        # create clients for message transfer
        conn = BlockingConnection(router.addresses[0])
        sender = conn.create_sender(address="Address1", options=AtMostOnce())
        nhop1 = conn.create_receiver(address="nextHop1", credit=100)
        nhop2 = conn.create_receiver(address="nextHop2", credit=100)

        # verify initial metrics
        self._validate_exchange(router, name='Exchange1',
                                bindingCount=5,
                                deliveriesReceived=0,
                                deliveriesDropped=0,
                                deliveriesForwarded=0,
                                deliveriesAlternate=0)

        for b in range(5):
            self._validate_binding(router,
                                   name='binding%s' % (b + 1),
                                   deliveriesMatched=0)

        # send message with subject "a.b"
        # matches (binding1, binding2)
        # forwarded to NextHop1 only
        sender.send(Message(subject='a.b', body='A'))
        self.assertEqual('A', nhop1.receive(timeout=TIMEOUT).body)

        # send message with subject "a.c"
        # matches (bindings 1,3,4)
        # ->  NextHop1, NextHop2
        sender.send(Message(subject='a.c', body='B'))
        self.assertEqual('B', nhop1.receive(timeout=TIMEOUT).body)
        self.assertEqual('B', nhop2.receive(timeout=TIMEOUT).body)

        # send message with subject "a.c.d"
        # matches bindings 3,4
        # -> NextHop1, NextHop2
        sender.send(Message(subject='a.c.d', body='C'))
        self.assertEqual('C', nhop1.receive(timeout=TIMEOUT).body)
        self.assertEqual('C', nhop2.receive(timeout=TIMEOUT).body)

        # send message with subject "x.y.z"
        # no binding match - expected to drop
        # not forwarded
        sender.send(Message(subject='x.y.z', body=["I am Noone"]))

        # send message with subject "a.b.c"
        # matches binding5
        # -> NextHop2
        sender.send(Message(subject='a.b.c', body='D'))
        self.assertEqual('D', nhop2.receive(timeout=TIMEOUT).body)

        # ensure there are no more messages on either hop:

        self.assertRaises(Timeout, nhop1.receive, timeout=0.25)
        self.assertRaises(Timeout, nhop2.receive, timeout=0.25)

        # validate counters
        self._validate_binding(router, name='binding1',
                               deliveriesMatched=2)
        self._validate_binding(router, name='binding2',
                               deliveriesMatched=1)
        self._validate_binding(router, name='binding3',
                               deliveriesMatched=2)
        self._validate_binding(router, name='binding4',
                               deliveriesMatched=2)
        self._validate_binding(router, name='binding5',
                               deliveriesMatched=1)
        self._validate_exchange(router, name="Exchange1",
                                deliveriesReceived=5,
                                deliveriesForwarded=4,
                                deliveriesAlternate=0,
                                deliveriesDropped=1)
        conn.close()

    def test_forwarding_mqtt(self):
        """
        Simple forwarding over a single mqtt exchange
        """
        config = [
            ('exchange', {'address': 'Address2',
                          'name': 'Exchange1',
                          'matchMethod': 'mqtt',
                          'alternate': 'altNextHop'}),

            ('binding', {'name': 'binding1',
                         'exchange': 'Exchange1',
                         'key': 'a/b',
                         'nextHop': 'nextHop1'}),
            ('binding', {'name': 'binding2',
                         'exchange': 'Exchange1',
                         'key': 'a/+',
                         'nextHop': 'nextHop2'}),
            ('binding', {'name': 'binding3',
                         'exchange': 'Exchange1',
                         'key': 'c/#',
                         'nextHop': 'nextHop1'}),
            ('binding', {'name': 'binding4',
                         'exchange': 'Exchange1',
                         'key': 'c/b',
                         'nextHop': 'nextHop2'}),
        ]
        router = self._create_router('B', config)

        # create clients for message transfer
        conn = BlockingConnection(router.addresses[0])
        sender = conn.create_sender(address="Address2", options=AtMostOnce())
        nhop1 = conn.create_receiver(address="nextHop1", credit=100)
        nhop2 = conn.create_receiver(address="nextHop2", credit=100)
        alt = conn.create_receiver(address="altNextHop", credit=100)

        # send message with subject "a.b"
        # matches (binding1, binding2)
        # forwarded to NextHop1, NextHop2
        sender.send(Message(subject='a/b', body='A'))
        self.assertEqual('A', nhop1.receive(timeout=TIMEOUT).body)
        self.assertEqual('A', nhop2.receive(timeout=TIMEOUT).body)

        # send message with subject "a/c"
        # matches binding2
        # ->  NextHop2
        sender.send(Message(subject='a/c', body='B'))
        self.assertEqual('B', nhop2.receive(timeout=TIMEOUT).body)

        # send message with subject "c/b"
        # matches bindings 3,4
        # -> NextHop1, NextHop2
        sender.send(Message(subject='c/b', body='C'))
        self.assertEqual('C', nhop1.receive(timeout=TIMEOUT).body)
        self.assertEqual('C', nhop2.receive(timeout=TIMEOUT).body)

        # send message with subject "c/b/dee/eee"
        # matches binding3
        # -> NextHop1
        sender.send(Message(subject='c/b/dee/eee', body='D'))
        self.assertEqual('D', nhop1.receive(timeout=TIMEOUT).body)

        # send message with subject "x.y.z"
        # no binding match
        # -> alternate
        sender.send(Message(subject='x.y.z', body="?"))
        self.assertEqual('?', alt.receive(timeout=TIMEOUT).body)

        # ensure there are no more messages on either hop:

        self.assertRaises(Timeout, nhop1.receive, timeout=0.25)
        self.assertRaises(Timeout, nhop2.receive, timeout=0.25)
        self.assertRaises(Timeout, alt.receive, timeout=0.25)

        # validate counters
        self._validate_binding(router, name='binding1',
                               deliveriesMatched=1)
        self._validate_binding(router, name='binding2',
                               deliveriesMatched=2)
        self._validate_binding(router, name='binding3',
                               deliveriesMatched=2)
        self._validate_binding(router, name='binding4',
                               deliveriesMatched=1)
        self._validate_exchange(router, name="Exchange1",
                                deliveriesReceived=5,
                                deliveriesForwarded=5,
                                deliveriesAlternate=1,
                                deliveriesDropped=0)
        conn.close()

    def test_forwarding_sync(self):
        """
        Forward unsettled messages to multiple subscribers
        """
        config = [
            ('router',   {'mode': 'standalone', 'id': 'QDR.mcast',
                          'allowUnsettledMulticast': True}),
            ('listener', {'role': 'normal', 'host': '0.0.0.0',
                          'port': self.tester.get_port(),
                          'saslMechanisms':'ANONYMOUS'}),
            ('address', {'pattern': 'nextHop2/#', 'distribution': 'multicast'}),
            ('exchange', {'address': 'Address3',
                          'name': 'Exchange1',
                          'alternate': 'altNextHop'}),
            ('binding', {'name': 'binding1',
                         'exchange': 'Exchange1',
                         'key': 'a.b',
                         'nextHop': 'nextHop1'}),
            ('binding', {'name': 'binding2',
                         'exchange': 'Exchange1',
                         'key': '*.b',
                         'nextHop': 'nextHop2'})
        ]
        router = self.tester.qdrouterd('QDR.mcast', Qdrouterd.Config(config))

        # create clients for message transfer
        conn = BlockingConnection(router.addresses[0])
        sender = conn.create_sender(address="Address3", options=AtLeastOnce())
        nhop1 = _AsyncReceiver(address=router.addresses[0], source="nextHop1")
        nhop2A = _AsyncReceiver(address=router.addresses[0], source="nextHop2")
        nhop2B = _AsyncReceiver(address=router.addresses[0], source="nextHop2")
        alt = _AsyncReceiver(address=router.addresses[0], source="altNextHop")

        sender.send(Message(subject='a.b', body='A'))
        sender.send(Message(subject='x.y', body='B'))

        self.assertEqual('A', nhop1.queue.get(timeout=TIMEOUT).body)
        self.assertEqual('A', nhop2A.queue.get(timeout=TIMEOUT).body)
        self.assertEqual('A', nhop2B.queue.get(timeout=TIMEOUT).body)
        self.assertEqual('B', alt.queue.get(timeout=TIMEOUT).body)
        nhop1.stop()
        nhop2A.stop()
        nhop2B.stop()
        alt.stop()
        conn.close()

        self.assertTrue(nhop1.queue.empty())
        self.assertTrue(nhop2A.queue.empty())
        self.assertTrue(nhop2B.queue.empty())
        self.assertTrue(alt.queue.empty())

        # ensure failure if unsettled multicast not allowed:

        config = [
            ('router',   {'mode': 'standalone', 'id': 'QDR.mcast2',
                          'allowUnsettledMulticast': False}),
            ('listener', {'role': 'normal', 'host': '0.0.0.0',
                          'port': self.tester.get_port(),
                          'saslMechanisms':'ANONYMOUS'}),
            ('exchange', {'address': 'Address4',
                          'name': 'Exchange1'}),
            ('binding', {'name': 'binding1',
                         'exchange': 'Exchange1',
                         'key': 'a.b',
                         'nextHop': 'nextHop1'})
        ]
        router = self.tester.qdrouterd('QDR.mcast2', Qdrouterd.Config(config))

        # create clients for message transfer
        conn = BlockingConnection(router.addresses[0])
        sender = conn.create_sender(address="Address4", options=AtLeastOnce())
        nhop1 = _AsyncReceiver(address=router.addresses[0], source="nextHop1")

        self.assertRaises(SendException,
                          sender.send,
                          Message(subject='a.b', body='A'))
        nhop1.stop()
        conn.close()

        self.assertTrue(nhop1.queue.empty())

    def test_remote_exchange(self):
        """
        Verify that the exchange and bindings are visible to other routers in
        the network
        """
        def router(self, name, extra_config):

            config = [
                ('router', {'mode': 'interior', 'id': 'QDR.%s'%name, 'allowUnsettledMulticast': 'yes'}),
                ('listener', {'port': self.tester.get_port(), 'stripAnnotations': 'no'})
            ] + extra_config

            config = Qdrouterd.Config(config)

            self.routers.append(self.tester.qdrouterd(name, config, wait=True))

        self.inter_router_port = self.tester.get_port()
        self.routers = []

        router(self, 'A',
               [('listener',
                 {'role': 'inter-router', 'port': self.inter_router_port}),

                ('address', {'pattern': 'nextHop1/#',
                             'distribution': 'multicast'}),
                ('address', {'pattern': 'nextHop2/#',
                             'distribution': 'balanced'}),
                ('address', {'pattern': 'nextHop3/#',
                             'distribution': 'closest'}),

                ('exchange', {'address': 'AddressA',
                              'name': 'ExchangeA',
                              'matchMethod': 'mqtt'}),

                ('binding', {'name': 'bindingA1',
                             'exchange': 'ExchangeA',
                             'key': 'a/b',
                             'nextHop': 'nextHop1'}),
                ('binding', {'name': 'bindingA2',
                             'exchange': 'ExchangeA',
                             'key': 'a/+',
                             'nextHop': 'nextHop2'}),
                ('binding', {'name': 'bindingA3',
                             'exchange': 'ExchangeA',
                             'key': '+/b',
                             'nextHop': 'nextHop3'}),
                ('binding', {'name': 'bindingA4',
                             'exchange': 'ExchangeA',
                             'key': 'a/#',
                             'nextHop': 'NotSubscribed'})
               ])

        router(self, 'B',
               [('connector', {'name': 'connectorToA',
                               'role': 'inter-router',
                               'port': self.inter_router_port,
                               'verifyHostName': 'no'}),
                ('address', {'pattern': 'nextHop1/#',
                             'distribution': 'multicast'}),
                ('address', {'pattern': 'nextHop2/#',
                             'distribution': 'balanced'}),
                ('address', {'pattern': 'nextHop3/#',
                             'distribution': 'closest'})
               ])

        self.routers[0].wait_router_connected('QDR.B')
        self.routers[1].wait_router_connected('QDR.A')
        self.routers[1].wait_address('AddressA')

        # connect clients to router B (no exchange)
        nhop1A = _AsyncReceiver(self.routers[1].addresses[0], 'nextHop1')
        nhop1B = _AsyncReceiver(self.routers[1].addresses[0], 'nextHop1')
        nhop2  = _AsyncReceiver(self.routers[1].addresses[0], 'nextHop2')
        nhop3  = _AsyncReceiver(self.routers[1].addresses[0], 'nextHop3')

        self.routers[0].wait_address('nextHop1', remotes=1)
        self.routers[0].wait_address('nextHop2', remotes=1)
        self.routers[0].wait_address('nextHop3', remotes=1)

        conn = BlockingConnection(self.routers[1].addresses[0])
        sender = conn.create_sender(address="AddressA", options=AtLeastOnce())
        sender.send(Message(subject='a/b', body='Hi!'))

        # multicast
        self.assertEqual('Hi!', nhop1A.queue.get(timeout=TIMEOUT).body)
        self.assertEqual('Hi!', nhop1B.queue.get(timeout=TIMEOUT).body)

        # balanced and closest
        self.assertEqual('Hi!', nhop2.queue.get(timeout=TIMEOUT).body)
        self.assertEqual('Hi!', nhop3.queue.get(timeout=TIMEOUT).body)

        nhop1A.stop()
        nhop1B.stop()
        nhop2.stop()
        nhop3.stop()
        conn.close()

    def test_large_messages(self):
        """
        Verify that multi-frame messages are forwarded properly
        """
        MAX_FRAME=1024
        config = [
            ('router', {'mode': 'interior', 'id': 'QDR.X',
                        'allowUnsettledMulticast': 'yes'}),
            ('listener', {'port': self.tester.get_port(),
                          'stripAnnotations': 'no',
                          'maxFrameSize': MAX_FRAME}),

            ('address', {'pattern': 'nextHop1/#',
                         'distribution': 'multicast'}),

            ('exchange', {'address': 'AddressA',
                          'name': 'ExchangeA'}),

            ('binding', {'name': 'bindingA1',
                         'exchange': 'ExchangeA',
                         'key': 'a/b',
                         'nextHop': 'nextHop1'})
        ]

        router = self.tester.qdrouterd('QDR.X',
                                       Qdrouterd.Config(config),
                                       wait=True)

        # connect clients to router B (no exchange)
        nhop1A = _AsyncReceiver(router.addresses[0], 'nextHop1',
                                conn_args={'max_frame_size': MAX_FRAME})
        nhop1B = _AsyncReceiver(router.addresses[0], 'nextHop1',
                                conn_args={'max_frame_size': MAX_FRAME})

        conn = BlockingConnection(router.addresses[0],
                                  max_frame_size=MAX_FRAME)
        sender = conn.create_sender(address="AddressA")
        jumbo = (10 * MAX_FRAME) * 'X'
        sender.send(Message(subject='a/b', body=jumbo))

        # multicast
        self.assertEqual(jumbo, nhop1A.queue.get(timeout=TIMEOUT).body)
        self.assertEqual(jumbo, nhop1B.queue.get(timeout=TIMEOUT).body)

        nhop1A.stop()
        nhop1B.stop()
        conn.close()


if __name__ == '__main__':
    unittest.main(main_module())


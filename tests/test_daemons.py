# -*- coding: utf-8 -*-
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import mock

import pytest

import rucio.db.sqla.util
from rucio.common import exception
from rucio.daemons.automatix import automatix
from rucio.daemons.badreplicas import minos, minos_temporary_expiration, necromancer
from rucio.daemons.c3po import c3po
from rucio.daemons.cache import consumer
from rucio.daemons.conveyor import finisher, poller, receiver, stager, submitter, throttler, preparer
from rucio.daemons.follower import follower
from rucio.daemons.hermes import hermes
from rucio.daemons.judge import cleaner, evaluator, injector
from rucio.daemons.oauthmanager import oauthmanager
from rucio.daemons.reaper import dark_reaper
from rucio.daemons.replicarecoverer import suspicious_replica_recoverer
from rucio.daemons.tracer import kronos
from rucio.daemons.transmogrifier import transmogrifier
from rucio.daemons.common import Daemon

DAEMONS = [
    automatix,
    minos,
    minos_temporary_expiration,
    necromancer,
    c3po,
    consumer,
    finisher,
    poller,
    receiver,
    stager,
    submitter,
    throttler,
    preparer,
    follower,
    hermes,
    cleaner,
    evaluator,
    injector,
    oauthmanager,
    dark_reaper,
    suspicious_replica_recoverer,
    kronos,
    transmogrifier,
]

ids = [mod.__name__ for mod in DAEMONS]


# This test is to be deleted once https://github.com/rucio/rucio/issues/6478 is completed
@pytest.mark.parametrize('daemon', argvalues=DAEMONS, ids=ids)
@mock.patch('rucio.db.sqla.util.is_old_db')
def test_fail_on_old_database_parametrized(mock_is_old_db, daemon):
    """ DAEMON: Test daemon failure on old database """
    mock_is_old_db.return_value = True
    assert rucio.db.sqla.util.is_old_db() is True

    with pytest.raises(exception.DatabaseException, match='Database was not updated, daemon won\'t start'):
        daemon.run()

    assert mock_is_old_db.call_count > 1


class DaemonTest(Daemon):
    def _run_once(self, heartbeat_handler, **_kwargs):
        pass


@mock.patch('rucio.db.sqla.util.is_old_db')
def test_fail_on_old_database(mock_is_old_db):
    """ DAEMON: Test daemon failure on old database """
    mock_is_old_db.return_value = True
    assert rucio.db.sqla.util.is_old_db() is True

    with pytest.raises(exception.DatabaseException, match='Database was not updated, daemon won\'t start'):
        DaemonTest().run()

    assert mock_is_old_db.call_count > 1

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

"""
Abacus-Collection-Replica is a daemon to update collection replica.
"""
import logging
import time
from typing import Any

from rucio.core.replica import get_cleaned_updated_collection_replicas, update_collection_replica
from rucio.daemons.common import Daemon, HeartbeatHandler


class AbacusCollectionReplica(Daemon):
    def __init__(self, limit: int = 1000, **_kwargs) -> None:
        """
        :param limit: Amount of collection replicas to retrieve per chunk.
        """
        super().__init__(daemon_name="abacus-collection-replica", **_kwargs)
        self.limit = limit

    def _run_once(self, heartbeat_handler: "HeartbeatHandler", **_kwargs) -> tuple[bool, Any]:
        worker_number, total_workers, logger = heartbeat_handler.live()
        must_sleep = False

        # Select a bunch of collection replicas for to update for this worker
        start = time.time()  # NOQA
        replicas = get_cleaned_updated_collection_replicas(total_workers=total_workers - 1,
                                                           worker_number=worker_number,
                                                           limit=self.limit)

        logger(logging.DEBUG, 'Index query time %f size=%d' % (time.time() - start, len(replicas)))
        # If the list is empty, sent the worker to sleep
        if not replicas:
            logger(logging.INFO, 'did not get any work')
            must_sleep = True
            return must_sleep, None

        for replica in replicas:
            worker_number, total_workers, logger = heartbeat_handler.live()
            if self.graceful_stop.is_set():
                break
            start_time = time.time()
            update_collection_replica(replica)
            logger(logging.DEBUG, 'update of collection replica "%s" took %f' % (replica['id'], time.time() - start_time))

        if self.limit and len(replicas) < self.limit:
            must_sleep = True

        return must_sleep, None

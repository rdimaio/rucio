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
Abacus-RSE is a daemon to update RSE counters.
"""

import logging
import time
from typing import Any

from rucio.core.rse_counter import get_updated_rse_counters, update_rse_counter
from rucio.daemons.abacus.common import AbacusDaemon
from rucio.daemons.common import HeartbeatHandler


class AbacusRSE(AbacusDaemon):
    def __init__(self, sleep_time: int = 10, **_kwargs) -> None:
        super().__init__(daemon_name="abacus-rse", sleep_time=sleep_time, **_kwargs)

    def _run_once(self, heartbeat_handler: "HeartbeatHandler", **_kwargs) -> tuple[bool, Any]:
        worker_number, total_workers, logger = heartbeat_handler.live()
        must_sleep = False

        # Select a bunch of rses for to update for this worker
        start = time.time()  # NOQA
        rse_ids = get_updated_rse_counters(total_workers=total_workers,
                                           worker_number=worker_number)
        logger(logging.DEBUG, 'Index query time %f size=%d' % (time.time() - start, len(rse_ids)))

        # If the list is empty, sent the worker to sleep
        if not rse_ids:
            logger(logging.INFO, 'did not get any work')
            return must_sleep, None

        for rse_id in rse_ids:
            worker_number, total_workers, logger = heartbeat_handler.live()
            if self.graceful_stop.is_set():
                break
            start_time = time.time()
            update_rse_counter(rse_id=rse_id)
            logger(logging.DEBUG, 'update of rse "%s" took %f' % (rse_id, time.time() - start_time))
        return must_sleep, None

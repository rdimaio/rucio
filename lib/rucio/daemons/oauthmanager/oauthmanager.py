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
OAuth Manager is a daemon which is reponsible for:
- deletion of expired access tokens (in case there is a valid refresh token,
  expired access tokens will be kept until refresh_token expires as well.)
- deletion of expired OAuth session parameters
- refreshing access tokens via their refresh tokens.

These 3 actions run consequently one after another in a loop with a sleeptime of 'looprate' seconds.
The maximum number of DB rows (tokens, parameters, refresh tokens) on which the script will operate
can be specified by 'max_rows' parameter.

"""


import logging
import traceback
from re import match
from typing import Any
from rucio.db.sqla.constants import ORACLE_CONNECTION_LOST_CONTACT_REGEX

from sqlalchemy.exc import DatabaseError

from rucio.common.exception import DatabaseException
from rucio.common.stopwatch import Stopwatch
from rucio.core.authentication import delete_expired_tokens
from rucio.core.monitor import MetricManager
from rucio.core.oidc import delete_expired_oauthrequests, refresh_jwt_tokens
from rucio.daemons.common import Daemon, HeartbeatHandler

METRICS = MetricManager(module=__name__)


class OAuthManager(Daemon):
    """
    Daemon to delete all expired tokens, refresh tokens eligible
    for refresh and delete all expired OAuth session parameters.
    It was decided to have only 1 daemon for all 3 of these cleanup activities.
    """
    def __init__(self, sleep_time: int = 300, max_rows: int = 100, **_kwargs) -> None:
        """
        :param max_rows: Max number of DB rows to deal with per operation.
        """
        super().__init__(daemon_name="oauth-manager", sleep_time=sleep_time, **_kwargs)
        self.max_rows = max_rows
        self.paused_dids = {}

    def _run_once(self, heartbeat_handler: "HeartbeatHandler", **_kwargs) -> tuple[bool, Any]:
        # make an initial heartbeat
        heartbeat_handler.live()

        must_sleep = False

        stopwatch = Stopwatch()

        ndeleted = 0
        ndeletedreq = 0
        nrefreshed = 0

        # make a heartbeat
        worker_number, total_workers, logger = heartbeat_handler.live()
        try:
            # ACCESS TOKEN REFRESH - better to run first (in case some of the refreshed tokens needed deletion after this step)
            logger(logging.INFO, '----- START ----- ACCESS TOKEN REFRESH ----- ')
            logger(logging.INFO, 'starting to query tokens for automatic refresh')
            nrefreshed = refresh_jwt_tokens(total_workers, worker_number, refreshrate=int(self.sleep_time), limit=self.max_rows)
            logger(logging.INFO, 'successfully refreshed %i tokens', nrefreshed)
            logger(logging.INFO, '----- END ----- ACCESS TOKEN REFRESH ----- ')
            METRICS.counter(name='oauth_manager.tokens.refreshed').inc(nrefreshed)

        except (DatabaseException, DatabaseError) as err:
            if match('.*QueuePool.*', str(err.args[0])):
                logger(logging.WARNING, traceback.format_exc())
                METRICS.counter('exceptions.{exception}').labels(exception=err.__class__.__name__).inc()
            elif match(ORACLE_CONNECTION_LOST_CONTACT_REGEX, str(err.args[0])):
                logger(logging.WARNING, traceback.format_exc())
                METRICS.counter('exceptions.{exception}').labels(exception=err.__class__.__name__).inc()
            else:
                logger(logging.CRITICAL, traceback.format_exc())
                METRICS.counter('exceptions.{exception}').labels(exception=err.__class__.__name__).inc()

        try:
            # waiting 1 sec as DBs does not store milisecond and tokens
            # eligible for deletion after refresh might not get deleted otherwise
            self.graceful_stop.wait(1)

            # make a heartbeat
            worker_number, total_workers, logger = heartbeat_handler.live()

            # EXPIRED TOKEN DELETION
            logger(logging.INFO, '----- START ----- DELETION OF EXPIRED TOKENS ----- ')
            logger(logging.INFO, 'starting to delete expired tokens')
            ndeleted += delete_expired_tokens(total_workers, worker_number, limit=self.max_rows)
            logger(logging.INFO, 'deleted %i expired tokens', ndeleted)
            logger(logging.INFO, '----- END ----- DELETION OF EXPIRED TOKENS ----- ')
            METRICS.counter(name='oauth_manager.tokens.deleted').inc(ndeleted)

        except (DatabaseException, DatabaseError) as err:
            if match('.*QueuePool.*', str(err.args[0])):
                logger(logging.WARNING, traceback.format_exc())
                METRICS.counter('exceptions.{exception}').labels(exception=err.__class__.__name__).inc()
            elif match(ORACLE_CONNECTION_LOST_CONTACT_REGEX, str(err.args[0])):
                logger(logging.WARNING, traceback.format_exc())
                METRICS.counter('exceptions.{exception}').labels(exception=err.__class__.__name__).inc()
            else:
                logger(logging.CRITICAL, traceback.format_exc())
                METRICS.counter('exceptions.{exception}').labels(exception=err.__class__.__name__).inc()

        try:
            # make a heartbeat
            worker_number, total_workers, logger = heartbeat_handler.live()

            # DELETING EXPIRED OAUTH SESSION PARAMETERS
            logger(logging.INFO, '----- START ----- DELETION OF EXPIRED OAUTH SESSION REQUESTS ----- ')
            logger(logging.INFO, 'starting deletion of expired OAuth session requests')
            ndeletedreq += delete_expired_oauthrequests(total_workers, worker_number, limit=self.max_rows)
            logger(logging.INFO, 'expired parameters of %i authentication requests were deleted', ndeletedreq)
            logger(logging.INFO, '----- END ----- DELETION OF EXPIRED OAUTH SESSION REQUESTS ----- ')
            METRICS.counter(name='oauth_manager.oauthreq.deleted').inc(ndeletedreq)

        except (DatabaseException, DatabaseError) as err:
            if match('.*QueuePool.*', str(err.args[0])):
                logger(logging.WARNING, traceback.format_exc())
                METRICS.counter('exceptions.{exception}').labels(exception=err.__class__.__name__).inc()
            elif match(ORACLE_CONNECTION_LOST_CONTACT_REGEX, str(err.args[0])):
                logger(logging.WARNING, traceback.format_exc())
                METRICS.counter('exceptions.{exception}').labels(exception=err.__class__.__name__).inc()
            else:
                logger(logging.CRITICAL, traceback.format_exc())
                METRICS.counter('exceptions.{exception}').labels(exception=err.__class__.__name__).inc()

        stopwatch.stop()
        logger(logging.INFO, 'took %f seconds to delete %i tokens, %i session parameters and refreshed %i tokens', stopwatch.elapsed, ndeleted, ndeletedreq, nrefreshed)
        METRICS.timer('duration').observe(stopwatch.elapsed)
        return must_sleep, None

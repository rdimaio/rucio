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

import datetime
import logging
import random
from sys import exc_info
from traceback import format_exception
from typing import Any

import rucio.core.lifetime_exception
import rucio.db.sqla.util
from rucio.common.exception import InvalidRSEExpression, RuleNotFound
from rucio.core.did import set_metadata
from rucio.core.lock import get_dataset_locks
from rucio.core.rse import get_rse_name, get_rse_vo
from rucio.core.rse_expression_parser import parse_expression
from rucio.core.rule import get_rules_beyond_eol, update_rule
from rucio.daemons.common import Daemon, HeartbeatHandler
from rucio.db.sqla.constants import LifetimeExceptionsState


class Atropos(Daemon):
    def __init__(self, once: bool = True, partition_wait_time: int = 10,
                 date_check: datetime.datetime = datetime.datetime.utcnow(),
                 dry_run: bool = True,
                 grace_period: int = 86400,
                 purge_replicas: bool = False,
                 spread_period: int = 0,
                 unlock: bool = False,
                 **_kwargs) -> None:
        """
        :param date_check:     The reference date that should be compared to eol_at. Cannot be used for a date in the future if dry-run is not enabled.
        :param dry_run:        If True, run Atropos in dry-run mode.
        :param grace_period:   Grace period (in seconds) for the rules.
        :param purge_replicas: If True, set replicas to be deleted instead of secondarised.
        :param spread_period:  Set the rules to randomly expire over a period (in seconds). Uses a uniform distribution
        :param unlock:         If True, when a rule is locked, set it to be unlocked.
        """
        super().__init__(daemon_name="atropos", once=once, partition_wait_time=partition_wait_time, **_kwargs)
        self.date_check = date_check
        self.dry_run = dry_run
        self.grace_period = grace_period
        self.purge_replicas = purge_replicas
        self.spread_period = spread_period
        self.unlock = unlock

    def _run_once(self, heartbeat_handler: "HeartbeatHandler", **_kwargs) -> tuple[bool, Any]:
        must_sleep = False
        worker_number, total_workers, logger = heartbeat_handler.live()
        logger(logging.DEBUG, 'Starting worker')

        if not self.dry_run and self.date_check > datetime.datetime.utcnow():
            logger(logging.ERROR, 'Atropos cannot run in non-dry-run mode for date in the future')
            return must_sleep, None

        # Process the list of approved exceptions. In case a DID has
        # multiple exceptions, the one with the expiration date further in
        # the future is what matters.
        summary = {}
        lifetime_exceptions = {}
        for excep in rucio.core.lifetime_exception.list_exceptions(exception_id=None, states=[LifetimeExceptionsState.APPROVED, ], session=None):
            key = '{}:{}'.format(excep['scope'].internal, excep['name'])
            if key not in lifetime_exceptions:
                lifetime_exceptions[key] = excep['expires_at']
            elif lifetime_exceptions[key] < excep['expires_at']:
                lifetime_exceptions[key] = excep['expires_at']
        logger(logging.DEBUG, '%d active exceptions', len(lifetime_exceptions))

        rand = random.Random(worker_number)

        try:
            rules = get_rules_beyond_eol(self.date_check, worker_number, total_workers, session=None)
            logger(logging.INFO, '%d rules to process', len(rules))
            for rule_idx, rule in enumerate(rules, start=1):
                did = '%s:%s' % (rule.scope, rule.name)
                did_key = '{}:{}'.format(rule.scope.internal, rule.name)
                logger(logging.DEBUG, 'Working on rule %s on DID %s on %s', rule.id, did, rule.rse_expression)

                if (rule_idx % 1000) == 0:
                    logger(logging.INFO, '%s/%s rules processed', rule_idx, len(rules))

                # We compute the expected eol_at
                try:
                    rses = parse_expression(rule.rse_expression, filter_={'vo': rule.account.vo})
                except InvalidRSEExpression:
                    logger(logging.WARNING, 'Rule %s has an RSE expression that results in an empty set: %s', rule.id, rule.rse_expression)
                    continue
                eol_at = rucio.core.lifetime_exception.define_eol(rule.scope, rule.name, rses)
                if eol_at != rule.eol_at:
                    logger(logging.WARNING, 'The computed eol %s differs from the one recorded %s for rule %s on %s at %s',
                           eol_at, rule.eol_at, rule.id, did, rule.rse_expression)
                    try:
                        update_rule(rule.id, options={'eol_at': eol_at})
                    except RuleNotFound:
                        logger(logging.WARNING, 'Cannot find rule %s on DID %s', rule.id, did)
                        continue

                # Check the exceptions
                if did_key in lifetime_exceptions:
                    if eol_at > lifetime_exceptions[did_key]:
                        logger(logging.INFO, 'Rule %s on DID %s on %s has longer expiration date than the one requested : %s',
                               rule.id, did, rule.rse_expression, lifetime_exceptions[did_key])
                    else:
                        # If eol_at < requested extension, update eol_at
                        logger(logging.INFO, 'Updating rule %s on DID %s on %s according to the exception till %s',
                               rule.id, did, rule.rse_expression, lifetime_exceptions[did_key])
                        eol_at = lifetime_exceptions[did_key]
                        try:
                            update_rule(rule.id, options={'eol_at': lifetime_exceptions[did_key]})
                        except RuleNotFound:
                            logger(logging.WARNING, 'Cannot find rule %s on DID %s', rule.id, did)
                            continue

                # Now check that the new eol_at is expired
                if eol_at and eol_at <= self.date_check:
                    set_metadata(scope=rule.scope, name=rule.name, key='eol_at', value=eol_at)
                    no_locks = True
                    for lock in get_dataset_locks(rule.scope, rule.name):
                        if lock['rule_id'] == rule[4]:
                            no_locks = False
                            if lock['rse_id'] not in summary:
                                summary[lock['rse_id']] = {}
                            if did_key not in summary[lock['rse_id']]:
                                summary[lock['rse_id']][did_key] = {'length': lock['length'] or 0, 'bytes': lock['bytes'] or 0}
                    if no_locks:
                        logger(logging.WARNING, 'Cannot find a lock for rule %s on DID %s', rule.id, did)
                    if not self.dry_run:
                        lifetime = self.grace_period + rand.randrange(self.spread_period + 1)
                        logger(logging.INFO, 'Setting %s seconds lifetime for rule %s', lifetime, rule.id)
                        options = {'lifetime': lifetime}
                        if self.purge_replicas:
                            options['purge_replicas'] = True
                        if rule.locked and self.unlock:
                            logger(logging.INFO, 'Unlocking rule %s', rule.id)
                            options['locked'] = False
                        try:
                            update_rule(rule.id, options=options)
                        except RuleNotFound:
                            logger(logging.WARNING, 'Cannot find rule %s on DID %s', rule.id, did)
                            continue
        except Exception:
            exc_type, exc_value, exc_traceback = exc_info()
            logger(logging.CRITICAL, ''.join(format_exception(exc_type, exc_value, exc_traceback)).strip())

        for rse_id in summary:
            tot_size, tot_files, tot_datasets = 0, 0, 0
            for did in summary[rse_id]:
                tot_datasets += 1
                tot_files += summary[rse_id][did].get('length', 0)
                tot_size += summary[rse_id][did].get('bytes', 0)
            vo = get_rse_vo(rse_id=rse_id)
            logger(logging.INFO, 'For RSE %s%s %d datasets will be deleted representing %d files and %d bytes',
                   get_rse_name(rse_id=rse_id), '' if vo == 'def' else ' on VO ' + vo, tot_datasets, tot_files, tot_size)
        return must_sleep, None

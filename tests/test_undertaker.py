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

from collections import namedtuple
from datetime import datetime, timedelta
from logging import getLogger

import pytest

from rucio.common.exception import DataIdentifierNotFound
from rucio.common.policy import get_policy
from rucio.common.types import InternalScope
from rucio.core.account_limit import set_local_account_limit
from rucio.core.did import add_dids, attach_dids, list_all_parent_dids, list_content, list_expired_dids, get_did, set_metadata
from rucio.core.replica import add_replicas, get_replica
from rucio.core.rse import add_rse
from rucio.core.rule import add_rule, add_rules, list_rules
from rucio.daemons.judge.cleaner import rule_cleaner
from rucio.daemons.undertaker.undertaker import undertaker
from rucio.db.sqla.constants import OBSOLETE
from rucio.db.sqla.util import json_implemented
from rucio.tests.common import rse_name_generator, did_name_generator

LOG = getLogger(__name__)
RSE_namedtuple = namedtuple('RSE_namedtuple', ['name', 'id'])


@pytest.mark.dirty
@pytest.mark.noparallel(reason='uses pre-defined rses; runs undertaker, which impacts other tests')
class TestUndertaker:

    def test_undertaker(self, rse_factory, mock_scope, root_account, jdoe_account):
        """
        UNDERTAKER (CORE): Test expired datasets.

        Dataset DIDs which are expired must be deleted. All attached file DIDs
        must be detached and their replicas must receive the epoch tombstone.
        """

        nbdatasets = 5
        nbfiles = 5
        rse, rse_id = rse_factory.make_rse()

        set_local_account_limit(jdoe_account, rse_id, -1)

        dsns1 = [{'name': did_name_generator('dataset'),
                  'scope': mock_scope,
                  'type': 'DATASET',
                  'lifetime': -1} for _ in range(nbdatasets)]

        dsns2 = [{'name': did_name_generator('dataset'),
                  'scope': mock_scope,
                  'type': 'DATASET',
                  'lifetime': -1,
                  'rules': [{'account': jdoe_account, 'copies': 1,
                             'rse_expression': rse,
                             'grouping': 'DATASET'}]} for _ in range(nbdatasets)]

        add_dids(dids=dsns1 + dsns2, account=root_account)

        # arbitrary keys do not work without JSON support (sqlite, Oracle < 12)
        if json_implemented():
            # Add generic metadata on did
            set_metadata(mock_scope, dsns1[0]['name'], "test_key", "test_value")

        replicas = list()
        for dsn in dsns1 + dsns2:
            files = [{'scope': mock_scope, 'name': did_name_generator('file'),
                      'bytes': 1, 'adler32': '0cc737eb',
                      'tombstone': datetime.utcnow() + timedelta(weeks=2), 'meta': {'events': 10}} for _ in range(nbfiles)]
            attach_dids(scope=mock_scope, name=dsn['name'], rse_id=rse_id, dids=files, account=root_account)
            replicas += files

        add_rules(dids=dsns1, rules=[{'account': jdoe_account, 'copies': 1, 'rse_expression': rse, 'grouping': 'DATASET'}])

        undertaker(once=True)

        # Assert dataset no longer exists
        for dsn in dsns1 + dsns2:
            with pytest.raises(DataIdentifierNotFound):
                get_did(mock_scope, dsn['name'])

        for replica in replicas:
            assert get_replica(scope=replica['scope'], name=replica['name'], rse_id=rse_id)['tombstone'] == OBSOLETE

    def test_list_expired_dids_with_locked_rules(self, rse_factory, vo, mock_scope, root_account, jdoe_account):
        """ UNDERTAKER (CORE): Test that the undertaker does not list expired dids with locked rules"""

        # Add quota
        rse, rse_id = rse_factory.make_rse()
        set_local_account_limit(jdoe_account, rse_id, -1)

        dsn = {'name': did_name_generator('dataset'),
               'scope': mock_scope,
               'type': 'DATASET',
               'lifetime': -1,
               'rules': [{'account': jdoe_account, 'copies': 1,
                          'rse_expression': rse, 'locked': True,
                          'grouping': 'DATASET'}]}

        add_dids(dids=[dsn], account=root_account)

        for did in list_expired_dids(limit=1000):
            assert (did['scope'], did['name']) != (dsn['scope'], dsn['name'])

    def test_atlas_archival_policy(self, vo, mock_scope, root_account, jdoe_account):
        """ UNDERTAKER (CORE): Test the atlas archival policy. """
        if get_policy() != 'atlas':
            LOG.info("Skipping atlas-specific test")
            return

        nbdatasets = 5
        nbfiles = 5

        rse = 'LOCALGROUPDISK_%s' % rse_name_generator()
        rse_id = add_rse(rse, vo=vo)

        set_local_account_limit(jdoe_account, rse_id, -1)

        dsns2 = [{'name': did_name_generator('dataset'),
                  'scope': mock_scope,
                  'type': 'DATASET',
                  'lifetime': -1,
                  'rules': [{'account': jdoe_account, 'copies': 1,
                             'rse_expression': rse,
                             'grouping': 'DATASET'}]} for _ in range(nbdatasets)]

        add_dids(dids=dsns2, account=root_account)

        replicas = list()
        for dsn in dsns2:
            files = [{'scope': mock_scope, 'name': did_name_generator('file'), 'bytes': 1,
                      'adler32': '0cc737eb', 'tombstone': datetime.utcnow() + timedelta(weeks=2), 'meta': {'events': 10}} for _ in range(nbfiles)]
            attach_dids(scope=mock_scope, name=dsn['name'], rse_id=rse_id, dids=files, account=root_account)
            replicas += files

        undertaker(once=True)

        for replica in replicas:
            assert (get_replica(scope=replica['scope'], name=replica['name'], rse_id=rse_id)['tombstone'] is None)

        for dsn in dsns2:
            assert (get_did(scope=InternalScope('archive', vo=vo), name=dsn['name'])['name'] == dsn['name'])
            assert (len([x for x in list_rules(filters={'scope': InternalScope('archive', vo=vo), 'name': dsn['name']})]) == 1)


@pytest.mark.noparallel(reason='runs undertaker, which impacts other tests')
@pytest.mark.parametrize('add_rule', [True, False])
@pytest.mark.parametrize("core_config_mock", [{"table_content": [
    ('undertaker', 'purge_all_replicas', True)
]}], indirect=True)
@pytest.mark.parametrize("caches_mock", [{"caches_to_mock": [
    'rucio.core.config.REGION',
]}], indirect=True)
@pytest.mark.parametrize("file_config_mock", [
    # Run test twice: with, and without, temp tables
    {"overrides": [('core', 'use_temp_tables', 'True')]},
    {"overrides": [('core', 'use_temp_tables', 'False')]},
], indirect=True)
def test_file_did_deletion(add_rule, mock_scope, rse_factory, root_account, core_config_mock, caches_mock, file_config_mock):
    """
    UNDERTAKER (CORE): test file DID deletion
    For a file DID that is not attached to any dataset (i.e. having no parent),
    test whether the Undertaker sets the epoch tombstone on the file replica.
    This must happen regardless of whether rules exist on the file DID. The file
    DID itself must not be deleted.
    """

    rse, rse_id = rse_factory.make_mock_rse()
    name = did_name_generator('file')

    # Add DID for individual file
    file = {
        'scope': mock_scope,
        'name': name,
        'bytes': 1,
    }
    add_replicas(
        rse_id=rse_id,
        files=[file],
        account=root_account,
        ignore_availability=True
    )

    set_metadata(mock_scope, name, 'expired_at', datetime.utcnow() - timedelta(weeks=2))

    # Set rule: at the moment, a rule needs to be set for undertaker to set a tombstone (and not just remove the expiry date)
    if add_rule:
        rule = {'account': root_account, 'copies': 1, 'rse_expression': rse}
        add_rules([file], [rule])

    undertaker(once=True)

    assert get_replica(scope=mock_scope, name=name, rse_id=rse_id)['tombstone'] == OBSOLETE

    # Assert that the DID still exists
    assert get_did(mock_scope, name) is not None


@pytest.mark.parametrize("core_config_mock", [{"table_content": [
    ('undertaker', 'purge_all_replicas', True)
]}], indirect=True)
@pytest.mark.parametrize("caches_mock", [{"caches_to_mock": [
    'rucio.core.config.REGION',
]}], indirect=True)
@pytest.mark.parametrize("file_config_mock", [
    # Run test twice: with, and without, temp tables
    {"overrides": [('core', 'use_temp_tables', 'True')]},
    {"overrides": [('core', 'use_temp_tables', 'False')]},
], indirect=True)
def test_file_dids_in_dataset(mock_scope, rse_factory, did_factory, root_account, core_config_mock, caches_mock, file_config_mock):
    """
    UNDERTAKER (CORE): delete expired files DIDs within datasets
    Test on expired file DIDs that are assigned to a non-expired dataset.
    The expected outcome is that the expired file DID is detached
    and all its replicas (on both RSE1 and RSE2) are epoch-tombstoned,
    while the non-expired file DID is unchanged and the dataset is unchanged (except for losing a member).
    """

    rse1, rse2 = RSE_namedtuple(*rse_factory.make_mock_rse()), RSE_namedtuple(*rse_factory.make_mock_rse())

    # Create three file DIDs
    fnames = [did_name_generator('file') for _ in range(3)]
    files = [
        {'name': name, 'scope': mock_scope, 'bytes': 1}
        for name in fnames
    ]
    add_replicas(rse1.id, files, root_account)
    add_replicas(rse2.id, files, root_account)

    # Add metadata expiry dates
    # Past expiry date
    set_metadata(mock_scope, fnames[0], 'expired_at', datetime.utcnow() - timedelta(weeks=2))
    # Future expiry date
    set_metadata(mock_scope, fnames[1], 'expired_at', datetime.utcnow() + timedelta(weeks=2))

    # Create dataset on RSE1, attach files to dataset
    dataset = did_factory.make_dataset()
    attach_dids(dids=files, account=root_account, rse_id=rse1.id, **dataset)

    # Create dataset in container hierarchy for RSE2, attach files to child_dataset
    container = did_factory.make_container()
    child_dataset = did_factory.make_dataset()
    attach_dids(dids=[child_dataset], account=root_account, rse_id=rse2.id, **container)
    attach_dids(dids=files, account=root_account, rse_id=rse2.id, **child_dataset)

    undertaker(once=True)

    # assert expired file DID replica has epoch tombstone
    for rse in (rse1, rse2):
        assert get_replica(scope=mock_scope, name=fnames[0], rse_id=rse.id)['tombstone'] == OBSOLETE
        assert get_replica(scope=mock_scope, name=fnames[1], rse_id=rse.id)['tombstone'] is None
        assert get_replica(scope=mock_scope, name=fnames[2], rse_id=rse.id)['tombstone'] is None

    # assert each dataset still exists, contains only two file DIDs now
    for dset in (dataset, child_dataset):
        dsetcontent = list(list_content(mock_scope, dset['name']))
        for fname in fnames[1:]:
            assert len(list(filter(lambda d: d['name'] == fname, dsetcontent))) == 1

    # assert no contents in list_parent_dids for detached file
    assert len(list(list_all_parent_dids(mock_scope, fnames[0]))) == 0


@pytest.mark.parametrize("core_config_mock", [{"table_content": [
    ('undertaker', 'purge_all_replicas', True)
]}], indirect=True)
@pytest.mark.parametrize("caches_mock", [{"caches_to_mock": [
    'rucio.core.config.REGION',
]}], indirect=True)
@pytest.mark.parametrize("file_config_mock", [
    # Run test twice: with, and without, temp tables
    {"overrides": [('core', 'use_temp_tables', 'True')]},
    {"overrides": [('core', 'use_temp_tables', 'False')]},
], indirect=True)
def test_file_protected_by_rule(mock_scope, rse_factory, did_factory, root_account, core_config_mock, caches_mock, file_config_mock):
    """
    UNDERTAKER (CORE): test the deletion of a file that is protected by a rule
    Setup:
        - Create dataset ds1 (expired).
        - Assign ds1 to rse1 by ru1.
        - Create files f0 and f1 and attach both to dataset ds1.
        - Assign f0 to rse2 via ru2.
        - Run the undertaker.
    Expected results:
        - Dataset ds1 is removed.
        - File f1 has no more parents and its only replica (on rse1) has the epoch tombstone.
        - File f0 is detached from ds1.
        - f0 replica on rse1 has the epoch tombstone.
        - f0 replica on rse2 does not have a tombstone.
    """
    rse1, rse2 = RSE_namedtuple(*rse_factory.make_mock_rse()), RSE_namedtuple(*rse_factory.make_mock_rse())

    # Create dataset ds1, assign to rse1 via rule ru1, lifetime expired
    ds1 = did_factory.make_dataset(scope=mock_scope)
    ru1 = add_rule(  # noqa: F841
        dids=[ds1],
        account=root_account,
        copies=1,
        rse_expression=rse1.name,
        grouping='DATASET',
        weight=None,
        lifetime=None,
        subscription_id=None,
        locked=False
    )[0]

    # Set expiry date in the past
    set_metadata(key='expired_at', value=datetime.utcnow() - timedelta(weeks=2), **ds1)

    # Create 2 files, f0 and f1 and add to rse1
    fname0, fname1 = (did_name_generator('file') for _ in range(2))
    f0 = {'name': fname0, 'scope': mock_scope, 'bytes': 1}
    f1 = {'name': fname1, 'scope': mock_scope, 'bytes': 1}
    files = [f0, f1]
    add_replicas(rse1.id, files, root_account)

    # Add f0 and f1 to ds1
    attach_dids(dids=files, account=root_account, rse_id=rse1.id, **ds1)

    # Protect f0 by rule ru2, assigning to RSE2
    ru2 = add_rule(  # noqa: F841
        dids=[f0],
        account=root_account,
        copies=1,
        rse_expression=rse2.name,
        grouping='ALL',
        weight=None,
        lifetime=None,
        subscription_id=None,
        locked=False
    )[0]

    undertaker(once=True)

    # Assert ds1 (which was expired) no longer exists
    with pytest.raises(DataIdentifierNotFound):
        get_did(**ds1)

    # Assert f0 has no more parents (but the DID exists)
    assert not list(list_all_parent_dids(scope=mock_scope, name=fname0))

    # Assert replica r0 of f0 on rse1 to be tombstoned
    assert get_replica(scope=mock_scope, name=fname0, rse_id=rse1.id)['tombstone'] == OBSOLETE

    # Assert replica of f1 on rse1 to be tombstoned
    assert get_replica(scope=mock_scope, name=fname1, rse_id=rse1.id)['tombstone'] == OBSOLETE

    # Assert replica r1 of f0 on rse2 to not be tombstoned
    assert get_replica(scope=mock_scope, name=fname0, rse_id=rse2.id)['tombstone'] is None


@pytest.mark.parametrize("core_config_mock", [{"table_content": [
    ('undertaker', 'purge_all_replicas', True)
]}], indirect=True)
@pytest.mark.parametrize("caches_mock", [{"caches_to_mock": [
    'rucio.core.config.REGION',
]}], indirect=True)
@pytest.mark.parametrize("file_config_mock", [
    # Run test twice: with, and without, temp tables
    {"overrides": [('core', 'use_temp_tables', 'True')]},
    {"overrides": [('core', 'use_temp_tables', 'False')]},
], indirect=True)
def test_file_protected_by_dataset(mock_scope, rse_factory, did_factory, root_account, core_config_mock, caches_mock, file_config_mock):
    """
    UNDERTAKER (CORE): test the deletion of a file that is protected by a dataset
    Setup:
        - Create setup  with two datasets, ds1 (expired) and ds2.
        - Assign ds1 to rse1 by ru1 and assign ds2 to rse2 by ru2.
        - Create and attach File f1 to ds1. Create and attach File f2 to both datasets ds1, ds2.
        - Run the undertaker.
    Expected results:
        - Dataset ds1 is removed.
        - File f1 has no more parents and its only replica (on rse1) has the epoch tombstone.
        - File f0 is detached from ds1 but remains attached to ds2.
        - f0 replica on rse1 has the epoch tombstone.
        - f0 replica on rse2 does not have a tombstone.
    """
    rse1, rse2 = RSE_namedtuple(*rse_factory.make_mock_rse()), RSE_namedtuple(*rse_factory.make_mock_rse())

    ds1 = did_factory.make_dataset(scope=mock_scope)
    ru1 = add_rule(  # noqa: F841
        dids=[ds1],
        account=root_account,
        copies=1,
        rse_expression=rse1.name,
        grouping='DATASET',
        weight=None,
        lifetime=None,
        subscription_id=None,
        locked=False
    )[0]

    # Set expiry date
    set_metadata(key='expired_at', value=datetime.utcnow() - timedelta(weeks=2), **ds1)

    # Create dataset ds2, assign to rse2 via ru2
    ds2 = did_factory.make_dataset(scope=mock_scope)
    ru2 = add_rule(  # noqa: F841
        dids=[ds2],
        account=root_account,
        copies=1,
        rse_expression=rse2.name,
        grouping='DATASET',
        weight=None,
        lifetime=None,
        subscription_id=None,
        locked=True
    )[0]

    # Create 2 files, f0 and f1 and add to rse1
    fname0, fname1 = (did_name_generator('file') for _ in range(2))
    f0 = {'name': fname0, 'scope': mock_scope, 'bytes': 1}
    f1 = {'name': fname1, 'scope': mock_scope, 'bytes': 1}
    files = [f0, f1]
    add_replicas(rse1.id, files, root_account)

    # Add f0 and f1 to ds1
    attach_dids(dids=files, account=root_account, rse_id=rse1.id, **ds1)
    attach_dids(dids=[f0], account=root_account, rse_id=rse2.id, **ds2)

    undertaker(once=True)

    # Assert ds1 (which was expired) no longer exists
    with pytest.raises(DataIdentifierNotFound):
        get_did(**ds1)

    # Assert ds2 is the only parent of f0
    parentlist = list(list_all_parent_dids(scope=mock_scope, name=fname0))
    assert len(list(filter(lambda d: d['name'] == ds2['name'], parentlist))) == 1

    # Assert replica r0 of f0 on rse1 to be tombstoned
    assert get_replica(scope=mock_scope, name=fname0, rse_id=rse1.id)['tombstone'] == OBSOLETE

    # Assert replica of f1 on rse1 to be tombstoned
    assert get_replica(scope=mock_scope, name=fname1, rse_id=rse1.id)['tombstone'] == OBSOLETE

    # Assert replica r1 of f0 on rse2 to not be tombstoned
    assert get_replica(scope=mock_scope, name=fname0, rse_id=rse2.id)['tombstone'] is None


@pytest.mark.noparallel(reason='runs undertaker, which impacts other tests')
@pytest.mark.parametrize("core_config_mock", [{"table_content": [
    ('undertaker', 'purge_all_replicas', True)
]}], indirect=True)
@pytest.mark.parametrize("caches_mock", [{"caches_to_mock": [
    'rucio.core.config.REGION',
]}], indirect=True)
def test_removal_all_replicas2(rse_factory, root_account, mock_scope, core_config_mock, caches_mock):
    """ UNDERTAKER (CORE): Test the undertaker is setting Epoch tombstone on all the replicas. """
    rse1, rse1_id = rse_factory.make_posix_rse()
    rse2, rse2_id = rse_factory.make_posix_rse()

    set_local_account_limit(root_account, rse1_id, -1)
    set_local_account_limit(root_account, rse2_id, -1)

    nbdatasets = 1
    nbfiles = 5
    dsns1 = [{'name': did_name_generator('dataset'),
              'scope': mock_scope,
              'type': 'DATASET',
              'lifetime': -1} for _ in range(nbdatasets)]

    add_dids(dids=dsns1, account=root_account)

    replicas = list()
    for dsn in dsns1:
        files = [{'scope': mock_scope,
                  'name': did_name_generator('file'),
                  'bytes': 1,
                  'adler32': '0cc737eb'} for _ in range(nbfiles)]
        attach_dids(scope=mock_scope, name=dsn['name'], rse_id=rse1_id, dids=files, account=root_account)
        add_replicas(rse_id=rse2_id, files=files, account=root_account, ignore_availability=True)
        replicas += files

    add_rules(dids=dsns1, rules=[{'account': root_account, 'copies': 1, 'rse_expression': rse1, 'grouping': 'DATASET'}])
    add_rules(dids=dsns1, rules=[{'account': root_account, 'copies': 1, 'rse_expression': rse2, 'grouping': 'DATASET', 'lifetime': -86400}])

    # Clean the rules on rse2. Replicas are tombstoned with non Epoch
    rule_cleaner(once=True)
    for replica in replicas:
        assert get_replica(scope=replica['scope'], name=replica['name'], rse_id=rse2_id)['tombstone'] == OBSOLETE
    undertaker(once=True)
    undertaker(once=True)

    for replica in replicas:
        assert get_replica(scope=replica['scope'], name=replica['name'], rse_id=rse1_id)['tombstone'] == datetime(year=1970, month=1, day=1)
    for replica in replicas:
        assert get_replica(scope=replica['scope'], name=replica['name'], rse_id=rse2_id)['tombstone'] == datetime(year=1970, month=1, day=1)

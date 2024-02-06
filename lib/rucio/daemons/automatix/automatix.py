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

import logging
import random
import tempfile
from configparser import NoOptionError, NoSectionError
from datetime import datetime
from json import load
from os import remove, rmdir
from typing import Any
from collections.abc import Callable

from rucio.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.common.config import config_get, config_get_int, config_get_bool
from rucio.common.stopwatch import Stopwatch
from rucio.common.types import InternalScope
from rucio.common.utils import execute, generate_uuid
from rucio.core.monitor import MetricManager
from rucio.core.scope import list_scopes
from rucio.core.vo import map_vo
from rucio.daemons.common import Daemon, HeartbeatHandler

METRICS = MetricManager(module=__name__)


class Automatix(Daemon):
    def __init__(self, input_file: str = "/opt/rucio/etc/automatix.json", **_kwargs) -> None:
        """
        :param input_file: The input file where the parameters of the distribution are set
        """
        super().__init__(daemon_name="automatix", **_kwargs)
        self.input_file = input_file

    def _get_data_distribution(self) -> None:
        with open(self.input_file) as data_file:
            data = load(data_file)
        probabilities = {}
        probability = 0
        for key in data:
            probability += data[key]["probability"]
            probabilities[key] = probability
        for key in list(probabilities):
            probabilities[key] = float(probabilities[key]) / probability
        self.probabilities = probabilities
        self.data = data

    def _choose_element(self) -> float:
        rnd = random.uniform(0, 1)
        prob = 0
        for key in self.probabilities:
            prob = self.probabilities[key]
            if prob >= rnd:
                return self.data[key]
        return self.data[key]

    def _generate_file(fname: str, size: int, logger: "Callable[..., Any]" = logging.log) -> int:
        cmd = "/bin/dd if=/dev/urandom of=%s bs=%s count=1" % (fname, size)
        exitcode, out, err = execute(cmd)
        logger(logging.DEBUG, out)
        logger(logging.DEBUG, err)
        return exitcode

    @staticmethod
    def _generate_didname(metadata: dict, dsn: str, did_type: str) -> str:
        try:
            did_prefix = config_get("automatix", "did_prefix")
        except (NoOptionError, NoSectionError, RuntimeError):
            did_prefix = ""
        try:
            pattern = config_get("automatix", "%s_pattern" % did_type)
            separator = config_get("automatix", "separator")
        except (NoOptionError, NoSectionError, RuntimeError):
            return generate_uuid()
        fields = pattern.split(separator)
        file_name = ""
        for field in fields:
            if field == 'date':
                field_str = str(datetime.utcnow().date())
            elif field == 'did_prefix':
                field_str = did_prefix
            elif field == "dsn":
                field_str = dsn
            elif field == "uuid":
                field_str = generate_uuid()
            elif field == "randint":
                field_str = str(random.randint(0, 100000))
            else:
                field_str = metadata.get(field, None)
                if not field_str:
                    field_str = str(random.randint(0, 100000))
            file_name = "%s%s%s" % (file_name, separator, field_str)
        len_separator = len(separator)
        return file_name[len_separator:]

    def _run_once(self, heartbeat_handler: "HeartbeatHandler", **_kwargs) -> tuple[bool, Any]:
        _, _, logger = heartbeat_handler.live()
        must_sleep = False

        try:
            rses = [
                s.strip() for s in config_get("automatix", "rses").split(",")
            ]  # TODO use config_get_list
        except (NoOptionError, NoSectionError, RuntimeError):
            logging.log(
                logging.ERROR,
                "Option rses not found in automatix section. Trying the legacy sites option",
            )
            try:
                rses = [
                    s.strip() for s in config_get("automatix", "sites").split(",")
                ]  # TODO use config_get_list
                logging.log(
                    logging.WARNING,
                    "Option sites found in automatix section. This option will be deprecated soon. Please update your config to use rses.",
                )
            except (NoOptionError, NoSectionError, RuntimeError):
                logger(logging.ERROR, "Could not load sites from configuration")
                must_sleep = True
                return must_sleep, None

        set_metadata = config_get_bool(
            "automatix", "set_metadata", raise_exception=False, default=True
        )
        dataset_lifetime = config_get_int(
            "automatix", "dataset_lifetime", raise_exception=False, default=0
        )
        account = config_get("automatix", "account", raise_exception=False, default="root")
        scope = config_get("automatix", "scope", raise_exception=False, default="test")
        client = Client(account=account)
        vo = map_vo(client.vo)
        filters = {"scope": InternalScope("*", vo=vo)}
        scopes = list_scopes(filter_=filters)
        if InternalScope(scope, vo=vo) not in scopes:
            logger(logging.ERROR, "Scope %s does not exist. Exiting", scope)
            must_sleep = True
            return must_sleep, None

        logger(logging.INFO, "Getting data distribution")
        self._get_data_distribution()
        logger(logging.DEBUG, "Probabilities %s", self.probabilities)

        cycle_stopwatch = Stopwatch()
        for rse in rses:
            stopwatch = Stopwatch()
            _, _, logger = heartbeat_handler.live()
            tmpdir = tempfile.mkdtemp()
            logger(logging.INFO, "Running on RSE %s", rse)
            dic = self._choose_element()
            metadata = dic["metadata"]
            try:
                nbfiles = dic["nbfiles"]
            except KeyError:
                nbfiles = 2
                logger(
                    logging.WARNING, "No nbfiles defined in the configuration, will use 2"
                )
            try:
                filesize = dic["filesize"]
            except KeyError:
                filesize = 1000000
                logger(
                    logging.WARNING,
                    "No filesize defined in the configuration, will use 1M files",
                )
            dsn = Automatix._generate_didname(metadata, None, "dataset")
            fnames = []
            lfns = []
            physical_fnames = []
            files = []
            for _ in range(nbfiles):
                fname = Automatix._generate_didname(metadata=metadata, dsn=dsn, did_type="file")
                lfns.append(fname)
                logger(logging.INFO, "Generating file %s in dataset %s", fname, dsn)
                physical_fname = "%s/%s" % (tmpdir, "".join(fname.split("/")))
                physical_fnames.append(physical_fname)
                Automatix._generate_file(physical_fname, filesize, logger=logger)
                fnames.append(fname)
                file_ = {
                    "did_scope": scope,
                    "did_name": fname,
                    "dataset_scope": scope,
                    "dataset_name": dsn,
                    "rse": rse,
                    "path": physical_fname,
                }
                if set_metadata:
                    file_["dataset_meta"] = metadata
                    if dataset_lifetime:
                        file_["dataset_meta"]["lifetime"] = dataset_lifetime
                files.append(file_)
            logger(logging.INFO, "Upload %s:%s to %s", scope, dsn, rse)
            upload_client = UploadClient(client)
            ret = upload_client.upload(files)
            if ret == 0:
                logger(logging.INFO, "%s sucessfully registered" % dsn)
                METRICS.counter(name="addnewdataset.done").inc()
                METRICS.counter(name="addnewfile.done").inc(nbfiles)
                METRICS.timer(name='datasetinjection').observe(stopwatch.elapsed)
            else:
                logger(logging.INFO, "Error uploading files")
            for physical_fname in physical_fnames:
                remove(physical_fname)
            rmdir(tmpdir)
        logger(
            logging.INFO,
            "It took %f seconds to upload one dataset on %s",
            cycle_stopwatch.elapsed,
            str(rses),
        )
        must_sleep = True
        return must_sleep, None

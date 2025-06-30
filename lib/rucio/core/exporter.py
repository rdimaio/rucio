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

from typing import TYPE_CHECKING, Any

from rucio.common.constants import DEFAULT_VO
from rucio.core import distance as distance_module
from rucio.core import rse as rse_module
from rucio.db.sqla.session import transactional_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@transactional_session
def export_rses(vo: str = DEFAULT_VO, *, session: "Session") -> dict[str, dict[str, Any]]:
    """
    Export RSE data.

    :param vo: The VO to export.
    :param session: database session in use.
    """
    data = {}
    for rse in rse_module.list_rses(filters={'vo': vo}, session=session):
        rse_id = rse['id']
        data[rse_id] = rse_module.export_rse(rse_id, session=session)

    return data


@transactional_session
def export_data(vo: str = DEFAULT_VO, distance: bool = True, *, session: "Session") -> dict[str, Any]:
    """
    Export data.

    :param vo: The VO to export.
    :param distance: To enable the reporting of distance.
    :param session: database session in use.
    """
    if distance:
        data = {
            'rses': export_rses(vo=vo, session=session),
            'distances': distance_module.export_distances(vo, session=session)
        }
    else:
        data = {
            'rses': export_rses(vo=vo, session=session)
        }
    return data

from typing import List

from jetavator.config import ComputeServiceConfig
import wysdom

DatabricksLibraryConfig = wysdom.SchemaDict(wysdom.SchemaAnything())


class DatabricksConfig(ComputeServiceConfig):

    type: str = wysdom.UserProperty(wysdom.SchemaConst('remote_databricks'))
    host: str = wysdom.UserProperty(str)
    cluster_id: str = wysdom.UserProperty(str)
    org_id: str = wysdom.UserProperty(str)
    token: str = wysdom.UserProperty(str)
    libraries: List[DatabricksLibraryConfig] = wysdom.UserProperty(
        wysdom.SchemaArray(DatabricksLibraryConfig), default=[{}])

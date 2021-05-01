from jetavator.config import ComputeServiceConfig
import wysdom


class LocalDatabricksConfig(ComputeServiceConfig):
    type: str = wysdom.UserProperty(wysdom.SchemaConst('local_databricks'))

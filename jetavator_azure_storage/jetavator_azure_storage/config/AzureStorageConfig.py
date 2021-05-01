from jetavator.config import ServiceConfig
import wysdom


class AzureStorageConfig(ServiceConfig):
    type = wysdom.UserProperty(wysdom.SchemaConst('azure_storage'))
    name = wysdom.UserProperty(str)
    account_name = wysdom.UserProperty(str)
    account_key = wysdom.UserProperty(str)
    blob_container_name = wysdom.UserProperty(str)

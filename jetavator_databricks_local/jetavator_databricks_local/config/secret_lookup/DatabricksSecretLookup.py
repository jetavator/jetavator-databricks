from jetavator.config.secret_lookup import SecretLookup

from lazy_property import LazyProperty


class DatabricksSecretLookup(SecretLookup, register_as='databricks'):

    @LazyProperty
    def dbutils(self):
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]

    def lookup_secret(self, secret_name):
        return self.dbutils.secrets.get(scope="jetavator", key=secret_name)

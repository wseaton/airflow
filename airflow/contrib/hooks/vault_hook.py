import os
import hvac

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class VaultHook(BaseHook, LoggingMixin):
    """
    Hook to interact with Hashicorp Vault
    """

    def __init__(self, vault_conn_id="vault_default", tls=True):
        """
        Prepares hook to connect to Vault instance via Token
        authentication.

        Defaults to TLS, accepts `certfile` and `keyfile` in the extra
        field to use client-side certificate authentication.

        :param conn_id:     name of the connection containing Vault connection details.
        :param tls:         whether or not to use TLS for the connection,
                            defaults to True.
        """
        self.vault_conn_id = vault_conn_id
        self.client = None
        conn = self.get_connection(self.vault_conn_id)
        self.host = conn.host
        self.port = int(conn.port)
        self.token = conn.password or None

        self.cert_tuple = (
            conn.extra_dejson.get("certfile"),
            conn.extra_dejson.get("keyfile"),
        )

        self.tls = tls

    def get_conn(self):
        """
        Returns a Vault connection.
        """

        if not self.client:
            self.log.debug(
                'generating Vault client for conn_id "%s" on %s:%s',
                self.vault_conn_id,
                self.host,
                self.port,
            )
        try:
            scheme = "http" if not self.tls else "https"
            url = "{scheme}://{host}:{port}"

            self.client = hvac.Client(
                url=url.format(scheme=scheme, host=self.host, port=self.port),
                token=self.token,
                cert=self.cert_tuple,
            )
        except Exception as e:
            raise AirflowException(
                "Failed to connect to vault, error: {error}".format(error=str(e))
            )

        return self.client

    def read(self, key):
        """
        Read a secret from the vault.

        :param key:     the vault key to read from
        """
        return self.get_conn().read(key)

    def write(self, key, **kwargs):
        """
        Write to a vault location.
            
        :param key:     the vault key to write to
        :param kwargs:  passthrough arguments to hvac client
        """
        return self.get_conn().write(key, **kwargs)

    def delete(self, key):
        """
        Delete a key at a specified location.
        """
        return self.get_conn().delete(key)

    @property
    def is_authenticated(self):
        return self.get_conn().is_authenticated()

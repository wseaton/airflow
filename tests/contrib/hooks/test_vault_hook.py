import unittest

from airflow import configuration
from airflow.contrib.hooks.vault_hook import VaultHook
from airflow.models.connection import Connection
from airflow.utils import db


class TestVaultHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            Connection(
                conn_id='vault_test', conn_type='vault',
                host='vault', port='8200', password='airflow'
            )
        )

    def test_actual_read_and_write(self):
        hook = VaultHook(vault_conn_id='vault_test', tls=False)
        client = hook.get_conn()

        self.assertTrue(client.write('secret/foo', bar='bar'), 'Vault WRITE works.')
        self.assertEqual(client.read('secret/foo')['data']['bar'], 'bar', 'Vault READ works.')
        
        client.delete('secret/foo')
        self.assertFalse(client.read('secret/foo'))

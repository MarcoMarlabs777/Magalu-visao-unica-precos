from google.auth.transport.requests import Request
from google.oauth2 import id_token
from sness.etl import CHECKPOINT_CLIENT_ID, CHECKPOINT_SEGA_URL
import requests
import json
from sness.settings import cloud_config


class Checkpoint:
    def __init__(
        self,
        environment,
        zone,
        namespace,
        dataset,
        cloud_environment=cloud_config.CLOUD_ENVIRONMENT,
    ):
        self.environment = environment
        self.namespace = namespace
        self.zone = zone
        self.dataset = dataset
        self.last_value = None
        self.check_column = None
        self.cloud_environment = cloud_environment

    def __enter__(self):
        return self._get_checkpoint()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            raise Exception(
                f"Error while executing the code: '{exc_val}' "
                f"The checkpoint will not be updated."
            )
        elif self.last_value and self.check_column:
            post_result = self._set_checkpoint()
            if post_result:
                raise Exception(
                    f"Something went wrong when updating "
                    f"the checkpoint: {post_result}"
                )
        else:
            raise ValueError(
                "The variables 'last_value' "
                "and 'check_column' must be different than None"
            )

    def _get_checkpoint(self):
        sega = self._sega_request(method='GET')
        if sega:
            self.check_column = sega.get("check_column", "")
            self.last_value = sega.get("last_value", "")
        return self

    def _set_checkpoint(self):
        sega_data = {
            "key": f"{self.environment}_"
            f"{self.zone}_{self.namespace}_{self.dataset}",
            "check_column": self.check_column,
            "last_value": self.last_value,
            "cloud_environment": self.cloud_environment,
        }
        return self._sega_request(method='POST', data=sega_data)

    def _sega_request(self, method='GET', data=None):
        url = CHECKPOINT_SEGA_URL
        client_id = CHECKPOINT_CLIENT_ID
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._get_token(client_id)}',
        }
        param = (
            f"checkpoint={self.environment}_"
            f"{self.zone}_{self.namespace}_{self.dataset}&"
            f"cloud_environment={self.cloud_environment}"
        )
        if method == 'GET':
            response = requests.get(url, params=param, headers=headers)
            return response.json()
        else:
            response = requests.post(
                url, params=param, data=json.dumps(data), headers=headers
            )
            return response.json()

    def _get_token(self, client_id):
        return id_token.fetch_id_token(Request(), client_id)

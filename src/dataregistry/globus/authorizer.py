import globus_sdk
from .globus_info import CLIENT_ID, NERSC_COLLECTION_ID, APP_NAME

__all__ = ['Authorizer']

def _make_config():
    return globus_sdk.GlobusAppConfig(
        request_refresh_tokens=True,
        )


def _make_scopes():
    a_dict = {
        "transfer.api.globus.org": globus_sdk.TransferClient.scopes.all,
        NERSC_COLLECTION_ID: f"https://auth.globus.org/scopes/{NERSC_COLLECTION_ID}/data_access"
    }
    return a_dict


class Authorizer(globus_sdk.UserApp):
    def __init__(self,
                 client_id=CLIENT_ID,
                 fixed_endpoint_id=NERSC_COLLECTION_ID,
                 scope_requirements=_make_scopes()):
        '''
        Parameters
        ----------
        client_id : string          Id for registered native app
        fixed_endpoint_id : string  Collection id for dataregistry file storage
        scope_requirements : dict(str, str or Scope)
        '''

        super().__init__(
            APP_NAME,
            client_id=client_id,
            config=_make_config(),
        )
        self._client_id = client_id
        self._ep_id = fixed_endpoint_id

        self.add_scope_requirements(scope_requirements)

    def authorize(self, force=False):
        if force or self.login_required():
            self.login(force=True)

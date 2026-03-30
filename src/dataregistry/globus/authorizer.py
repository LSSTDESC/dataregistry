import globus_sdk
from globus_sdk.token_storage import JSONTokenStorage
from .globus_info import CLIENT_ID, NERSC_COLLECTION_ID, TOKEN_FILE


def _make_config():
    return globus_sdk.GlobusAppConfig(
        request_refresh_tokens=True,
        token_storage=JSONTokenStorage(TOKEN_FILE),
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
                 token_file=TOKEN_FILE,
                 scope_requirements=_make_scopes()):
        '''
        Parameters
        ----------
        client_id : string          Id for registered native app
        fixed_endpoint_id : string  Collection id for dataregistry file storage
        token_file :   string       Path to file where tokens will be kept
        scope_requirements : dict(str, str or Scope)
        '''

        super().__init__(
            app_name="dr_authorizer_app",
            client_id=client_id,
            config=_make_config(),
        )
        self._client_id = client_id
        self._ep_id = fixed_endpoint_id
        self._token_file = token_file

        self.add_scope_requirements(scope_requirements)

    def authorize(self, force=False):
        if force or self.login_required():
            self.login(force=True)

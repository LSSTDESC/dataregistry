from pathlib import Path
from globus_sdk import TransferClient, TransferData, UserApp, GlobusAppConfig
from .globus_info import NERSC_COLLECTION_ID, CLIENT_ID

__all__ = ["transfer_from_NERSC", "transfer_NERSC", "transfer_NERSC_auth"]


def transfer_from_NERSC(src_path, dest_path, dest_collection_id,
                        label="transfer from NERSC", logger=None):
    config = GlobusAppConfig(request_refresh_tokens=True)
    with UserApp("dreg_transfer", client_id=CLIENT_ID, config=config) as dr_app:
        with TransferClient(app=dr_app) as trans_client:
            trans_client.add_app_data_access_scope(NERSC_COLLECTION_ID)

            # NOTE: if dest_collection_id is not NERSC we might have to
            # something about data access scope - or not.  Generally don't
            # have to for a personal device.

            tdata = TransferData(NERSC_COLLECTION_ID,
                                 dest_collection_id,
                                 label=label,
                                 )
            # Are we transferring a directory?
            is_dir = Path(src_path).is_dir()
            # if is_dir:  # may have to add src base to dest

            tdata.add_item(src_path, dest_path, recursive=is_dir)
            transfer_result = trans_client.submit_transfer(tdata)

    if logger:
        logger.info("task_id = ", transfer_result["task_id"])
    else:
        print("task_id = ", transfer_result["task_id"])
    return transfer_result


def transfer_NERSC(src_path, dest_path, label="internal NERSC transfer",
                   logger=None):

    return transfer_from_NERSC(src_path, dest_path, NERSC_COLLECTION_ID,
                               label=label, logger=logger)


def _make_scopes():
    a_dict = {
        "transfer.api.globus.org": TransferClient.scopes.all,
        NERSC_COLLECTION_ID: f"https://auth.globus.org/scopes/{NERSC_COLLECTION_ID}/data_access"
    }
    return a_dict


def transfer_NERSC_auth(logger=None, force=False):
    '''
    Benign operation with transfer client to be sure authorization is in
    place for future operations
    '''

    config = GlobusAppConfig(request_refresh_tokens=True)
    with UserApp("dreg_transfer", client_id=CLIENT_ID, config=config,
                 scope_requirements=_make_scopes()) as dr_app:
        if force or dr_app.login_required():
            dr_app.login(force=True)

        # with TransferClient(app=dr_app) as trans_client:
        #     trans_client.add_app_data_access_scope(NERSC_COLLECTION_ID)
        #     ls_result = client.operation_ls(NERSC_COLLECTION_ID)

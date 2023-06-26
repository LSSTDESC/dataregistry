import os
import git

__all__ = ["get_git_info"]

def get_git_info(pkg_root):
    '''
    Parameters
    pkg_root   string    root directory of a git repo

    Returns
    git_hash    string   current git hash
    is_clean    boolean

    '''
    repo = git.Repo(pkg_root)
    has_uncommitted = repo.is_dirty()
    has_untracked = (len(repo.untracked_files) > 0)
    return repo.commit().hexsha, not (has_uncommitted or has_untracked)

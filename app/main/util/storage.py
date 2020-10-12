import os
from pathlib import Path

from flask import current_app as app


def get_path(folder, filename, as_folder=False, create=False):
    """Creates the path for a file under UPLOAD FOLDER"""
    folder_path = Path(folder)
    if create:
        folder_path.mkdir(parents=True, exist_ok=True)
    if as_folder:
        return str(folder_path)
    else:
        return str(folder_path.joinpath(filename))


def get_export_path(filename):
    export_folder = os.path.join(app.config['UPLOAD_FOLDER'], 'exports')
    return get_path(export_folder, filename)

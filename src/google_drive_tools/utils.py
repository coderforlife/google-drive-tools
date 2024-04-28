import os
import sys
import pickle
import argparse
from typing import Optional
from urllib.parse import urlparse

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("Failed to import Google APIs, make sure they are installed:", file=sys.stderr)
    print("    pip3 install google-api-python-client google-auth-oauthlib", file=sys.stderr)
    sys.exit(1)


MIME_TYPE_DOC = ('application/vnd.google-apps.document', 'application/vnd.google-apps.kix')
MIME_TYPE_SHEET = ('application/vnd.google-apps.spreadsheet', 'application/vnd.google-apps.ritz')
MIME_TYPE_FOLDER = 'application/vnd.google-apps.folder'
MIME_TYPE_SHORTCUT = 'application/vnd.google-apps.shortcut'
FOLDER_OR_SHORTCUT = f"(mimeType='{MIME_TYPE_FOLDER}' or " \
    f"(mimeType='{MIME_TYPE_SHORTCUT}' and shortcutDetails.targetMimeType='{MIME_TYPE_FOLDER}'))"


def get_credentials(scopes: list[str],
                    token_name: str = 'token.pickle',
                    cred_name: str = 'credentials.json',
                    config_dir_name: str = 'google-app-credentials'):
    """
    Gets Google API credentials. The credentials are stored in a token file that is created
    automatically when the authorization flow completes for the first time. If the token file
    doesn't exist, the user is prompted to log in and authorize the application. The credentials
    are then saved to the token file for future use.

    Possible locations for the token and credentials files are:
        * Environment variables GOOGLE_APP_TOKEN and GOOGLE_APP_CREDENTIALS
        * Current working directory
        * This file's directory
        * User's config directory (default is ~/.config/<config_dir_name>)
    """
    file_dir = os.path.dirname(__file__) if '__file__' in globals() else os.getcwd()
    config_dir = os.path.expanduser(os.environ.get('XDG_CONFIG_HOME', '~/.config/'))
    config_dir = os.path.join(config_dir, config_dir_name)
    token_paths = (
        os.environ.get('GOOGLE_APP_TOKEN'),  # from environment variable
        token_name,  # current working directory
        os.path.join(file_dir, token_name),  # this file's directory
        os.path.join(config_dir, token_name),  # user's config directory
    )
    cred_paths = (
        os.environ.get('GOOGLE_APP_CREDENTIALS'),  # from environment variable
        cred_name,  # current working directory
        os.path.join(file_dir, cred_name),  # this file's directory
        os.path.join(config_dir, cred_name),  # user's config directory
    )

    # token.pickle stores the user's access and refresh tokens, and is created
    # automatically when the authorization flow completes for the first time.
    loaded_token_path = None
    creds = None
    for token_path in token_paths:
        if token_path is not None and os.path.exists(token_path):
            with open(token_path, 'rb') as token_file:
                creds = pickle.load(token_file)
            if creds and creds.valid: return creds
            loaded_token_path = token_path

    if creds and creds.expired and creds.refresh_token:
        # Refresh the credentials with a new login and save to the file that was loaded
        creds.refresh(Request())
        with open(loaded_token_path, 'wb') as token_file:
            pickle.dump(creds, token_file)
        return creds

    # Start the login process from scratch
    for i, cred_path in enumerate(cred_paths):
        if cred_path is not None and os.path.exists(cred_path):
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, scopes)
            creds = flow.run_local_server(port=0)

            # Save the credentials for the next run (prefer same directory as cred file)
            for token_path in (token_paths[0], token_paths[i]) + token_paths[1:]:
                if token_path is not None:
                    if os.path.dirname(token_path) != '':
                        os.makedirs(os.path.dirname(token_path), exist_ok=True)
                    try:
                        with open(token_path, 'wb') as token:
                            pickle.dump(creds, token)
                    except OSError:
                        pass

            # Return the drive credentials
            return creds

    raise FileNotFoundError("Credentials or token file not found")


def get_services(services: tuple[tuple[str, str]], scopes: tuple[str],
                 token_name='token.pickle', cred_name='credentials.json',
                 config_dir_name='google-app-credentials'):
    """Gets the specific API services (from their name and version)."""
    credentials = get_credentials(scopes, token_name, cred_name, config_dir_name)
    return [build(name, version, credentials=credentials) for name, version in services]


def escape(filename: str) -> str:
    """Escapes a file name for use in a Google Drive query."""
    return filename.replace('\\', '\\\\').replace("'", "\\'")


def get_resolve_shortcut(drive, fileId: str, fields: str) -> str:
    """
    Gets information about a file, resolving shortcuts if needed. It is recommended to include 'id'
    in the fields so that you have access to the updated ID if needed.

    If the fields contains 'parents', then a key 'origParents' is added to the file with the
    original parents if the file is a shortcut.
    """
    sep_fields = fields.split(',')
    mod_fields = fields
    if 'mimeType' not in sep_fields: mod_fields += ',mimeType'
    if 'shortcutDetails' not in sep_fields: mod_fields += ',shortcutDetails'
    file = drive.files().get(fileId=fileId, fields=mod_fields, supportsAllDrives=True).execute()
    if file.get('mimeType') == MIME_TYPE_SHORTCUT:
        fileId = file.get('shortcutDetails').get('targetId')
        orig_parents = file.get('parents')
        file = drive.files().get(fileId=fileId, fields=fields, supportsAllDrives=True).execute()
        if 'parents' in sep_fields:
            file['origParents'] = orig_parents
    return file


def get_folder_id(drive, folder: str, make_dirs: bool, parent_id: Optional[str] = None) -> str:
    """
    Determines the folder ID. The given folder can be an id or a path. If the folder doesn't exist
    and make_dirs is True, it will create the folders in the path. The parent is used to determine
    the parent folder if the path is relative.
    """
    try:
        dest_id = file_id_exists(drive, folder)
    except argparse.ArgumentTypeError:
        dest_id = find_folder(drive, folder, make_dirs, parent_id if parent_id else 'root')
    return dest_id


def find_folder(drive, path: str, make_dirs: bool = False, parent_id: str = 'root') -> str:
    """
    Finds the folder ID for the given path. If the path doesn't exist and make_dirs is True, it
    will create the folders in the path. The path can be absolute (starting with '/') or relative
    to the parent_id (which defaults to 'root' though).

    File paths are separated by '/' and can contain '.' and '..' to represent the current and
    parent directories respectively. This supports both folders and shortcuts to folders.

    Returns the ID of the folder.
    """
    # TODO: allow escaping \ and / in the path
    files = drive.files()

    path = path.replace('\\', '/')
    current = 'root' if path.startswith('/') else parent_id
    parts = path.strip('/').split('/')
        
    for part in parts:
        if part == '': # skip empty parts
            continue
        elif part == '..': # go up one level
            response = files.get(fileId=current, fields='parents', supportsAllDrives=True).execute()
            current = response.get('parents')[0]
        elif part != '.': # go down one level
            response = files.list(
                q=f"'{current}' in parents and name='{escape(part)}' and trashed=false and " +
                    FOLDER_OR_SHORTCUT,
                fields='files(id,mimeType,shortcutDetails)',
                includeItemsFromAllDrives=True, supportsAllDrives=True).execute()
            folders = response.get('files', [])
            if not folders:
                if not make_dirs:
                    raise FileNotFoundError(f"Folder '{part}' not found in '{current}'")
                folder = files.create(body={
                    'name': part, 'mimeType': MIME_TYPE_FOLDER, 'parents': [current]
                }, fields='id', supportsAllDrives=True).execute()
            else:
                folder = folders[0]
            if folder.get('mimeType') == MIME_TYPE_SHORTCUT:
                current = folder.get('shortcutDetails').get('targetId')
            else:
                current = folder.get('id')
    
    return current


def get_file_id(drive, name: str, parent_id: str, mimeType: Optional[str] = None) -> Optional[str]:
    """Gets the ID of a file with the given name, parent directory, and optional mime type."""
    condition = f"name='{escape(name)}' and '{parent_id}' in parents and trashed=false"
    if mimeType: condition += f" and mimeType='{mimeType}'"
    response = drive.files().list(q=condition, fields='files(id)',
                                  includeItemsFromAllDrives=True, supportsAllDrives=True).execute()
    files = response.get('files', [])
    return files[0].get('id') if files else None


def file_exists(drive, name: str, parent_id: str, mimeType: Optional[str] = None) -> bool:
    """Checks if a given file name exists in a given directory with an optional given mime type."""
    return get_file_id(drive, name, parent_id, mimeType) is not None


def file_id_check(value: str) -> str:
    """
    Checks if a command line argument is a valid Google Document ID. If given as a URL, this
    attempts to extact the document ID from the URL.
    """
    value = value.strip()
    if ':' in value:
        try:
            url = urlparse(value)
            if url.query.startswith('id='):
                value = url.query[3:]
                if '&' in value: value = value[:value.index('&')]
            else:
                value = [part for part in url.path.split('/') if len(part) >= 25][-1]
        except (ValueError, IndexError):
            raise argparse.ArgumentTypeError('Invalid document id')
    elif len(value) < 25:
        raise argparse.ArgumentTypeError('Invalid document id')
    legal = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-'
    if any(ch not in legal for ch in value):
        raise argparse.ArgumentTypeError('Invalid document id')
    return value


def file_id_exists(drive, value: str) -> str:
    """
    Same as file_id_check() but also checks that the file exists in Google Drive.
    """
    value = file_id_check(value)
    drive.files().get(fileId=value, fields='name', supportsAllDrives=True).execute()
    return value


def copy_file(drive, file_id: str, file_name: str, dest_id: Optional[str] = None) -> str:
    """Copies a file and moves it to the destination folder if one is given."""
    files = drive.files()

    # Copy the file for the group
    copied = files.copy(fileId=file_id, body={'name': file_name},
                        fields='id,parents', supportsAllDrives=True).execute()
    new_id = copied.get('id')

    # Move the file to the destination folder
    if dest_id:
        files.update(fileId=new_id, addParents=dest_id, supportsAllDrives=True,
                     removeParents=','.join(copied.get('parents'))).execute()

    return new_id


def get_all_pages(func, key: str, **kwargs: dict) -> list:
    """
    Get all pages from an API list() or similar function. The function is the function to call
    (e.g. drive.files().list), the key is the key in the response dictionary containing the list
    of values (e.g. 'files'), and all other arguments are passed to the function call.
    """
    response = func(**kwargs).execute()
    lst = response.get(key, [])
    while 'nextPageToken' in response:
        try:
            perms = func(pageToken=perms['nextPageToken'], **kwargs).execute()
            lst.extend(perms.get(key, []))
        except HttpError as error:
            print(f"Error getting next page of {key}: {error}")
            break
    return lst

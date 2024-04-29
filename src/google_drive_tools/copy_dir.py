#!/usr/bin/env python3
"""
Creates a copy of a directory with recursion.
"""

import os
import argparse
from enum import Enum
from functools import partial
from typing import Optional
from dataclasses import dataclass
import dataclasses
from datetime import datetime, timedelta

import pathspec

from .utils import get_services, file_id_exists, get_file_id, file_exists, get_folder_id, escape
from .utils import copy_file, get_all_pages, get_resolve_shortcut
from .utils import MIME_TYPE_FOLDER, MIME_TYPE_SHORTCUT


class Shortcut(Enum):
    """Options for copying a shortcut."""
    # TODO: should there be support for re-shortcutting things that are also being copied?
    AS_IS = 0 # Copy the shortcut itself
    FOLLOW_DIR = 1  # Follow to the directory the shortcut points to
    FOLLOW_FILE = 2 # Follow to the file the shortcut points to
    FOLLOW = 3  # Follow to the directory or file the shortcut points to


class ConflictMode(Enum):
    """Options for handling conflicts when copying a file."""
    NEVER = -1 # Never overwrite or merge files - raise an error right off the bat
    KEEP_EXISTING = 0  # Keep the existing file when there is a conflict
    OVERWRITE = 1  # Overwrite the file when there is a conflict
    KEEP_BOTH = 2  # Keep both files when there is a conflict by appending a number
    INTERACTIVE = 3  # Ask what to do when there is a conflict


@dataclass
class Options:
    """Options for copying a directory."""
    verbose: bool = False
    mode: ConflictMode = ConflictMode.NEVER
    shortcuts: Shortcut = Shortcut.AS_IS
    copy_perms: bool = False  # cannot transfer ownership, current user will always be owner
    send_emails: bool = False
    copy_comments: bool = False  # cannot copy ownership of comments, adds extra text to comments
                                 # about ownership and when originally created
    match: Optional[pathspec.PathSpec] = None


@dataclass
class Entry:
    """Used in a stack during recursive processing"""
    src_id: str
    name: str
    id: Optional[str] = None


@dataclass
class State:
    """State for copying a directory."""
    options: Options
    dest_id: str
    stack: list[Entry] = dataclasses.field(default_factory=list)

    @property
    def indent(self) -> str: return '  '*(len(self.stack)+1)

    @property
    def path(self) -> str: return os.path.join(*(entry.name for entry in self.stack))


def copy_dir(
        drive, dir_id: str, dest: str, copy_name: Optional[str] = None,
        make_parent_dirs: bool = False, options: Options = Options()):
    """
    Uses the Google Drive services to copy a directory recursively.

    :param drive: The Google Drive service object.
    :param dir_id: The ID of the directory to copy.
    :param dest: The destination directory to copy into. Can be '.' for the same directory.
    :param copy_name: The name to give the copied directory or None for the same name.
    :param make_parent_dirs: Whether to create the destination directory if it doesn't exist.
    :param options: Options for copying the directory.
    """
    # Get info about the directory
    response = get_resolve_shortcut(drive, dir_id, fields='id,name,mimeType,parents')
    dir_id, name, mime_type = response.get('id'), response.get('name'), response.get('mimeType')
    if mime_type != MIME_TYPE_FOLDER:
        raise ValueError(f"File {dir_id} is not a folder")
    copy_name = copy_name or name

    # Determine destination folder ID
    dest_id = get_folder_id(drive, dest, make_parent_dirs, response.get('parents')[0])

    # Make sure destination does not already exist
    if options.mode == ConflictMode.NEVER and \
        file_exists(drive, copy_name, dest_id, MIME_TYPE_FOLDER):
        raise ValueError(f"Destination folder {copy_name} already exists in {dest_id}")

    # Copy the directory
    if options.verbose: print(f"Copying directory {name} ({dir_id}) to {copy_name} (in {dest_id})")
    __copy_dir(drive, dir_id, copy_name, State(options, dest_id))


def get_drive_service():
    """Gets the Google Drive API services."""
    return get_services((('drive', 'v3'),),
                        ('https://www.googleapis.com/auth/drive',),
                        'dup-and-share-token.pickle', 'dup-and-share-credentials.json')


def __copy_dir(drive, src_id: str, name: str, state: State) -> None:
    """
    Copy a directory recursively. This does the bulk of the work for copy_dir().
    The creation of directories is done lazily (they are not created until needed). This is
    required for file matching to work properly. It also means that empty directories are not
    created.
    """
    # Update the stack
    state.stack.append(Entry(src_id, name))

    # List the contents of the directory
    files = get_all_pages(drive.files().list, 'files',
                          q=f"'{escape(src_id)}' in parents and trashed=false",
                          fields='files(id,name,mimeType,shortcutDetails)', supportsAllDrives=True)

    # Copy the contents of the directory
    for file in files:
        file_id, name, mime_type = file.get('id'), file.get('name'), file.get('mimeType')
        if mime_type == MIME_TYPE_FOLDER:
            __copy_dir(drive, file_id, name, state)
        elif mime_type == MIME_TYPE_SHORTCUT:
            __copy_shortcut(drive, file, state)
        else:
            __copy_file(drive, file, state)

    entry = state.stack.pop()  # done with this directory
    if entry.id is not None:  # directory was created, need to process it
        __process(drive, src_id, entry.id, state.options)


def __ensure_dirs(drive, state: State) -> str:
    """
    Ensure directories exist for the current stack. This is done lazily so that file matching
    can work properly. Returns the ID of the last directory in the stack.
    """
    if state.stack[-1].id is None:
        verbose = state.options.verbose
        dest_id = state.dest_id
        indent = '  '
        for entry in state.stack:
            if entry.id is None:
                name = entry.name
                if state.options.mode != ConflictMode.NEVER:
                    entry.id = get_file_id(drive, name, dest_id, MIME_TYPE_FOLDER)
                    if entry.id and verbose:
                        print(f"{indent}Merging into existing directory {name}")
                if not entry.id:
                    if verbose: print(f"{indent}Creating directory {name}")
                    entry.id = drive.files().create(body={
                        'name': name, 'mimeType': MIME_TYPE_FOLDER, 'parents': [dest_id]
                    }, fields='id', supportsAllDrives=True).execute().get('id')
            dest_id = entry.id
            indent += '  '
    return state.stack[-1].id


def __copy_shortcut(drive, file: dict, state: State) -> None:
    """
    Copy a shortcut described by the file dictionary to the directory at the destination id. The
    new name of the file is given along with copying options and the indentation for verbose
    outputs. Returns the new file id.
    """
    name = file.get('name')
    details = file.get('shortcutDetails')
    target_id, mime_type = details.get('targetId'), details.get('targetMimeType')
    is_dir = mime_type == MIME_TYPE_FOLDER

    # Check if the target is a directory or file and if it should be copied
    if is_dir and state.options.shortcuts in (Shortcut.FOLLOW_DIR, Shortcut.FOLLOW):
        if all(target_id != entry.src_id for entry in state.stack):  # check for recursion
            return __copy_dir(drive, target_id, name, state)
    elif not is_dir and state.options.shortcuts in (Shortcut.FOLLOW_FILE, Shortcut.FOLLOW):
        return __copy_file(drive, {'id': target_id, 'name': name, 'mimeType': mime_type}, state)

    # Copy the shortcut itself
    __copy_file(drive, file, state,
                dataclasses.replace(state.options, copy_comments=False), 'shortcut')


def __copy_file(drive, file: dict, state: State,
                options: Optional[Options] = None, type_: str = 'file') -> None:
    """
    Copy a file at the file id to the directory at the destination id. The new name of the file
    is given along with copying options and the indentation for verbose outputs. Returns the new
    file id.
    """
    if options is None: options = state.options
    show_msg = options.verbose
    mode = options.mode
    indent = state.indent

    # Get the file info
    file_id, name, mime_type = file.get('id'), file.get('name'), file.get('mimeType')

    # Check if the file matches the patterns
    if state.options.match and not state.options.match.match_file(os.path.join(state.path, name)):
        if show_msg: print(f"{indent}Skipping {name}")
        return

    # Make sure the destination folder exists
    dest_id = __ensure_dirs(drive, state)

    # Check for conflicts
    if mode != ConflictMode.NEVER and (old_id := get_file_id(drive, name, dest_id, mime_type)):
        if mode == ConflictMode.INTERACTIVE:
            mode = __get_interactive_conflict_mode(name, mime_type)
        
        if mode == ConflictMode.KEEP_EXISTING:
            # merge/skip/keep existing
            if show_msg: print(f"{indent}Skipping {type_} {name} (already exists)")
            return
        
        elif mode == ConflictMode.KEEP_BOTH:
            # keep both
            new_name = __get_new_name(drive, name, dest_id, mime_type)
            if show_msg: print(f"{indent}Copying {type_} from {name} to {new_name}")
            name = new_name

        elif mode == ConflictMode.OVERWRITE:
            # overwrite
            if show_msg: print(f"{indent}Overwriting {type_} {name}")
            drive.files().delete(fileId=old_id).execute()
        
        show_msg = False

    # Copy the file
    if show_msg: print(f"{indent}Copying {type_} {name}")
    __process(drive, file_id, copy_file(drive, file_id, name, dest_id), options)


def __get_interactive_conflict_mode(name: str, mime_type: str) -> ConflictMode:
    """Gets the conflict mode for the file with the given name and mime type."""
    opts = {
        's': ConflictMode.KEEP_EXISTING,
        'o': ConflictMode.OVERWRITE,
        'k': ConflictMode.KEEP_BOTH,
    }
    opt = input(
        f"Conflict with file {name} ({mime_type}): [s]kip, [o]verwrite, [k]eep both? ").lower()
    while opt not in opts:
        opt = input("Invalid option. Choose [s]kip, [o]verwrite, or [k]eep both: ").lower()
    return opts[opt]


def __get_new_name(drive, name: str, dest_id: str, mime_type: str) -> str:
    """Gets a new name for the file when keeping both."""
    i = 1
    new_name = f"{name} ({i})"
    while file_exists(drive, new_name, dest_id, mime_type):
        i += 1
        new_name = f"{name} ({i})"
    return new_name


def __process(drive, src_id: str, dest_id: str, options: Options):
    """Process a file after copying it by copying permissions and comments as needed."""
    if options.copy_perms: __copy_permissions(drive, src_id, dest_id, options.send_emails)
    if options.copy_comments: __copy_comments(drive, src_id, dest_id)


def __copy_permissions(drive, src_id: str, dest_id: str, send_emails: bool = False):
    """
    Copy the permissions from one file to another file, optionally sending the email notifications.
    This cannot change ownership and assumes the current user is and will remain the owner.
    """
    # Get the current user's email address and cache it
    if not hasattr(drive, '__email_address'):
        response = drive.about().get(fields='user').execute()
        drive.__email_address = response.get("user").get("emailAddress")
    email_address = drive.__email_address

    # Get all of the current permissions
    fields = 'permissions(type,role,emailAddress,domain,allowFileDiscovery,expirationTime,deleted)'
    perms = get_all_pages(drive.permissions().list, 'permissions', fileId=src_id,
                          fields=fields, supportsAllDrives=True)

    # Cannot change ownership away from current user
    perms = [perm for perm in perms if perm["emailAddress"] != email_address]

    # Create all of the permissions
    create = drive.permissions().create
    for perm in perms:
        if perm.pop('deleted', False): continue
        if perm['role'] == 'owner': perm['role'] = 'writer'
        kwargs = dict(fileId=dest_id, body=perm, supportsAllDrives=True)
        if perm.get('type') in ('user', 'group'):
            kwargs["sendNotificationEmail"] = send_emails
        create(**kwargs).execute()


def __copy_comments(drive, src_id: str, dest_id: str):
    """
    Copy all of the comments and replies from one file to another file. This cannot copy the
    authorship or timstamps on the comments so those are written into the comments themselves.
    """
    fields = 'comments(id,content,anchor,quotedFileContent,createdTime,modifiedTime,replies,author)'
    comments = get_all_pages(drive.comments().list, 'comments', fileId=src_id, fields=fields)

    create_comment = drive.comments().create
    create_reply = drive.replies().create
    for comment in comments:
        new = {"content": __make_comment_content(comment)}
        if comment.get("anchor"): new["anchor"] = comment["anchor"]
        if comment.get("quotedFileContent"): new["quotedFileContent"] = comment["quotedFileContent"]
        response = create_comment(fileId=dest_id, body=new, fields='id').execute()

        comment_id = response.get('id')
        for reply in comment.get("replies"):
            new = {"content": __make_comment_content(reply)}
            if reply.get("action"): new["action"] = reply["action"]
            create_reply(fileId=dest_id, commentId=comment_id, body=new, fields='id').execute()


def __make_comment_content(comment: dict) -> str:
    """
    Makes the content string of a comment (or reply) by adding extra text about the original
    author (if not the current user) and creation/modifiation times.
    """
    content = comment["content"]
    content += "\n\n_Originally "
    author = comment.get("author", {})
    if not author.get("me", False):
        content += f"by {author.get('displayName')} "
    createdTime = datetime.fromisoformat(comment['createdTime'].removesuffix('Z'))
    modifiedTime = datetime.fromisoformat(comment['modifiedTime'].removesuffix('Z'))
    content += f"at {createdTime.strftime('%a %d %b %Y, %I:%M%p UTC')}"
    if createdTime - modifiedTime > timedelta(seconds=30):
        content += f" and modified at {modifiedTime.strftime('%a %d %b %Y, %I:%M%p UTC')}"
    content += "_"
    return content


def main():
    # Activate the Drive service
    drive, = get_drive_service()

    # Get the command line arguments
    parser = argparse.ArgumentParser(description="""Copies a Google Drive folder recursively.""")
    parser.add_argument('id', type=partial(file_id_exists, drive),
                        help="Google folder ID or URL to copy")
    parser.add_argument('dest',
                        help="Destination folder to copy to. Either an ID or a path relative to "
                             "the folder (supports .. and starting with / for root)")
    parser.add_argument('name', default=None, nargs='?',
                        help="Copy name. Defaults to the name of the original.")
    parser.add_argument('--verbose', '-v', action='store_true',
                        help="Show information about all folders created and files copied")
    parser.add_argument('--make-dirs', '-d', action='store_true',
                        help="Create the destination folder (and its parents) if it doesn't exist")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--merge', '-m', action='store_true',
                       help="Keep existing files when there are conflicts")
    group.add_argument('--keep-both', '-k', action='store_true',
                       help="Keep both files when there are conflicts by appending a number")
    group.add_argument('--overwrite', '-o', action='store_true',
                       help="Overwrite existing files when there are conflicts")
    group.add_argument('--interactive', '-i', action='store_true',
                       help="Ask what to do when there are conflicts")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--follow-shortcuts', '-f', action='store_true',
                       help="Follow shortcuts and copy their contents instead of copying the "
                            "shortcut itself.")
    group.add_argument('--follow-file-shortcuts', action='store_true',
                       help="Follow shortcuts to file and copy their contents instead of copying "
                            "the shortcut itself.")
    group.add_argument('--follow-folder-shortcuts', action='store_true',
                       help="Follow shortcuts to fodlers and copy their contents instead of "
                            "copying the shortcut itself.")
    parser.add_argument('--perms', '-p', action='store_true',  # TODO: not tested at all
                        help="Copy permissions to new files. The ownership of the new files will "
                             "always be the current user, but other permissions are updated as "
                             "able.")
    parser.add_argument('--emails', '-e', action='store_true',
                        help="Send email notifications to individualsabout sharing. Only used "
                             "when permissions are copied")
    parser.add_argument('--comments', '-c', action='store_true',
                        help="Copy comments to new files. The authorship of comments cannot be "
                             "copied and the current user will be the author of all of them "
                             "but the comments are amended to include the original author and "
                             "timestamp")
    parser.add_argument("--match", "-M", metavar="pattern", action='append',
                        help="Match files to copy based on gitignore-style match patterns. Can be "
                             "used multiple times for multiple patterns.")
    parser.add_argument("--match-include", "-I", metavar="file", action='append',
                        type=argparse.FileType("r"), help="Load match patterns from a file.")

    args = parser.parse_args()

    # Get the mode
    mode = (ConflictMode.KEEP_BOTH if args.keep_both else
            ConflictMode.OVERWRITE if args.overwrite else
            ConflictMode.KEEP_EXISTING if args.merge else
            ConflictMode.INTERACTIVE if args.interactive else
            ConflictMode.NEVER)
    
    # Get the shortcut option
    shortcuts = (Shortcut.FOLLOW_DIR if args.follow_folder_shortcuts else
                 Shortcut.FOLLOW_FILE if args.follow_file_shortcuts else
                 Shortcut.FOLLOW if args.follow_shortcuts else
                 Shortcut.AS_IS)

    # Built pathspec for matching
    match_lines = []
    for include in args.match_include or []:
        match_lines.extend(include.readlines())
        include.close()
    match_lines.extend(args.match or [])
    match = pathspec.PathSpec.from_lines('gitwildmatch', match_lines) if match_lines else None

    # Copy the directory
    copy_dir(drive, args.id, args.dest, args.name, args.make_dirs, Options(
        verbose=args.verbose, mode=mode, shortcuts=shortcuts, match=match,
        copy_perms=args.perms, send_emails=args.emails, copy_comments=args.comments,
    ))


if __name__ == '__main__':
    main()

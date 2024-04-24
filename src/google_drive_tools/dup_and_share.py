#!/usr/bin/env python3
"""
Creates many duplicates of a Google Doc by appending a student's name to each
copy of the file and then sharing the file with the student.
"""

import os
import sys
import csv
import argparse
from functools import partial
from typing import Optional, Iterable
import concurrent.futures

from .utils import get_services, file_id_check, file_id_exists, get_file_id, copy_file, find_folder
from .utils import MIME_TYPE_DOC, MIME_TYPE_SHEET


def dup_and_share(
        drive, docs, file_id: str, groups: dict[str, list[str]],
        name_template: Optional[str] = None, dest: Optional[str] = None, make_dirs = False,
        send_email: bool = True, email_msg: Optional[str] = None,
        strip_answers: bool = False, answer_replacement: str = "",
        ):
    """
    Uses the Google Drive and Docs services to duplicate a document and share it with a list of
    students in groups. The document is shared with each student in the group with write access.
    """
    # Get info about the template file
    response = drive.files().get(fileId=file_id, fields='name,mimeType,parents',
                                 supportsAllDrives=True).execute()
    title, mime_type = response.get('name'), response.get('mimeType')
    print(f"Copying document {title} ({file_id})")

    # Determine destination folder ID
    dest_id, parents = get_dest(drive, dest, make_dirs, response.get('parents'))
    condition = f"mimeType='{mime_type}' and ({parents}) and trashed=false"

    # Get file template name
    if name_template is None: name_template = title
    if '{}' not in name_template: name_template += ' - {}'

    # The body of the permissions request
    permissions = {'type': 'user', 'role': 'writer', 'sendNotificationEmail': send_email}
    if send_email and email_msg: permissions['emailMessage'] = email_msg

    # Strip answers from the document
    needs_deletion = False
    if strip_answers and mime_type in MIME_TYPE_DOC:
        file_id = strip_answers_from_doc(drive, docs, file_id, answer_replacement)
        needs_deletion = True

    def __single(group: str, emails: list[str]) -> bool:
        file_name = name_template.format(group)

        # Make sure file doesn't already exist (i.e. group hasn't already been processed)
        if get_file_id(drive, file_name, condition):
            return False

        # Copy the file for the group
        doc_copy_id = copy_file(drive, file_id, file_name, dest_id)

        # Share the file with the students in the group
        permissions = permissions.copy()
        for email in emails:
            permissions['emailAddress'] = email
            drive.permissions().create(fileId=doc_copy_id, body=permissions).execute()
        
        return True

    # Go through each group (in parallel)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(__single, group, emails): group
                   for group, emails in groups.items()}
        for future in concurrent.futures.as_completed(futures):
            group = futures[future]
            try:
                created = future.result()
            except Exception as exc:
                print(f"Failed to duplicate and share for {group}: {str(exc)}")
            else:
                if created:
                    print(f"Created {group}: {', '.join(groups[group])}")
                else:
                    file_name = name_template.format(group)
                    print(f"Skipped, document '{file_name}' already exists in same folder")

    # Delete the temporary document
    if needs_deletion:
        drive.files().delete(fileId=file_id, supportsAllDrives=True).execute()


def get_drive_and_doc_services():
    """Gets the Google Drive and Docs API services."""
    return get_services((('drive', 'v3'), ('docs', 'v1')),
                        ('https://www.googleapis.com/auth/drive',
                         'https://www.googleapis.com/auth/documents'),
                        'dup-and-share-token.pickle', 'dup-and-share-credentials.json')


def get_dest(drive, dest, make_dirs, parents):
    """Determines the destination folder ID and the query string for the destination folder."""
    if dest:
        try:
            dest_id = file_id_exists(dest)
        except argparse.ArgumentTypeError:
            dest_id = find_folder(drive, dest, make_dirs, parents[0] if parents else 'root')
        parents = f"'{dest_id}' in parents"
    else:
        dest_id = None
        parents = ' or '.join(f"'{parent}' in parents" for parent in parents)
    return dest_id, parents


def strip_answers_from_doc(drive, docs, file_id, replacement=""):
    """
    Strips answers from a Google Doc. Returns a new document ID with the answers stripped. The
    new document must be deleted by the caller when done.
    """
    # Duplicate the document instead of modifying the original
    file_id = copy_file(drive, file_id, "TEMPORARY DOC WITHOUT ANSWERS")

    # Get the document content
    doc = docs.documents().get(documentId=file_id).execute()

    # Find all of the answers and replace them with the replacement text
    updates = []
    __answers_to_batch_updates(doc["body"]["content"], updates, replacement)
    updates.reverse()  # this assumes that the updates are in order of start index, could do a sort instead?

    # Apply the updates to the document
    docs.documents().batchUpdate(documentId=file_id, body={'requests': updates}).execute()

    return file_id


def __answers_to_batch_updates(content, updates, replacement=""):
    for elem in content:
        if "paragraph" in elem:
            start = elem["startIndex"]
            end = elem["endIndex"]
            style = elem["paragraph"]["paragraphStyle"]["namedStyleType"]
            if style == "HEADING_6":
                if replacement:
                    updates.append({"insertText": { "location": { "index": start }, "text": replacement }})
                updates.append({"deleteContentRange": { "range": { "startIndex": start, "endIndex": end-1 } }})

        elif "table" in elem:
            for row in elem["table"]["tableRows"]:
                for cell in row["tableCells"]:
                    __answers_to_batch_updates(cell["content"], updates, replacement)


BOM = {
    b'\xFE\xFF': 'utf_16_be', b'\xFF\xFE': 'utf_16_le',
    b'\xEF\xBB\xBF': 'utf_8',
    #b'\xF7\x64\x4C': 'utf-1',
    #b'\x0E\xFE\xFF': 'scsu', b'\xFB\xEE\x28': 'bocu-1',
    b'\x00\x00\xFE\xFF': 'utf_32_be', b'\xFF\xFE\x00\x00': 'utf_32_le',
    b'\x2B\x2F\x76\x38': 'utf_7', b'\x2B\x2F\x76\x39': 'utf_7',
    b'\x2B\x2F\x76\x2B': 'utf_7', b'\x2B\x2F\x76\x2F': 'utf_7',
    #b'\xDD\x73\x66\x73': 'utf_ebcdic',
}


def groups_check(drive, value):
    """
    Check that an argument can be used to load groups from. Possible values are:
        * a file path
        * a '-' for stdin
        * a Google Drive ID (of a CSV file) [or URL with ID]
        * a Google Sheet ID [or URL with ID]
    Returns the file object or a tuple of the file ID and mime type.
    """
    # TODO: support alternate sheet (tab) in a Google Sheet
    # see https://stackoverflow.com/questions/37705553/how-to-export-a-csv-from-google-sheet-api

    if value == '-': return sys.stdin
    if os.path.exists(value): return open_as_text_with_bom(value)
    value = file_id_check(value)
    response = drive.files().get(fileId=value, fields='mimeType',
                                 supportsAllDrives=True).execute()
    mime_type = response.get('mimeType')
    if mime_type in MIME_TYPE_SHEET: return (value, MIME_TYPE_SHEET[0])
    if mime_type == 'text/csv': return (value, mime_type)
    raise argparse.ArgumentTypeError('Invalid group file argument')


def open_as_text_with_bom(filename):
    """
    Looks at the first few bytes of a file to determine the BOM encoding if it is there and
    reopens the file as text with the appropriate encoding (common for Excel-saved CSV files).
    """
    bom = open(filename, 'rb').read(4)
    for i in range(len(bom), 1, -1):
        if bom[:i] in BOM:
            file = open(filename, 'rt', encoding=BOM[bom[:i]], newline='')
            if file.read(1) != '\uFEFF':
                raise UnicodeDecodeError('failed to decode BOM in file')
            return file
    return open(filename, 'rt', newline='')  # fallback to current system default


def read_groups(drive, value) -> dict[str, list[str]]:
    """
    Reads the groups from a file. The file can be a CSV file or a Google Drive file. The file
    should have one of the following layouts:
        * last-name,first-name,email (this is the CSV files the Gitkeeper uses - makes 1 copy per student)
        * group-name,email1,email2,... (makes 1 copy per group, duplicate group names are combined)
    Returns a dict of name:list-of-emails.
    """
    if not isinstance(value, tuple):
        # Assume it is a file object
        with value: return make_groups(csv.reader(value))
    
    # Download the data
    file_id, mime_type = value
    files = drive.files()
    if mime_type == 'text/csv':
        data = files.get_media(fileId=file_id, supportsAllDrives=True).execute()
    else:
        data = files.export_media(fileId=file_id, mimeType='text/csv',
                                  supportsAllDrives=True).execute()
    return make_groups(csv.reader(data.decode().splitlines(keepends=True)))


def make_groups(data: Iterable[list[str]]) -> dict[str, list[str]]:
    """
    Make groups from an iterable of rows of data. This is commonly made by csv.reader().
    The data should be in one of the following:
        last-name,first-name,email    (designed for gkeep)
        group-name,email1,email2,...  (duplicate group-names are combined)
    Returns a dict of name:list-of-emails.
    """
    groups = {}

    # Get the first entry to determine style of the file
    # If the first entry is a header row (i.e. no emails) move to the second entry
    data = iter(data)
    entry = next(data)
    n = len(entry)
    if n < 2 or n == 2 and '@' not in entry[1] or n > 2 and '@' not in entry[2]:
        entry = next(data)
    if len(entry) != 3 or '@' in entry[1]:
        # group name plus 1 or more emails per entry
        process = lambda entry: (entry[0], [email for email in entry[1:] if '@' in email])
    elif len(entry) > 1:
        # last name, first name, email per entry
        process = lambda entry: (entry[1] + ' ' + entry[0], [entry[2]])
    else:
        return groups # empty file

    # Process all entries
    name, emails = process(entry)
    groups.setdefault(name, []).extend(emails)
    for entry in data:
        name, emails = process(entry)
        groups.setdefault(name, []).extend(emails)
    return groups


def main():
    # Activate the Drive and Docs services
    drive, docs = get_drive_and_doc_services()

    # Get the command line arguments
    parser = argparse.ArgumentParser(description="""Duplicates a Google Drive file, updating the
name to include a student/group's name and sharing it with them.""", epilog="""The CSV file must
have one of the following layouts:
 * last-name,first-name,email (this is the CSV files the Gitkeeper uses - makes 1 copy per student)
 * group-name,email1,email2,... (makes 1 copy per group, duplicate group names are combined)
Every row in the CSV file must be consistent (i.e. all groups or all individual students). First
row is skipped if it doesn't contain an email address (assumed to be a header).
""")
    parser.add_argument('id', type=partial(file_id_exists, drive),
                        help="Google file ID or URL to copy")
    parser.add_argument('groups', type=partial(groups_check, drive),
                        help="CSV file describing duplications to make, see below for details")
    parser.add_argument(['--dest', '-d'],
                        help="Destination to save the copies to. Either an ID or a path relative to "
                             "the file, defaults to the same folder as the file (supports .. and "
                             "starting with / for root)")
    parser.add_argument(['--make-dirs', '-p'], action='store_true',
                        help="Create the destination folder (and its parents) if it doesn't exist")
    parser.add_argument(['--name', '-n'],
                        help="Name of the copied files with a {} placeholder for the group name, "
                             "default is the 'name of the file - {}'")
    parser.add_argument(['--strip-answers', '-a'], const=True, default=False, nargs='?',
                        help="Strip answers from the document before sharing. This only works for "
                             "Google Docs and removes all Heading-6 text (but leaves the paragraph "
                             "in place for styling). If a value is given, it will replace the "
                             "answers with that text.")
    parser.add_argument(['--no-email', '-N'], action='store_true',
                        help="Do not notify individuals of the new shared files")
    parser.add_argument(['--email', '-e'],
                        help="Additional email message to supply with the notification email")

    args = parser.parse_args()

    # Read the groups data
    groups = read_groups(args.groups)

    # Duplicate the document and share it with the students
    strip_answers = True if args.strip_answers is not False else None
    replacement = args.strip_answers if isinstance(args.strip_answers, str) else ""
    dup_and_share(
        drive, docs, args.id, groups, name_template=args.name,
        dest=args.dest, make_dirs=args.make_dirs,
        send_email=not args.no_email, email_msg=args.email,
        strip_answers=strip_answers, answer_replacement=replacement,
        )


if __name__ == '__main__':
    main()

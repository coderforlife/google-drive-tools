# Google Drive Tools

Tools for using Google Drive.

## `dup-and-share`

Duplicate a Google Doc (or any file on Google Drive) and share each of the duplicates with a specific subset of others. The copies and who they are shared with are specified using a CSV or Google Sheet.

### Features

* Duplicates a file on Google Drive by its ID or share URL
* Shares the duplicates with individuals specified by a CSV or Google Sheet in various layouts, including:
  * group name in the first column and all other columns are emails (duplicate groups are merged)
  * first two columns are last and first name of the individual and the third column their email
* Specifying a destination for the duplicates (default is same directory as original)
* Specifying an alternate name for the copies (always includes the group name)
* Sending a share email or not, and if sending a share email adding custom content to it
* Striping "answers" from the original before duplication (any paragraph that is marked as Heading 6 is removed, but the paragraph itself is kept)

### Setup

Install using pip like so:

```sh
pip3 install git+https://github.com/coderforlife/google-drive-tools.git
```

To use this you first must create a Google API OAuth 2.0 Client ID credentials file:

* Go to <https://console.cloud.google.com/apis/credentials>
* TODO: there are likely more steps here the first time you use APIs
* Click "+ Create Credentials" then OAuth Client ID
* Select the "Desktop app" application type and give it a name (the name should be clear to you, but can be anything)
* Download the JSON file
* The downloaded json file must be renamed to `dup-and-share-credentials.json` and moved to one of the following locations:
  * `~/.config/google-app-credentials` (you may need to make the directory)
  * Inside the directory of the google_drive_tools Python package
  * The current directory where you will run the script from

Once you have the credentials file copied to an appropriate location, you can run `dup-and-share` from the terminal and it will initialize the API, you will have to sign in and approve the application with Google in your browser. It will save the login state and reload it automatically.

### Known Issues

* Stripping answers can only remove entire paragraphs (limitation of Google Doc API)
* Stripping answers keeps every paragraph, even if there are consecutive paragraphs marked as answers
* Stripping answers only works for Google Docs (could add support for Google Sheets)
* Copying a document removes all comments and suggestions (limitation of Google Drive API)
* Grabbing groups from a Google Sheet can only use the first sheet/tab

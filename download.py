"""Download files from Dropbox."""
import os
import concurrent.futures
import dropbox


def download_file(dbx, entry, dest_folder, overwrite=False):
    """Download a single file from a Dropbox folder to a local folder."""
    file_path = os.path.join(dest_folder, entry.name)
    # Check if file exists and if overwrite is False
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0 and not overwrite:
        print(f"File {file_path} already exists. Skipping.")
        return
    with open(file_path, "wb") as f:
        _, res = dbx.files_download(path=entry.path_lower)
        f.write(res.content)


def download_folder(dbx, folder_path, dest_folder, overwrite=False):
    """Download all files from a large Dropbox folder to a local folder."""
    # Ensure the destination folder exists
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    # List all files and subfolders in the Dropbox folder
    result = dbx.files_list_folder(folder_path)

    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            # Use map to download files in parallel
            executor.map(lambda entry: download_file(dbx, entry, dest_folder, overwrite),
                         [entry for entry in result.entries if isinstance(entry, dropbox.files.FileMetadata)])

            # If it's a folder, recursively download its contents
            for entry in result.entries:
                if isinstance(entry, dropbox.files.FolderMetadata):
                    download_folder(dbx, entry.path_lower, os.path.join(
                        dest_folder, entry.name), overwrite)

            # Check if there are more files/subfolders to list
            if result.has_more:
                result = dbx.files_list_folder_continue(result.cursor)
            else:
                break


# Initialize Dropbox client
ACCESS_TOKEN = 'YOUR_ACCESS_TOKEN'
DBX = dropbox.Dropbox(ACCESS_TOKEN)

DROPBOX_FOLDER = 'DROPBOX_FOLDER_PATH'
LOCAL_FOLDER = 'LOCAL_FOLDER_PATH'

download_folder(DBX, DROPBOX_FOLDER, LOCAL_FOLDER)

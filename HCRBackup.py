import os
import os.path
import json
import subprocess
import tempfile
import argparse
import botocore.utils
import datetime, time
import backupmongo


# Only the *files* in a given directory are archived, not the subdirectories.
# The contents of the subdirectories live in archives of their own (except for any directories that *they* contain)
# This means that only the changed files in a given directory need to be checked - and that if a file in a
# sub-sub-subdirectory is changed, the whole parent directory doesn't need to be re-uploaded.
# The name of each archive is equal to the name of the directory.

def archive_directory(top_dir, subdir, tmpdir):
    # We're only archiving the *files* in this directory, not the subdirectories.

    files = []
    full_backup_path = os.path.join(top_dir, subdir)
    dir_contents = os.listdir(full_backup_path)

    # Only add files to 'files' list, not subdirs
    for c in dir_contents:
        fpath = os.path.join(top_dir, subdir, c)
        if os.path.isfile(fpath) and not fpath.endswith(".ini"):
            files.append(fpath)

    if files:  # No point creating empty zips!
        sevenz_loc = os.path.join("C:\\", "Program Files", "7-Zip", "7z.exe")
        archive_file_path = os.path.join(tmpdir, os.path.basename(subdir)) + ".7z"

        print "Archiving", subdir, "to", archive_file_path
        try:
            devnull = open(os.devnull, "w")
            subprocess.check_call(
                [sevenz_loc, "a", "-t7z", archive_file_path, os.path.join(full_backup_path, "*"),
                 "-m0=BZip2", "-y", "-aoa", "-xr-!*\\"], stdout=devnull, stderr=devnull)
            devnull.close()
            return archive_file_path
        except subprocess.CalledProcessError, e:
            ret_code = e.returncode
            if ret_code == 1:
                # Warning (Non fatal error(s)). For example, one or more files were locked by some
                # other application, so they were not compressed.
                print "\t7z: Non-fatal error(s)"
            elif ret_code == 2:
                # Fatal error
                print "\t7z: Fatal error"
            elif ret_code == 7:
                # Command-line error
                print "\t7z: Command-line error"
                print "\t7z:", e.cmd
            elif ret_code == 8:
                # Not enough memory for operation
                print "\t7z: Not enough memory for operation"
            elif ret_code == 255:
                # User stopped the process
                print "\t7z: User stopped the process"
            return None


def upload_archive(archive_path, aws_vault, archive_treehash, aws_account_id):
    print "\tUploading", archive_path, "to", aws_vault

    devnull = open(os.devnull, "w")

    aws_cli_path = os.path.join("C:\\", "Program Files", "Amazon", "AWSCLI", "aws.exe")
    try:
        aws_cli_op = subprocess.check_output(
            [aws_cli_path, "glacier", "upload-archive", "--vault-name", aws_vault, "--account-id", aws_account_id,
             "--checksum", archive_treehash, "--body", archive_path], stderr=devnull)

        aws_cli_op = aws_cli_op.replace("\r\n", "")

        # Returned fields from upload:
        # location -> (string)
        # The relative URI path of the newly added archive resource.
        # checksum -> (string)
        # The checksum of the archive computed by Amazon Glacier.
        # archiveId -> (string)
        # The ID of the archive. This value is also included as part of the location.

        aws_params = json.loads(aws_cli_op)
        return aws_params

    except subprocess.CalledProcessError, e:
        print "Upload failed with error", e.returncode
        print e.message
        print e.cmd
        print e.output

def delete_archive(archive_id, aws_vault, aws_account_id):
    devnull = open(os.devnull, "w")
    aws_cli_path = os.path.join("C:\\", "Program Files", "Amazon", "AWSCLI", "aws.exe")
    try:
        aws_cli_op = subprocess.check_output(
            [aws_cli_path, "glacier", "delete-archive", "--account-id", aws_account_id,
             "--archive-id", archive_id], stderr=devnull)
    except subprocess.CalledProcessError, e:
        print "Deletion failed with error", e.returncode
        print e.message
        print e.cmd
        print e.output


def compare_files(length_a, hash_a, length_b, hash_b):
    return (length_a == length_b) & (hash_a == hash_b)


def list_dirs(top_dir):
    dirs = []
    for dirname, subdirs, files in os.walk(top_dir):
        for s in subdirs:
            dirs.append(os.path.relpath(os.path.join(dirname, s), top_dir))
    return dirs


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser(
        description='A tool to manage differential file uploads to an Amazon Glacier repository')
    arg_parser.add_argument('directory', help='The top directory to back up', metavar='top_dir')
    arg_parser.add_argument('vault', help='The name of the vault to upload the archive to', metavar='vault_name')
    arg_parser.add_argument('--account-id', help='The AWS ID of the account that owns the specified vault',
                            metavar='aws_acct_id', default='-')
    arg_parser.add_argument('--database',
                            help='The database file name to connect to (relative paths will be evaluated from the execution directory)',
                            metavar='db_name')
    arg_parser.add_argument('--no-backup',
                            help='If passed, the backup operation will not take place, going straight to the maintenance operations',
                            action='store_true')

    args = arg_parser.parse_args()

    # Top of directory to backup
    root_dir = args.directory
    if not os.path.exists(root_dir):
        raise ValueError("%s does not exist" % root_dir)

    aws_vault_name = args.vault
    aws_account_id = args.account_id
    db_name = args.database

    db_client, db = backupmongo.connect(db_name)

    if not args.no_backup:

        # Temporary directory to create archives in
        temp_dir = tempfile.mkdtemp()

        print "Backing up %s to %s using AWS Account ID %s" % (root_dir, aws_vault_name, aws_account_id)

        subdirs_to_backup = list_dirs(root_dir)  # List of subtrees, relative to root_dir
        subdirs_to_backup.append("")  # Dammit I will get this working - get the root directory contents to be zipped

        for subdir_to_backup in subdirs_to_backup:

            # Archive each folder in the list to it's own ZIP file
            tmp_archive_fullpath = archive_directory(root_dir, subdir_to_backup, temp_dir)
            if not tmp_archive_fullpath:
                # Directory was empty - not being archived
                continue

            # Calculate the treehash of the local archive
            with open(tmp_archive_fullpath, 'rb') as arch_f:
                # archive_hash = calculate_tree_hash(arch_f)
                archive_hash = botocore.utils.calculate_tree_hash(arch_f)

            backup_subdir_abs_filename = os.path.join(root_dir, subdir_to_backup) + ".7z"
            backup_subdir_rel_filename = subdir_to_backup + ".7z"

            # Find most recent version of this file in Glacier
            most_recent_version = backupmongo.get_most_recent_version_of_archive(backup_subdir_rel_filename)

            if most_recent_version:
                print "\tFound path info in local database"
                hash_remote = most_recent_version['treehash']
                size_remote = most_recent_version['size']

            most_recent_version:
                print "\tPath does not exist in local database"
                hash_remote = size_remote = None

            # Compare it against the local copy of the Glacier version of the archive
            size_arch = os.stat(tmp_archive_fullpath).st_size

            # If the hashes are the same - don't upload the archive; it already exists
            if not compare_files(size_arch, archive_hash, size_remote, hash_remote):
                # Otherwise, upload the archive
                upload_status = upload_archive(tmp_archive_fullpath, aws_vault_name, archive_hash, aws_account_id)
                if upload_status:
                    print "\tUploaded", tmp_archive_fullpath, "to", aws_vault_name

                    # Get vault arn:
                    aws_vault_arn = backupmongo.get_vault_by_name(aws_vault_name)["arn"]

                    # Store the info about the newly uploaded file in the database
                    backupmongo.create_archive_entry(db,
                                                     backup_subdir_rel_filename,
                                                     aws_vault_arn
                                                     upload_status["archiveId"],
                                                     archive_hash,
                                                     size_arch,
                                                     upload_status["location"])
                else:
                    print "\tFailed to upload", backup_subdir_rel_filename
            else:
                print "\tNot uploaded", backup_subdir_rel_filename, "- the file is already uploaded"

            # Delete the temporary archive
            os.remove(tmp_archive_fullpath)

            # TODO: Mark old versions of archives for deletion

            # Find archives older than three months, with three more recent versions
            # available
            # This could only be the case when we've uploaded a new version of an archive, thereby
            # making an old version irrelevant - so we only need to look for archives with this path.
            old_archives = backupmongo.get_old_archives(db, backup_subdir_rel_filename, aws_vault_arn)
            for arch in old_archives:
                backupmongo.mark_archive_for_deletion(db, arch["_id"])

        # Delete the temporary directory.
        os.rmdir(temp_dir)

    # Find and delete old archives
    redundant_archives = get_archives_to_delete(db)
    for arch in redundant_archives:
        print "Deleting %s from AWS..."
        delete_archive(arch["_id"], aws_vault_name, aws_account_id)
        pymongo.delete_archive_document(db,arch["_id"])


    # Finished with the database
    db_client.close()

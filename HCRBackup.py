__author__ = 'Callum McLean <calmcl1@aol.com>'
__version__= '0.1.0'

import os, os.path
import json
import subprocess
import tempfile
import argparse
import botocore.utils
import datetime, time
import backupmongo
import logging


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

    if files:  # No point creating empty archives!
        archive_file_path = os.path.join(tmpdir, os.path.basename(subdir)) + ".7z"

        logging.info("Archiving %s to %s" % (subdir, archive_file_path))
        try:
            devnull = open(os.devnull, "w")
            subprocess.check_call(
                ["7z", "a", "-t7z", archive_file_path, os.path.join(full_backup_path, "*"),
                 "-m0=BZip2", "-y", "-aoa", "-xr-!*/"], stdout=devnull, stderr=devnull)
            devnull.close()
            logging.debug("Created archive at %s" % archive_file_path)
            return archive_file_path
        except subprocess.CalledProcessError, e:
            ret_code = e.returncode
            if ret_code == 1:
                # Warning (Non fatal error(s)). For example, one or more files were locked by some
                # other application, so they were not compressed.
                logging.error("7-Zip: Non-fatal error (return code 1)")
            elif ret_code == 2:
                # Fatal error
                logging.error("7-Zip: Fatal error (return code 2)")
            elif ret_code == 7:
                # Command-line error
                logging.error("7-Zip: Command-line error (return code 7)\n%s"
                              % e.cmd)
            elif ret_code == 8:
                # Not enough memory for operation
                logging.error("7-Zip: Not enough memory for operation (return code 8)")
            elif ret_code == 255:
                # User stopped the process
                logging.error("7-Zip: User stopped the process (return code 255)")
            return None


def upload_archive(archive_path, aws_vault, archive_treehash, aws_account_id, dummy=False):
    logging.info("Uploading {0} to vault {1}".format(archive_path, aws_vault))

    devnull = open(os.devnull, "w")

    try:
        if not dummy:
            aws_cli_op = subprocess.check_output(
                ["aws" "glacier", "upload-archive", "--vault-name", aws_vault, "--account-id", aws_account_id,
                 "--checksum", archive_treehash, "--body", archive_path], stderr=devnull)
            devnull.close()

            aws_cli_op = aws_cli_op.replace("\r\n", "")

            # Returned fields from upload:
            # location -> (string)
            # The relative URI path of the newly added archive resource.
            # checksum -> (string)
            # The checksum of the archive computed by Amazon Glacier.
            # archiveId -> (string)
            # The ID of the archive. This value is also included as part of the location.

            aws_params = json.loads(aws_cli_op)

        else:
            # This is a dummy upload, for testing purposes. Create a fake
            # AWS URI and location, but don't touch the archive.
            aws_params = {}
            aws_params["archiveId"] = aws_vault + "-hcrbackup-" + time.time()
            aws_params["location"] = "aws://dummy-uri-" + aws_params["archiveId"]
            aws_params["checksum"] = archive_treehash

        logging.debug("Uploaded archive {archpath} \n \
                      Returned fields: \n \
                      \tlocation: {params[location]} \n \
                      \tchecksum: {params[checksum]} \n \
                      \tarchiveId: {params[archiveId]}".format(archpath=archive_path,
                                                              params=aws_params))
        return aws_params

    except subprocess.CalledProcessError, e:
        logging.error("Upload failed! Error: {err.returncode}\n \
                      \t{err.message}\n\t{err.cmd}\n\t{err.output}".format(err=e))
        return None

def delete_aws_archive(archive_id, aws_vault, aws_account_id):
    logging.info("Deleting archive with id {0} from vault {1}".format(
        archive_id, aws_vault))

    devnull = open(os.devnull, "w")
    try:
        aws_cli_op = subprocess.check_output(
            ["aws", "glacier", "delete-archive", "--account-id", aws_account_id,
             "--archive-id", archive_id], stderr=devnull)
        logging.info("Successfully deleted archive from AWS")
        return 1

    except subprocess.CalledProcessError, e:
        print "Deletion failed with error", e.returncode
        print e.message
        print e.cmd
        print e.output
        logging.error("Failed to delete archive from AWS! Error: {err.returncode}\n \
                      \t{err.message}\n\t{err.cmd}\n\t{err.output}".format(err=e))
        return None

def delete_redundant_archives(db, aws_vault_name, aws_account_id):
    redundant_archives = backupmongo.get_archives_to_delete(db)
    for arch in redundant_archives:
        deleted_aws = delete_aws_archive(arch["_id"], aws_vault_name, aws_account_id)
        if deleted_aws:
            pymongo.delete_archive_document(db,arch["_id"])
        else:
            logging.info("AWS deletion failed; not removing database entry")

def compare_files(length_a, hash_a, length_b, hash_b):
    return (length_a == length_b) & (hash_a == hash_b)


def list_dirs(top_dir):
    logging.info("Finding subdirectories of {0}".format(top_dir))
    dirs = []
    for dirname, subdirs, files in os.walk(top_dir):
        for s in subdirs:
            dirs.append(os.path.relpath(os.path.join(dirname, s), top_dir))
            logging.debug("Found {0}".format(s))
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
    arg_parser.add_argument('--no-prune',
                            help='If passed, the process of finding and removing old archives will not take place.',
                            action='store_true')
    arg_parser.add_argument('--dummy-upload',
                            help='If passed, the archives will not be uploaded, but a dummy AWS URI and archive ID will be generated. Use for testing only.',
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
        logging.info("Created temporary directory at {0}".format(temp_dir))

        logging.info("Backing up {0} to {1} using AWS Account ID {2}".format(
            root_dir, aws_vault_name, aws_account_id))

        subdirs_to_backup = list_dirs(root_dir)  # List of subtrees, relative to root_dir
        subdirs_to_backup.append("")  # TODO: Dammit I will get this working - get the root directory contents to be zipped

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
                logging.info("Archive for this path exists in local database")
                hash_remote = most_recent_version['treehash']
                size_remote = most_recent_version['size']

            most_recent_version:
                logging.info("No archive found for this path in local database")
                hash_remote = size_remote = None

            # Compare it against the local copy of the Glacier version of the archive
            size_arch = os.stat(tmp_archive_fullpath).st_size

            # If the hashes are the same - don't upload the archive; it already exists
            if not compare_files(size_arch, archive_hash, size_remote, hash_remote):
                # Otherwise, upload the archive
                upload_status = upload_archive(tmp_archive_fullpath, aws_vault_name, archive_hash, aws_account_id, args.dummy_upload)
                if upload_status:
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
                    print logging.error("Failed to upload {0}".format(backup_subdir_rel_filename))
            else:
                print logging.info("Skipped uploading {0} - archive has not changed".format(
                    backup_subdir_rel_filename))

            # Delete the temporary archive
            logging.info("Removing temporary archive")
            os.remove(tmp_archive_fullpath)

            # Find archives older than three months, with three more recent versions
            # available
            # This could only be the case when we've uploaded a new version of an archive, thereby
            # making an old version irrelevant - so we only need to look for archives with this path.
            if not args.no_prune:
                old_archives = backupmongo.get_old_archives(db, backup_subdir_rel_filename, aws_vault_arn)
                for arch in old_archives:
                    logging.info("Marking archive with ID {0} as redundant").format(arch["_id"])
                    backupmongo.mark_archive_for_deletion(db, arch["_id"])
            else:
                logging.info("Not marking old versions")

        # Delete the temporary directory.
        logging.info("Removing temporary working folder")
        os.rmdir(temp_dir)

    if not args.no_prune:
        # Find and delete old archives
        logging.info("Skipping archive pruning - '--no-prune' supplied.")
        delete_redundant_archives(db, aws_vault_name, aws_account_id)


    # Finished with the database
    logging.info("Closing MongoDB database")
    db_client.close()

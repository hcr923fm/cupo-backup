__author__ = 'Callum McLean <calmcl1@aol.com>'
__version__= '0.1.0'

import os, os.path
import json
import subprocess
import tempfile
import botocore.utils
import datetime, time
import logging

import cupocore


# TODO:50 Move old archive detection into own method, and add unique path detection, so not only triggered when adding new archives.
# TODO:20 Add network rate limiting issue:1
# TODO:0 Add a way of specifying the amount of redundant backups that should be kept issue:2
# TODO:30 Specify the minimum amount of time that a backup should be kept for if there are more than <min amount> of backups remaining. issue:3
# DONE:10 Use a config file! issue:5


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
            logging.debug("Adding to archive list: {0}".format(c))
            files.append(fpath)

    if files:  # No point creating empty archives!
        archive_file_path = os.path.join(tmpdir, os.path.basename(subdir)) + ".7z"

        logging.info("Archiving %s to %s" % (subdir, archive_file_path))
        try:
            devnull = open(os.devnull, "w")
            subprocess.check_call(
                ["7z", "a", "-t7z", archive_file_path, os.path.join(full_backup_path, "*"),
                 "-m0=BZip2", "-y", "-aoa", "-xr-!*/", "-xr-!*sync-conflict*",
                 "-xr-!*desktop.ini", "-xr-!*.tmp", "-xr-!*thumbs.db"], stdout=devnull, stderr=devnull)
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
                cmd = ["aws", "glacier", "upload-archive", "--account-id", aws_account_id]
                if aws_profile: cmd.extend(["--profile", aws_profile])
                cmd.extend(["--checksum", archive_treehash, "--vault-name",
                            aws_vault,"--body", archive_path])
                aws_cli_op = subprocess.check_output(cmd, stderr=devnull)
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
            logging.info("Dummy upload - not actually uploading archive!")
            aws_params = {}
            aws_params["archiveId"] = "{0}-hcrbackup-{1}".format(aws_vault, time.time())
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
        cmd = ["aws", "glacier", "delete-archive", "--account-id", aws_account_id]
        if aws_profile: cmd.extend(["--profile", aws_profile])
        cmd.extend(["--archive-id", archive_id])

        aws_cli_op = subprocess.check_output(cmd, stderr=devnull)

        logging.info("Successfully deleted archive from AWS")
        return 1

    except subprocess.CalledProcessError, e:
        logging.error("Failed to delete archive from AWS! Error: {err.returncode}\n \
                      \t{err.message}\n\t{err.cmd}\n\t{err.output}".format(err=e))
        return None

def delete_redundant_archives(db, aws_vault_name, aws_account_id):
    redundant_archives = cupocore.mongoops.get_archives_to_delete(db)
    for arch in redundant_archives:
        deleted_aws = delete_aws_archive(arch["_id"], aws_vault_name, aws_account_id)
        if deleted_aws:
            cupocore.mongoops.delete_archive_document(db,arch["_id"])
            logging.debug("Deleted archive with ID {0} from local database".format(arch["_id"]))
        else:
            logging.info("AWS deletion failed; not removing database entry")

def compare_files(length_a, hash_a, length_b, hash_b):
    return (length_a == length_b) & (hash_a == hash_b)


def list_dirs(top_dir):
    # Find all of the subdirectories in a given directory.
    logging.info("Finding subdirectories of {0}".format(top_dir))
    dirs = []
    for dirname, subdirs, files in os.walk(top_dir):
        for s in subdirs:
            dirs.append(os.path.relpath(os.path.join(dirname, s), top_dir))
            logging.debug("Found subdirectory {0}".format(s))
    return dirs

def add_new_vault(db, vault_name):
    logging.info("Creating new vault: {0}".format(vault_name))
    devnull = open(os.devnull, "w")
    try:
        cmd = ["aws", "glacier", "create-vault", "--account-id", aws_account_id]
        if aws_profile: cmd.extend(["--profile", aws_profile])
        cmd.extend(["--vault-name", vault_name])
        aws_cli_op = subprocess.check_output(cmd, stderr=devnull)

        # Returned fields from create-vault:
        # location -> (string)
        # The URI of the vault that was created.
        aws_cli_op = aws_cli_op.replace("\r\n", "")
        aws_cli_op_json = json.loads(aws_cli_op)
        aws_vault_arn = aws_cli_op_json["location"]

        logging.info("Successfully created AWS vault {0}:\n {1}".format(vault_name, aws_vault_arn))
        cupocore.mongoops.create_vault_entry(db, aws_vault_arn, vault_name)
        logging.info("Created database entry for vault {0}".format(vault_name))
        return 1

    except subprocess.CalledProcessError, e:
        logging.error("Failed to create new vault. Error: {err.returncode}\n \
                      \t{err.message}\n\t{err.cmd}\n\t{err.output}".format(err=e))
        return None


if __name__ == "__main__":

    args = cupocore.cmdparser.parse_args()

    logFormat = """%(asctime)s:%(levelname)s:%(module)s: %(message)s"""
    if args.debug:
        logging.basicConfig(filename=os.path.join(args.logging_dir,'.cupoLog'),
                            level=logging.DEBUG, format=logFormat)
    else:
        logging.basicConfig(filename=os.path.join(args.logging_dir,'.cupoLog'),
                            level=logging.INFO, format=logFormat)

    aws_account_id = args.account_id
    db_name = args.database
    aws_profile = args.aws_profile or None
    db_client, db = cupocore.mongoops.connect(db_name)

    # If we're just adding a new vault...
    # DONE:0 There has to be a better way to process the args command...
    if args.subparser_name == "new-vault":
        if args.new_vault_name:
            add_new_vault(db, args.new_vault_name)
            exit()

    # Top of directory to backup
    root_dir = args.backup_directory
    if not os.path.exists(root_dir):
        raise ValueError("%s does not exist" % root_dir)

    aws_vault_name = args.backup_vault_name

    if not args.no_backup:

        # Temporary directory to create archives in
        temp_dir = tempfile.mkdtemp()
        logging.info("Created temporary directory at {0}".format(temp_dir))

        logging.info("Backing up {0} to {1} using AWS Account ID {2}".format(
            root_dir, aws_vault_name, aws_account_id))

        subdirs_to_backup = list_dirs(root_dir)  # List of subtrees, relative to root_dir
        subdirs_to_backup.append("")  # TODO:40 Dammit I will get this working - get the root directory contents to be zipped issue:4

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
            most_recent_version = cupocore.mongoops.get_most_recent_version_of_archive(db, backup_subdir_rel_filename)

            if most_recent_version:
                logging.info("Archive for this path exists in local database")
                hash_remote = most_recent_version['treehash']
                size_remote = most_recent_version['size']

            else:
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
                    aws_vault_arn = cupocore.mongoops.get_vault_by_name(db, aws_vault_name)["arn"]

                    # Store the info about the newly uploaded file in the database
                    cupocore.mongoops.create_archive_entry(db,
                                                     backup_subdir_rel_filename,
                                                     aws_vault_arn,
                                                     upload_status["archiveId"],
                                                     archive_hash,
                                                     size_arch,
                                                     upload_status["location"])
                else:
                    logging.error("Failed to upload {0}".format(backup_subdir_rel_filename))
            else:
                logging.info("Skipped uploading {0} - archive has not changed".format(
                    backup_subdir_rel_filename))

            # Delete the temporary archive
            logging.info("Removing temporary archive")
            os.remove(tmp_archive_fullpath)

            # Find archives older than three months, with three more recent versions
            # available
            # This could only be the case when we've uploaded a new version of an archive, thereby
            # making an old version irrelevant - so we only need to look for archives with this path.
            if not args.no_prune:
                old_archives = cupocore.mongoops.get_old_archives(db, backup_subdir_rel_filename, aws_vault_name)
                for arch in old_archives:
                    logging.info("Marking archive with ID {0} as redundant".format(arch["_id"]))
                    cupocore.mongoops.mark_archive_for_deletion(db, arch["_id"])
            else:
                logging.info("Not marking old versions")

        # Delete the temporary directory.
        logging.info("Removing temporary working folder")
        os.rmdir(temp_dir)

    else:
        logging.info("Skipping file backup - '--no-backup' supplied.")

    if not args.no_prune:
        # Find and delete old archives
        logging.info("Deleting redundant archives")
        delete_redundant_archives(db, aws_vault_name, aws_account_id)
    else:
        logging.info("Skipping archive pruning - '--no-prune' supplied.")


    # Finished with the database
    logging.info("Closing MongoDB database\r\n\r\n")
    db_client.close()

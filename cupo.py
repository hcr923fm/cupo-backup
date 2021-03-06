import os, os.path
import subprocess
import tempfile
import zipfile
import botocore.utils, botocore.exceptions
import boto3
import logging, logging.handlers
import cupocore
import shutil

__author__ = 'Callum McLean <calmcl1@aol.com>'
__version__ = '0.1.0'


# TODO-refactor: Move old archive detection into own method, and add unique path detection, so not only triggered when
#  adding new archives.
# TODO-ratelimit: #1 Add network rate limiting
# TODO-backupscount: #2 Add a way of specifying the amount of redundant backups that should be kept
# TODO-backupsage: #3 Specify the minimum amount of time that a backup should be kept for if there are more than
# <min amount> of backups remaining.


# Only the *files* in a given directory are archived, not the subdirectories.
# The contents of the subdirectories live in archives of their own (except for any directories that *they* contain)
# This means that only the changed files in a given directory need to be checked - and that if a file in a
# sub-sub-subdirectory is changed, the whole parent directory doesn't need to be re-uploaded.
# The name of each archive is equal to the name of the directory.

def archive_directory(top_dir, subdir, tmpdir):
    """
    .. function:: archive_directory(top_dir, subdir, tmpdir)

    Given a sub-directory name under the root directory to be archived, archive the contents of the sub-directory
    to a temporary directory. Then return the full path to the temporary directory.
    :param top_dir: The root path that will be archived and uploaded to Glacier.
    :param subdir: The path to the subdirectory that is being archived here, relative to `top_dir`
    :param tmpdir: The path to the temporary directory to store archives in until they are uploaded to Glacier
    :return: If the subdirectory contains files, then the full path to the temporary archives; otherwise, None
    """
    # We're only archiving the *files* in this directory, not the subdirectories.

    files = []
    full_backup_path = os.path.join(top_dir, subdir)
    dir_contents = os.listdir(full_backup_path)

    # Only add files to 'files' list, not subdirs
    for c in dir_contents:
        fpath = os.path.join(full_backup_path, c)
        if os.path.isfile(fpath) and not fpath.endswith(".ini"):
            # logger.info("Adding to archive list: {0}".format(c))
            files.append(fpath)

    if not files:
        # No point creating empty archives!
        return None

    try:
        os.makedirs(os.path.join(tmpdir, subdir))
    except Exception:
        pass

    files.sort()

    devnull = open(os.devnull, "wb")

    archive_list = []
    cur_arch_suffix = 1

    try:
        while files:
            archive_file_path = "{0}.{1:08d}.zip".format(os.path.join(tmpdir, subdir), cur_arch_suffix)
            logger.info("Archiving %s to %s" % (subdir, archive_file_path))

            #with tarfile.open(archive_file_path, "w:gz") as arch_tar:
            arch_zip = zipfile.ZipFile(open(archive_file_path, "wb"), "w", allowZip64=True)
            for i in xrange(0, int(args.max_files)):
                try:
                    f = files.pop()
                    logger.info(
                        "Adding {0} to archive {1} ({2}/{3})".format(f, archive_file_path, i + 1, args.max_files))
                    arch_zip.write(f, os.path.basename(f))
                except IndexError, e:
                    # Run out of files, exit loop
                    logger.info("Completed adding files to archive")
                    arch_zip.close()
                    break

            archive_list.append(archive_file_path)
            cur_arch_suffix += 1

        return archive_list

    except Exception, e:
        logger.error("Failed to create archive: {0}".format(e.message))
        logger.debug("Error args: {0}".format(e.args))
        return None


def delete_aws_archive(archive_id, aws_vault):
    logger.info("Deleting archive with id {0} from vault {1}".format(
        archive_id, aws_vault))

    try:
        boto_client.delete_archive(vaultName=aws_vault,
                                   archiveId=archive_id)

        logger.info("Successfully deleted archive from AWS")
        return 1

    except botocore.exceptions.ConnectionClosedError:
        logger.error("AWS archive removal failed - connection to AWS server was unexpectedly closed")
        return None

    except botocore.exceptions.EndpointConnectionError:
        logger.error("AWS archive removal failed - unable to connect to AWS server")
        return None

    except botocore.exceptions.ClientError, e:
        logger.error("AWS archive removal failed - {0}".format(e.response["Error"]["Message"]))
        return None

    except botocore.exceptions.BotoCoreError, e:
        logger.error("AWS archive removal failed - {0}".format(e.message))
        return None

    except Exception, e:
        logger.error("AWS archive removal failed - {0}".format(e.message))


def delete_redundant_archives(db, aws_vault_name):
    redundant_archives = cupocore.mongoops.get_archives_to_delete(db)
    for arch in redundant_archives:
        deleted_aws = delete_aws_archive(arch["_id"], aws_vault_name)
        if deleted_aws:
            cupocore.mongoops.delete_archive_document(db, arch["_id"])
            logger.info("Deleted archive with ID {0} from local database".format(arch["_id"]))
        else:
            logger.info("AWS deletion failed; not removing database entry")


def compare_files(length_a, hash_a, length_b, hash_b):
    return (length_a == length_b) & (hash_a == hash_b)


def list_dirs(top_dir):
    # Find all of the subdirectories in a given directory.
    logger.info("Finding subdirectories of {0}".format(top_dir))
    dirs = []
    for dirname, subdirs, files in os.walk(top_dir):
        for s in subdirs:
            dirs.append(os.path.relpath(os.path.join(dirname, s), top_dir))
            logger.info("Found subdirectory {0}".format(os.path.join(dirname, s), top_dir))
    return dirs


def add_new_vault(db, aws_account_id, vault_name):
    logger.info("Creating new vault: {0}".format(vault_name))
    devnull = open(os.devnull, "w")
    try:

        response = boto_client.create_vault(accountId=aws_account_id,
                                            vaultName=vault_name)

        # Returned fields from create-vault:
        # location -> (string)
        # The URI of the vault that was created.
        aws_vault_arn = response["location"]
        logger.info("Successfully created AWS vault {0}:\n {1}".format(vault_name, aws_vault_arn))
        return cupocore.mongoops.create_vault_entry(db, aws_vault_arn, vault_name)

    except botocore.exceptions.ConnectionClosedError, e:
        logger.error("AWS vault creation failed - connection to AWS server was unexpectedly closed")
        return None

    except botocore.exceptions.EndpointConnectionError, e:
        logger.error("AWS vault creation failed - unable to connect to AWS server")
        return None

    except botocore.exceptions.ClientError, e:
        logger.error("AWS vault creation failed - {0}".format(e.response["Error"]["Message"]))
        return None

    except botocore.exceptions.BotoCoreError, e:
        logger.error("AWS vault creation failed - {0}".format(e.message))
        return None

    except Exception, e:
        logger.error("AWS vault creation failed - {0}".format(e.message))

    finally:
        devnull.close()


def init_logging():
    # Set up some logs - one rotating log, which contains all the debug output
    # and a STDERR log at the specified level.

    logger = logging.getLogger("cupobackup{0}".format(os.getpid()))
    logger.setLevel(logging.DEBUG)

    log_rotating = logging.handlers.RotatingFileHandler(filename=os.path.join(args.logging_dir, '.cupoLog'),
                                                        maxBytes=10485760,  # 10MB
                                                        backupCount=5)
    log_stream = logging.StreamHandler()

    log_rotate_formatter = logging.Formatter("""%(asctime)-26s : %(levelname)s : %(module)s : %(message)s""")
    log_stream_formatter = logging.Formatter("""%(levelname)s : %(message)s""")
    log_rotating.setFormatter(log_rotate_formatter)
    log_stream.setFormatter(log_stream_formatter)
    log_rotating.setLevel(logging.INFO)

    if args.debug:
        log_stream.setLevel(logging.DEBUG)
    else:
        log_stream.setLevel(logging.INFO)

    logger.addHandler(log_rotating)
    logger.addHandler(log_stream)
    return logger


def print_file_list(db, vault_name):
    paths = cupocore.mongoops.get_list_of_paths_in_vault(db, vault_name)

    print "Vault: {0}".format(vault_name)
    print "\tARN: {0}".format(cupocore.mongoops.get_vault_by_name(db, vault_name)["arn"])
    print "\tFiles available:"

    for p in paths:
        print "\t\t{0}".format(p)


def init_job_retrieval(db, vault_name, archive_id, download_location):
    # TODO-retrieval #8 Make job retrieval work
    raise NotImplementedError

    job_params = {
        "Format": "JSON",
        "Type": "archive-retrieval",
        "ArchiveID": archive_id
    }
    init_job_ret = boto_client.initiate_job(accountId=args.account_id,
                                            vaultName=args.vault_name,
                                            jobParameters=job_params)

    if init_job_ret:
        cupocore.mongoops.create_retrieval_entry(db,
                                                 cupocore.mongoops.get_vault_by_name(db, vault_name)["arn"],
                                                 init_job_ret["jobId"],
                                                 init_job_ret["location"],
                                                 download_location)


if __name__ == "__main__":

    # Parse the options from the command line and from the config file too.
    # Options specified on the command line will override anything specified
    # in the config file.

    args = cupocore.cmdparser.parse_args()

    # Start the logger
    logger = init_logging()

    # If we're only spitting out a sample config file...

    if args.subparser_name == "sample-config":
        cupocore.cmdparser.create_config_file(args.sample_file_location)
        exit()

    # On we go!

    if not hasattr(args, "account_id") or not args.account_id:
        logger.error(
            "AWS account ID has not been supplied. Use '--account-id' or specify the 'account_id' option in a config file.")
        exit(1)
    if not hasattr(args, "database") or not args.database:
        logger.error(
            "MongoDB database has not been supplied. Use '--database' or specify the 'database' option in a config file.")
        exit(1)

    db_client, db = cupocore.mongoops.connect(args.database)

    boto_session = boto3.Session(profile_name=args.aws_profile)
    boto_client = boto_session.client('glacier')

    # If we're only adding a new vault...

    if args.subparser_name == "new-vault":
        if args.new_vault_name:
            add_new_vault(db, args.account_id, args.new_vault_name)
        else:
            logger.error("New vault name not supplied. Cannot create vault.")
        exit()

    if args.temp_dir:
        tempfile.tempdir = args.temp_dir

    # If we're retrieving existing backups...
    elif args.subparser_name == "retrieve":
        if args.list_uploaded_archives:
            print_file_list(db, args.vault_name)
            exit()
        else:
            archive_list = cupocore.mongoops.get_archive_by_path(db, args.vault_name, args.top_path, True)

            if len(archive_list):
                for arch in archive_list:
                    init_job_retrieval(db, args.vault_name, arch["_id"], args.download_location)
            logger.critical("This hasn't been implemented yet D: - TODO: INITIATE JOB RETRIEVAL")

    # Top of directory to backup
    root_dir = args.backup_directory
    if not os.path.exists(root_dir):
        raise ValueError("%s does not exist" % root_dir)

    aws_vault_name = args.vault_name

    if not args.no_backup:

        # Temporary directory to create archives in
        temp_dir = tempfile.mkdtemp()
        logger.info("Created temporary directory at {0}".format(temp_dir))

        logger.info("Backing up {0} to {1} using AWS Account ID {2}".format(
            root_dir, aws_vault_name, args.account_id))

        subdirs_to_backup = list_dirs(root_dir)  # List of subtrees, relative to root_dir
        subdirs_to_backup.append(
            "")  # TODO-archiveroot: #4 Dammit I will get this working - get the root directory contents to be zipped

        upload_mgr = cupocore.uploadmanager.UploadManager(db, boto_client, aws_vault_name)

        for subdir_to_backup in subdirs_to_backup:
            # Archive each folder in the list to it's own (series of) zip file(s)
            tmp_archive_fullpath_list = archive_directory(root_dir, subdir_to_backup, temp_dir)

            if not tmp_archive_fullpath_list:
                # Directory was empty - not being archived
                continue
            for tmp_archive_fullpath in tmp_archive_fullpath_list:

                backup_subdir_abs_filename = os.path.join(root_dir, subdir_to_backup,
                                                          os.path.basename(tmp_archive_fullpath))
                backup_subdir_rel_filename = os.path.join(subdir_to_backup, os.path.basename(tmp_archive_fullpath))
                # Calculate the treehash of the local archive
                with open(tmp_archive_fullpath, 'rb') as arch_f:
                    # archive_hash = calculate_tree_hash(arch_f)
                    archive_hash = botocore.utils.calculate_tree_hash(arch_f)

                # Find most recent version of this file in Glacier
                most_recent_version = cupocore.mongoops.get_most_recent_version_of_archive(db, aws_vault_name,
                                                                                           backup_subdir_rel_filename)

                if most_recent_version:
                    logger.info("Archive for this path exists in local database")
                    hash_remote = most_recent_version['treehash']
                    size_remote = most_recent_version['size']

                else:
                    logger.info("No archive found for this path in local database")
                    hash_remote = size_remote = None

                # Compare it against the local copy of the Glacier version of the archive
                size_arch = os.path.getsize(tmp_archive_fullpath)

                # If the hashes are the same - don't upload the archive; it already exists
                if not compare_files(size_arch, archive_hash, size_remote, hash_remote):
                    logger.info("Uploading {0} to vault {1}".format(tmp_archive_fullpath, aws_vault_name))
                    if not args.dummy_upload:
                        print "tmp_archive_fullpath: {0}\n \
                        os.path.dirname: {1}".format(tmp_archive_fullpath, backup_subdir_rel_filename)
                        upload_mgr.initialize_upload(tmp_archive_fullpath, backup_subdir_rel_filename,
                                                     archive_hash, size_arch)
                    else:
                        # This is a dummy upload, for testing purposes. Create a fake
                        # AWS URI and location, but don't touch the archive.
                        logger.info("Dummy upload - not actually uploading archive!")

                else:
                    logger.info("Skipped uploading {0} - archive has not changed".format(
                        backup_subdir_rel_filename))

                # Find archives older than three months, with three more recent versions
                # available
                # This could only be the case when we've uploaded a new version of an archive, thereby
                # making an old version irrelevant - so we only need to look for archives with this path.
                if not args.no_prune:
                    old_archives = cupocore.mongoops.get_old_archives(db, backup_subdir_rel_filename, aws_vault_name)
                    for arch in old_archives:
                        logger.info("Marking archive with ID {0} as redundant".format(arch["_id"]))
                        cupocore.mongoops.mark_archive_for_deletion(db, arch["_id"])
                else:
                    logger.info("Not marking old versions")
        # Wait for uploads to complete
        upload_mgr.wait_for_finish()

        # Delete the temporary directory.
        logger.info("Removing temporary working folder")
        shutil.rmtree(temp_dir)

    else:
        logger.info("Skipping file backup - '--no-backup' supplied.")

    if not args.no_prune:
        # Find and delete old archives
        logger.info("Deleting redundant archives")
        delete_redundant_archives(db, aws_vault_name)
    else:
        logger.info("Skipping archive pruning - '--no-prune' supplied.")

    # Finished with the database
    logger.info("Closing MongoDB database\r\n\r\n")
    db_client.close()

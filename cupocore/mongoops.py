import pymongo, pymongo.errors
import time, datetime
import logging, os

logger = logging.getLogger("cupobackup{0}.mongoOps".format(os.getpid()))


# Use the format:
#
# db.archives.insert_one({
#     "path": "/path/to/archived/subdir",
#     "vault_arn": "aws://vault_arn"
#     "_id": "AWS-ARCHIVE-ID-GOES-HERE-ABCDEFHGIJKLMNOPQRSTUVWXYZ0123456789",
#     "treehash": "SHA256-TREEHASH-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#     "size": 123456789,
#     "uploaded_time": 147258369,
#     "aws_URI": "aws://AWS-ARCHIVE-URI-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#     "to_delete": False
# })
#
#
# db.vaults.insert_one({
#     "arn": "aws://AWS-VAULT-ARN-123456789",
#     "name": "vault name",
# })
#
#
# db.jobs.insert_one({
#     "vault_arn":                    "aws://AWS-VAULT-ARN-123456789",
#     "_id":                          "AWS-JOB-ID-abcdefghijklmnopqrstuvwxyz"
#     "job_type":                     "retrieval"
#     "job_retrieval_destination":    "/path/to/download" Only if job_type is 'retrieval'
#     "job_last_polled_time":         0123456789
# })


def create_backup_database(database_name, db_client, drop_existing=True):
    """
    Creates a MongoDB database `database_name` that is ready to be used as a backup tracking
    database for this software.
    :param database_name The name of the database we'll use
    :param host The hostname of the server running MongoDB
    :param port The port number to connect to `host` on
    :param drop_existing If true, drop any existing database with this name
    """

    if drop_existing and (database_name in db_client.database_names()):
        db_client.drop_database(database_name)

    try:
        db = db_client[database_name]
        db.create_collection('archives')
        db.create_collection('vaults')
        db.create_collection('jobs')
        db.create_collection('mparts')

        return db

    except pymongo.errors.ConnectionFailure, e:
        logger.error("Database creation failed - lost connection to MongoDB instance")
    except pymongo.errors.ExecutionTimeout, e:
        logger.error("Database creation failed - operation execution timed out")
    except pymongo.errors.WriteError, e:
        logger.error("Database creation failed - could not write to database")
    except pymongo.errors.PyMongoError, e:
        logger.error("Database creation failed - MongoDB error '{0}'".format(e.message))
    except Exception, e:
        logger.error("Database creation failed - '{0}'".format(e.message))
    finally:
        return None


def create_vault_entry(db, vault_arn, vault_name):
    # Check first to see if there's already a vault by this name.
    existing_vault_entry = db["vaults"].find_one({"name": vault_name})
    # If so, return that instead.
    if existing_vault_entry: return existing_vault_entry["_id"]

    # If not, let's create one with the specified info...
    doc_vault = {}
    doc_vault["arn"] = vault_arn
    doc_vault["name"] = vault_name

    return db["vaults"].insert_one(doc_vault).inserted_id


def create_archive_entry(db, archived_dir_path, vault_arn, aws_archive_id,
                         archive_treehash, archive_size, aws_uri):
    # Find an entry in the archives list that matches the path and vault arn
    # that we are uploading to..
    doc_arch = {}
    doc_arch["path"] = archived_dir_path
    doc_arch["vault_arn"] = vault_arn
    doc_arch["_id"] = aws_archive_id
    doc_arch["treehash"] = archive_treehash
    doc_arch["size"] = archive_size
    doc_arch["uploaded_time"] = time.time()
    doc_arch["aws_URI"] = aws_uri
    doc_arch["to_delete"] = 0

    # Add the entry.
    return db['archives'].insert(doc_arch)


def create_mpart_part_entry(db, vault_arn, uploadId, first_byte, last_byte, tmp_archive_location, arch_size,
                            arch_checksum, subdir_rel_path):
    doc_mpart = {}
    doc_mpart["uploadId"] = uploadId
    doc_mpart["is_active"] = False
    doc_mpart["first_byte"] = first_byte
    doc_mpart["last_byte"] = last_byte
    doc_mpart["tmp_archive_location"] = tmp_archive_location
    doc_mpart["full_size"] = arch_size
    doc_mpart["full_hash"] = arch_checksum
    doc_mpart["subdir_rel_path"] = subdir_rel_path


    return db["mparts"].insert(doc_mpart)


# TODO: Make this correct
def get_oldest_inactive_mpart_entry(db, vault_name):
    vault = get_vault_by_name(db, vault_name)
    return db["mparts"].find_one(
        {"is_active": False},
        sort=[('first_byte', pymongo.ASCENDING)])


def set_mpart_active(db, mpart_id):
    db["mparts"].find_one_and_update({"_id": mpart_id},
                                     {"$set":
                                          {"is_active": True}
                                      })


def set_mpart_inactive(db, mpart_id):
    db["mparts"].find_one_and_update({"_id": mpart_id},
                                     {"$set":
                                          {"is_active": False}
                                      })


def delete_mpart_entry(db, mpart_id):
    return db["mparts"].delete_one({"_id": mpart_id})


def is_existing_mparts_remaining(db, vault_name, uploadId):
    vault = get_vault_by_name(db, vault_name)
    p = db["mparts"].find_one(
        {"uploadId": uploadId})
    if not p:
        return False
    else:
        return True


def create_retrieval_entry(db, vault_arn, archive_id, aws_job_id, aws_job_location, download_path):
    doc_entry = {}
    doc_entry["_id"] = aws_job_id
    doc_entry["location"] = aws_job_location
    doc_entry["vault_arn"] = vault_arn
    doc_entry["job_type"] = "retrieval"
    doc_entry["job_retrieval_destination"] = download_path
    doc_entry["archive_id"] = archive_id
    doc_entry["job_last_polled_time"] = time.time()

    return db['jobs'].insert_one(doc_entry)


def delete_retrieval_entry(db, entry_id):
    return db["jobs"].delete_one({"_id": entry_id})


def get_oldest_retrieval_entry(db, vault_name):
    vault = get_vault_by_name(db, vault_name)
    return db["archives"].find_one(
        {"job_type": "retrieval"},
        sort=[('uploaded_time', pymongo.ASCENDING)])


def get_list_of_paths_in_vault(db, vault_name):
    vault = get_vault_by_name(db, vault_name)
    archives = db["archives"].distinct("path", {"vault_arn": vault["arn"]})

    return archives


def get_most_recent_version_of_archive(db, vault_name, path):
    vault_arn = get_vault_by_name(db, vault_name)["arn"]
    return db["archives"].find_one(
        {"path": path, "to_delete": 0, "vault_arn": vault_arn},
        sort=[('uploaded_time', pymongo.DESCENDING)])


def get_old_archives(db, archived_dir_path, vault_name):
    vault_arn = get_vault_by_name(db, vault_name)["arn"]

    deadline_dt = datetime.datetime.utcnow() - datetime.timedelta(days=93)
    deadline_ts = time.mktime(deadline_dt.timetuple())
    cursor = db["archives"].find({"to_delete": 0,
                                  "path": archived_dir_path,
                                  "uploaded_time":
                                      {"$lt": deadline_ts}
                                  },
                                 sort=[("uploaded_time", pymongo.DESCENDING)],
                                 skip=3)

    old_archives = []
    for arch in cursor: old_archives.append(arch)
    return old_archives


def mark_archive_for_deletion(db, archive_id):
    db["archives"].find_one_and_update({"_id": archive_id},
                                       {"$set":
                                            {"to_delete": 1}
                                        })


def get_archives_to_delete(db):
    cursor = db["archives"].find({"to_delete": 1})
    redundant_archives = []
    for arch in cursor:
        redundant_archives.append(arch)

    return redundant_archives


def get_archive_by_path(db, vault_name, path, retrieve_subpath_archs=False):
    """
    Will attempt to find the most recent version of an archive representing a given path.
    If retrieve_subpath_archs is True, then will also retrieve latest versions of archives representing
    subdirs of the path.
    :param path: The path whose contents we want to retrieve, relative to the top_dir that was backed up.
    :param retrieve_subpath_archs: If True, will return a list of all of the archives of subdirectories below the `path`
    in the directory tree
    :return: archive, list
    """

    if not retrieve_subpath_archs:
        get_most_recent_version_of_archive(db, vault_name, path)

    else:
        # When trying to find subdirectories, the daft assumption that we make is that the 'path' of the archive will
        # start with `path` and be longer than `path`. It'll work for now, but seems inelegant...

        path_list = get_list_of_paths_in_vault(db, vault_name)

        subdir_list = []
        while len(path_list):
            cur_path = path_list.pop()
            if cur_path.startswith(path) and len(cur_path) >= len(path):
                subdir_list.append(cur_path)

        arch_list = []
        for subdir in subdir_list:
            arch = get_most_recent_version_of_archive(db, vault_name, subdir)
            if arch: arch_list.append(arch)

        return arch_list


def get_archive_by_id(db, archive_id):
    return db["archives"].find_one({"_id": archive_id})


def delete_archive_document(db, archive_id):
    db["archives"].find_one_and_delete({"_id": archive_id})


def get_vault_by_name(db, vault_name):
    return db['vaults'].find_one({"name": vault_name})


def get_vault_by_arn(db, vault_arn):
    return db['vaults'].find_one({"arn": vault_arn})


def connect(database_name, host="localhost", port=27017):
    mongodb_uri = "{host}:{port}".format(host=host, port=port)
    client = pymongo.MongoClient(mongodb_uri)

    if database_name in client.database_names():
        db = client[database_name]
    else:
        db = create_backup_database(database_name, client)

    return client, db


def disconnect(client):
    client.close()

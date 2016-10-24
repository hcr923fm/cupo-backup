import argparse
import os, os.path
import json
import logging


class cmdOptions():
    pass


def __parse_cmd_args(options_namespace):
    arg_parser = argparse.ArgumentParser(
        description='A tool to manage differential file uploads to an Amazon Glacier repository')
    arg_parser.add_argument('--account-id', "-i", help='The AWS ID of the account that owns the specified vault',
                            metavar='aws_acct_id')
    arg_parser.add_argument('--aws-profile', "-p",
                            help='If supplied, the "--profile" switch will be passed to the AWS CLI for credential management.')
    arg_parser.add_argument('--database', "-d",
                            help='The database name to connect to.',
                            metavar='DB_NAME')
    arg_parser.add_argument('--debug', "-v",
                            help='If passed, the default logging level will be set to DEBUG.',
                            action='store_true')
    arg_parser.add_argument('--logging-dir', "-l",
                            help='The log will be stored in this directory, if passed.',
                            default=os.path.expanduser('~'))
    arg_parser.add_argument('--config-file', "-c",
                            help='Loads options from a config file.')

    subparsers = arg_parser.add_subparsers(help="Run HCRBackup [ backup | new-vault ] --help for more info on each command.",
                                           dest="subparser_name")
    arg_parser_backup = subparsers.add_parser('backup', help="Execute incremental backup of a directory to an Amazon Glacier \
                                              vault, and prune any outdated archives.")
    arg_parser_backup.add_argument('backup_directory', help='The top directory to back up', metavar='top_dir')
    arg_parser_backup.add_argument('backup_vault_name', help='The name of the vault to upload the archive to',
                                   metavar='vault_name')
    arg_parser_backup.add_argument('--no-backup',
                                   help='If passed, the backup operation will not take place, going straight to the maintenance operations',
                                   action='store_true')
    arg_parser_backup.add_argument('--no-prune',
                                   help='If passed, the process of finding and removing old archives will not take place.',
                                   action='store_true')
    arg_parser_backup.add_argument('--dummy-upload',
                                   help='If passed, the archives will not be uploaded, but a dummy AWS URI and archive ID will be generated. Use for testing only.',
                                   action='store_true')

    arg_parser_retrieve = subparsers.add_parser('retrieve', help="Retrieve a \
     directory tree from the specified vault and download it to the local system.")
    arg_parser_retrieve.add_argument('vault_name', help='The name of the vault to download from.')
    arg_parser_retrieve.add_argument('top_path', help="The relative directory of the top directory to download.\
                                     Use --list for a list of directories available.")
    arg_parser_retrieve.add_argument('download_location', help="The local directory to download the file tree to.")
    arg_parser_retrieve.add_argument('--list', help="Print a list of the directories available for download.",
                                     action='store_true', dest="list_uploaded_archives")

    arg_parser_new_vault = subparsers.add_parser('new-vault', help="Add a new \
     vault to the specified Glacier account, and register it with the local database.")
    arg_parser_new_vault.add_argument('new_vault_name', help='The name of the new vault to create.',
                                      metavar='new_vault_name')

    arg_parser_new_config = subparsers.add_parser('sample-config', help="Create a sample configuration file \
    that can be passed to Cupo by --config-file.")
    arg_parser_new_config.add_argument('sample_file_location', help='The path and filename for the generated \
                                       config file.', metavar='config_location')

    args = arg_parser.parse_args(namespace=options_namespace)


def __load_config_file_args(config_file_path, options_namespace):
    with open(config_file_path) as conf_f:
        config_opts = json.load(conf_f)

    for k in config_opts.viewkeys():
        # Don't overwrite options that have been explicitly set on the command line
        if not hasattr(options_namespace, k) or not getattr(options_namespace, k):
            # Add the config option to the global options object
            setattr(options_namespace, k, config_opts[k])


def parse_args():
    # Creating a separate object so that both argparse'd switches and config file
    # options can be accessed from the same object
    cmd_opts = cmdOptions()
    __parse_cmd_args(cmd_opts)
    if hasattr(cmd_opts, "config_file") and cmd_opts.config_file:
        print "Config file:", cmd_opts.config_file
        if os.path.exists(cmd_opts.config_file):
            __load_config_file_args(cmd_opts.config_file, cmd_opts)
        else:
            logging.error(
                "Config file does not exist. Use '--config-file' to specify a location or create a file at ~/.cupo.json")
            exit(1)
    return cmd_opts


def create_config_file(file_location):
    config_opts = {"database": "",
                   "vault_name": "",
                   "account_id": "",
                   "aws_profile": "",
                   "debug": False,
                   "logging_dir": "",
                   "backup_directory": ""
                   }

    with open(file_location, "w") as f:
        json.dump(config_opts, f, indent=4, separators=(",", ": "))

import argparse
import os, os.path

class cmdOptions():
    pass

def __parse_cmd_args():
    arg_parser = argparse.ArgumentParser(description='A tool to manage differential file uploads to an Amazon Glacier repository')
    arg_parser.add_argument('--account-id', help='The AWS ID of the account that owns the specified vault',
                            metavar='aws_acct_id', default='-', required=True)
    arg_parser.add_argument('--aws-profile',
                            help='If supplied, the "--profile" switch will be passed to the AWS CLI for credential management.',
                            metavar='aws-profile')
    arg_parser.add_argument('--database',
                            help='The database name to connect to.',
                            metavar='db_name', required=True)
    arg_parser.add_argument('--debug',
                            help='If passed, the default logging level will be set to DEBUG.',
                            action='store_true')
    arg_parser.add_argument('--logging-dir',
                            help='The log will be stored in this directory, if passed.',
                            metavar='logging_dir',
                            default=os.path.expanduser('~'))

    subparsers = arg_parser.add_subparsers(help="Run HCRBackup backup|new-vault --help for more info on each command.")
    arg_parser_backup = subparsers.add_parser('backup', help="Execute incremental backup of a directory to an Amazon Glacier \
                                              vault, and prune any outdated archives.")
    arg_parser_backup.add_argument('backup_directory', help='The top directory to back up', metavar='top_dir')
    arg_parser_backup.add_argument('backup_vault_name', help='The name of the vault to upload the archive to', metavar='vault_name')
    arg_parser_backup.add_argument('--no-backup',
                            help='If passed, the backup operation will not take place, going straight to the maintenance operations',
                            action='store_true')
    arg_parser_backup.add_argument('--no-prune',
                            help='If passed, the process of finding and removing old archives will not take place.',
                            action='store_true')
    arg_parser_backup.add_argument('--dummy-upload',
                            help='If passed, the archives will not be uploaded, but a dummy AWS URI and archive ID will be generated. Use for testing only.',
                            action='store_true')

    arg_parser_new_vault = subparsers.add_parser('new-vault', help="Add a new \
     vault to the specified Glacier account, and register it with the local database.")
    arg_parser_new_vault.add_argument('new_vault_name', help='The name of the new vault to create.',
                                      metavar='new_vault_name')


    args = arg_parser.parse_args(namespace=cmd_opts)

def parse_args():
    # Creating a separate object so that both argparse'd switches and config file
    # options can be accessed from the same object
    cmd_opts = cmdOptions()
    __parse_cmd_args()

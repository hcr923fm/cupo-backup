---
permalink: /quick-start/
---
{% include toc %}

# Getting Started
So, you want to start a backup system...

## Installing

### Prerequisites
* Python < 2.7+ *(but not Python 3 - yet)*
* Amazon AWS command-line interface
* MongoDB
* `python-botocore`
* `python-boto3`
* `p7zip-full` *(must be the 'full' version!)*

#### Installing and Configuring Prerequisites
* Use the following command to install most of the dependencies from apt:
`sudo apt-get install python-all python-pip python-botocore python-boto3 p7zip-full`
* Install MongoDB for your system as described [in the MongoDB docs](http://docs.mongodb.com/manual/administration/install-on-linux).
* Once that's completed, install the AWS command-line tool:
`pip install awscli`
* The AWS CLI needs to be configured before it can be used. Run:
`aws --configure`
and supply your AWS credentials.
* MongoDB needs a data directory to run - by default it's `/data/db`. Create this directory, and make sure that the current user is a member of the `mongodb` group, and that the group has full permissions to that directory:
`mkdir -p /data/db; usermod -a -G mongodb <username>; chgrp -R mongodb /data/db`

### Grabbing the Source
Download a copy of the source from [here](https://calmcl1.github.com/cupo-backup/get-cupo).

## Usage

Cupo will take care of most of the process of backing up archives and retrieving them from Glacier. As such, to avoid a complicated command string, it's easiest to use a config file. Cupo can generate a default one for you at a specified location - just run `cupo.py sample-config /path/to/config/file`.

In there, you will find most of the common options that are required to run any Cupo command. To tell Cupo that you are using a config file, use `cupo.py -c /path/to/config/file`.

### Creating a New Vault
To start off, create a new vault in Glacier (or, if a vault that you want to use already exists in AWS, register it in the local database):

`cupo.py -c /path/to/config/file new-vault NEW_VAULT_NAME`, or
`cupo.py --account-id AWS_ACCOUNT_ID --database DATABASE_NAME new-vault NEW_VAULT_NAME`

where:

* `AWS_ACCOUNT_ID` is, unsurprisingly, the account ID associated with your AWS account - a numerical value.
* `DATABASE_NAME` is the name of the MongoDB database that we're using to store the local Glacier archive tracking data in. It will be created if it does not already exist.
* `NEW_VAULT_NAME` is the name of the Glacier vault that we're storing the the backup archives in. It will be created in Glacier if it does not yet exist.

### Backing Up a Directory

Now, it's as simple as specifying a directory to back up and a vault!

`cupo.py -c /path/to/config/file backup` or
`cupo.py --account-id AWS_ACCOUNT_ID --d DATABASE_NAME backup -r TOP_DIR -n VAULT_NAME`

where:

* `TOP_DIR` is the root directory to back up.
* `VAULT_NAME` is the Glacier vault to back up to. It will not be created if it doesn't exist - use `cupo.py new-vault` first.

There isn't much output on the terminal, but a log will created that you can `tail -f` if you wish. The default log location is `~/.CupoLog`, but this can be changed with the `--logging-dir` switch.

For more info, use `cupo.py [backup | retrieve | new-vault | sample-config ] -h`.

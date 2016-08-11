---
permalink: /quick_start/
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
* `p7zip-full` *(must be the 'full' version!)*

#### Installing and Configuring Prerequisites
* Use the following command to install most of the dependencies from apt:
`sudo apt-get install python-all python-pip python-botocore p7zip-full`
* Install MongoDB for your system as described [in the MongoDB docs](http://docs.mongodb.com/manual/administration/install-on-linux).
* Once that's completed, install the AWS command-line tool:
`pip install awscli`
* The AWS CLI needs to be configured before it can be used. Run:
`aws --configure`
and supply your AWS credentials.
* MongoDB needs a data directory to run - by default it's `/data/db`. Create this directory, and make sure that the current user is a member of the `mongodb` group, and that the group has full permissions to that directory:
`mkdir -p /data/db; usermod -a -G mongodb <username>; chgrp -R mongodb /data/db`

### Grabbing the Source
It's all Python, so no compilation required!
Open a terminal, `cd` into your favourite directory and run:

`git clone https://github.com/calmcl1/cupo-backup.git; cd cupo-backup`

That's it!

## Usage

### Creating a New Vault
To start off, create a new vault in Glacier (or, if a vault that you want to use already exists in AWS, register it in the local database):

`cupo.py --account-id AWS_ACCOUNT_ID --database DATABASE_NAME new-vault NEW_VAULT_NAME`

where:

* `AWS_ACCOUNT_ID` is, unsurprisingly, the account ID associated with your AWS account - a numerical value.
* `DATABASE_NAME` is the name of the MongoDB database that we're using to store the local Glacier archive tracking data in. It will be created if it does not already exist.
* `NEW_VAULT_NAME` is the name of the Glacier vault that we're storing the the backup archives in. It will be created in Glacier if it does not yet exist.

### Backing Up a Directory

Now, it's as simple as specifying a directory to back up and a vault!

`cupo.py --account-id AWS_ACCOUNT_ID --database DATABASE_NAME backup TOP_DIR VAULT_NAME`

where:
* `TOP_DIR` is the root directory to back up.
* `VAULT_NAME` is the Glacier vault to back up to. It will not be created if it doesn't exist - use `cupo.py new-vault` first.

There isn't much output on the terminal, but a log will created that you can `tail -f` if you wish. The default log location is `~/.CupoLog`, but this can be changed with the `--logging-dir` switch.

For more info, use `cupo.py [backup | new-vault] -h`.

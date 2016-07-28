# HCRBackup
So, you want to back up a radio station...

## Installing

### Prerequisites
* Python < 2.7+ *(but not Python 3 - yet)*
* Amazon AWS command-line interface (`pip install awscli`)
* MongoDB
* `python-botocore`
* `p7zip-full` *(must be the 'full' version!)*

#### Installing and Configuring Prerequisites
* Use the following command to install most of the dependencies from apt:
  `sudo apt-get install python-all python-pip p7zip-full`
* Install MongoDB for your system as described [in the MongoDB docs](http://docs.mongodb.com/manual/administration/install-on-linux).
* Once that's completed, install the AWS command-line tool:
  `pip install awscli`
* The AWS CLI needs to be configured before it can be used. Run:
  `aws --configure`
  and supply your AWS credentials.
* MongoDB needs a data directory to run - by default it's `/data/db`. Create this directory, and make sure that the current user is a member of the `mongodb` group, and that the group has full permissions to that directory:
  `mkdir -p /data/db; usermod -a -G mongodb <username>; chgrp -R mongodb /data/db`

## Usage

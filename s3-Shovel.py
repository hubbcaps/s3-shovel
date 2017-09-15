# Script to traverse the AFI Directory structure, syncing the files to S3 and then archiving all loose files

import os, sys, ast
import logging

import boto
from boto.s3.connection import S3Connection

# Initialize various variables we will use through out the script

# NOTE
# These two AWS variables are not needed when deployed to an EC2 Instance with the correct role applied as boto will by default use the metadata service to grab them via IAM.
AWS_ACCESS_KEY = ''  # Fill in Access Key or leave blank if using EC2 Roles
AWS_SECRET_ACCESS_KEY = ''  # Fill in Secret Access Key or leave blank if using EC2 Roles
BUCKET_NAME = ''  # Fill in Bucket Name
REGION_HOST = 's3.us-west-2.amazonaws.com'  # Replace with closest S3 Endpoint to your S3 Region

# These need to be set to the directory you want crawled and shoveled, the archive directory you want the files archived to on the system you'll be shoveling from, and the log file you want splunk to monitor for failures.
SOURCE_DIR = ''  # Set the source directory to search for files to shovel up.
DEST_DIR = ''  # Set the directory to move the files to after shoveling
LOG_FILE = ''  # Set the full path to where you want the log file to write to.
LOCK_DIR = 'shovel-lock'  # This will auto write under the dest dir
META_FILE = 'shovel.meta'  # This will auto write under the dest dir

#max size in bytes before uploading in parts. between 1 and 5 GB recommended
MAX_SIZE = 20 * 1000 * 1000
#size of parts when uploading in parts
PART_SIZE = 6 * 1000 * 1000

# Setup logger
logging.basicConfig(filename=LOG_FILE,level=logging.INFO)

if not os.path.exists(os.path.join(DEST_DIR, LOCK_DIR)):
	os.makedirs(os.path.join(DEST_DIR, LOCK_DIR))
else:
	logging.info('Job Already in Progress')
	sys.exit(0)

# Initialize the S3 Connector with our keys
try:
    connector = S3Connection(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, host=REGION_HOST)
except:
    os.rmdir(os.path.join(DEST_DIR, LOCK_DIR))
    logging.critical('S3 Connection unable to be established. Did you properly enter the AWS Keys?')
    sys.exit(1)
	
# Initialize the bucket we will be working with
try:
    bucket = connector.get_bucket(BUCKET_NAME, validate=False)
except:
    os.rmdir(os.path.join(DEST_DIR, LOCK_DIR))
    logging.critical('Bucket was not reachable, did you put the name in right? Does the bucket even exist?')
    sys.exit(1)
	
# Initialize array to hold the files we will be pushing up.
fileList = []

# Do some fun tuple stuff, crawling the base directory structure and feed the full file paths into the array by doing a join
for( dirpath, dirs, files) in os.walk( SOURCE_DIR ):
    fileList.extend( os.path.join(dirpath, f) for f in files)

# NOTE
# These two hardcoded paths need to be changed to work with the directories you will have this working against so it can properly strip the top levels out and properly create a relative directory in the S3 Bucket.
# Here we chew up the Absolute Paths into relative paths easily appened to s3 bucket names so as to create a nice directory structure
uploadFiles = [s.replace(SOURCE_DIR, '') for s in fileList]

# Here we change pythons working directory so that the relative path names make sense
os.chdir(SOURCE_DIR)

if not os.path.exists(os.path.join(DEST_DIR, META_FILE)):
    meta = open(os.path.join(DEST_DIR, META_FILE), 'w')
    for f in uploadFiles:
        meta.write("('%s',%i)\n" % (f, os.path.getsize(f)))
    meta.close()
    os.rmdir(os.path.join(DEST_DIR, LOCK_DIR))
    logging.info('First Pass, Meta File has been built, will be compared against on next run')
    sys.exit(0)

# Define function to allow properly closing each write over the connection. Something necessary for boto s3, not sure why, but it works.
def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()

with open(os.path.join(DEST_DIR, META_FILE)) as meta:
    metafiles = [ast.literal_eval(line) for line in meta]

newmeta = []
movefiles = []

# Here we begin the magic and start the upload depending on the size of the files.
for filename in uploadFiles:
    for line in metafiles:
        if line[0] == filename:
            if int(line[1]) == os.path.getsize(filename):
                sourcepath = filename
                destpath = filename
                print("Uploading %s to S3 Bucket %s" % (sourcepath, BUCKET_NAME))

# He            re we check the filesize and depending do a regular upload or a multipart upload
                try:
                    filesize = os.path.getsize(sourcepath)
                    if filesize > MAX_SIZE:
                        print("multipart upload")
                        # Here we are creating a multipart upload object to allow us to feed the parts into the upload.
                        multipart = bucket.initiate_multipart_upload(destpath, encrypt_key=True, policy='bucket-owner-full-control')
                        filepart = open(sourcepath, 'rb')
                        filepart_num = 0
                        while (filepart.tell() < filesize):
                            filepart_num += 1
                            print("uploading part %i" % filepart_num)
                            multipart.upload_part_from_file(filepart, filepart_num, cb=percent_cb, num_cb=10, size=PART_SIZE)

                        mp.complete_upload()
                        movefiles.append(filename)
                        logging.info("%s was uploaded to %s" % (sourcepath, BUCKET_NAME))

                    else:
                        print("Single Part Upload")
                        k = boto.s3.key.Key(bucket)
                        k.key = destpath
                        k.set_contents_from_filename(sourcepath, cb=percent_cb, num_cb=10, encrypt_key=True, policy='bucket-owner-full-control')
                        movefiles.append(filename)
                        logging.info("%s was uploaded to %s" % (sourcepath, BUCKET_NAME))
                except:
                    logging.warning('File was unable to be uploaded for some reason.')
            else:
                logging.info('File is still being uploaded')
                newmeta.append(filename)

for filesuped in movefiles:
    uploadFiles.remove(filesuped)

for filesleft in uploadFiles:
    newmeta.append(filesleft)

# Remove the old meta, in with the new meta.
os.remove(os.path.join(DEST_DIR, META_FILE))
meta = open(os.path.join(DEST_DIR, META_FILE), 'w')
for f in newmeta:
    meta.write("('%s',%i)\n" % (f, os.path.getsize(f)))
meta.close()

# After we're done with the upload, we'll go ahead and move all of the uploaded files into an archive directory.
# Note that the archive directory won't follow the same folder structure. It's just a top level directory we will dump the files for later retreival.

for fp in movefiles:
    filename = os.path.split(fp)
    os.rename(fp, os.path.join(DEST_DIR,filename[-1]))
    logging.info("%s was moved to the archive" % (filename[-1]))

os.rmdir(os.path.join(DEST_DIR, LOCK_DIR))

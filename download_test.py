import boto3
import os
profile_name = 'ds_sandbox'
data_bucket = 'programsdatabucket'
results_bucket = 'chainsresults'
session = boto3.session.Session(profile_name=profile_name)
S3_CLIENT = session.client('s3')
S3_RESOURCE = boto3.resource('s3')

def downloadExperimentS3(experiment, s3client, bucket_name):
    keys_list = s3client.list_objects(Bucket=bucket_name)['Contents']
    for s3_key in keys_list:
        s3_object = s3_key['Key']
        path, filename = os.path.split(s3_key.key)
        print('s3_object,path, filename:', s3_object, path, filename)
        if not s3_object.endswith("/"):
            s3client.download_file(bucket_name, s3_object, s3_object)
        else:
            object_dir = os.path.join(experiment, s3_object)
            print('object_dir:', object_dir)
            if not os.path.exists(object_dir):
                os.makedirs(object_dir)

# download file into current directory
my_bucket = S3_RESOURCE.Bucket(results_bucket)
for s3_object in my_bucket.objects.all():
    # Need to split s3_object.key into path and file name, else it will give error file not found.
    path, filename = os.path.split(s3_object.key)
    print(path, filename)
    #my_bucket.download_file(s3_object.key, filename)


import io
import zipfile

zip_buffer = io.BytesIO()
with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zipper:
    infile_object = S3_RESOURCE.get_object(Bucket=results_bucket, Key=object_key)
    infile_content = infile_object['Body'].read()
    zipper.writestr(file_name, infile_content)

S3_RESOURCE.put_object(Bucket=bucket, Key=PREFIX + zip_name, Body=zip_buffer.getvalue())


# print('downloading experiment files')
# downloadExperimentS3('EMS_DCMA_DD_prt', S3_CLIENT, results_bucket)
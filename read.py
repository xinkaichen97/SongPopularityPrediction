import h5py
import os
import boto3
from botocore.exceptions import NoCredentialsError
import tempfile
import pandas as pd
import csv
import glob
from itertools import chain


def upload_to_aws(local_file, bucket, s3_file):
    #s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    s3 = boto3.resource('s3')

    try:
        #s3.upload_file(local_file, bucket, s3_file)
        s3.Object(bucket, s3_file).put(Body=open(local_file, 'rb'))
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def process_h5_file(h5_file):
    metadata = h5_file['metadata']['songs'][:1]
    analysis = h5_file['analysis']['songs'][:1]
    brainz = h5_file['musicbrainz']['songs'][:1]
    return list(metadata[0]) + list(analysis[0]) + list(brainz[0])
#return h5_file['metadata']['songs'][:1]


def transform_local(path):
    try:
        with h5py.File(path) as h5:
            return process_h5_file(h5)
    except Exception:
        return []


def paths_to_file(processed, filename):
    with open(filename, 'w') as f:
        for path in processed:
            row = transform_local(path)
            #print(row)
            write = csv.writer(f)
            write.writerow(row)


def get_all_paths(root, output='paths.txt'):
    paths = []
    for root, dirs, files in os.walk(root):
        for f in files:
            path = (os.path.join(root, f))
            paths.append(path)
            with open(output, 'a') as out:
                out.write(path+'\n')
    assert len(paths) == 1000000
    return paths


if __name__ == "__main__":
    print('enter', flush=True)
    processed = []
    chunk_size = 50000
    machine = 0
    file_count = 0
    paths = ['msd/data/C', 'msd/data/D', 'msd/data/E', 'msd/data/F', 'msd/data/G', 'msd/data/H']
    for root, dirs, files in chain.from_iterable(os.walk(path) for path in paths):#os.walk('msd/data'):
        for f in files:
            processed.append(os.path.join(root, f))
            if len(processed) % chunk_size == 0:
                file_name = 'data_' + str(file_count) + '_m' + str(machine) + '.csv'
                paths_to_file(processed, file_name)
                print('Chunk {0} read finished'.format(file_count), flush=True)
                upload_to_aws(file_name, 'msd-bucket-10605', 'msd-data/' + file_name)
                print('Chunk {0} upload to s3 finished'.format(file_count), flush=True) 
                file_count += 1
                processed = []
    if len(processed) > 0:
         file_name = 'data_' + str(file_count) + '_m' + str(machine) + '.csv'
         paths_to_file(processed, file_name)
         print('Chunk {0} read finished'.format(file_count), flush=True)
         upload_to_aws(file_name, 'msd-bucket-10605', 'msd-data/' + file_name)
         print('Chunk {0} upload to s3 finished'.format(file_count), flush=True)
         file_count += 1
         processed = []



import os
import time
import uuid
from multiprocessing import Pool

import boto3
from botocore.exceptions import ClientError


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True


def download_file(bucket_name, object_name, file_name):
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, object_name, file_name)


def get_full_path(filename):
    cur_path = os.path.dirname(__file__)
    return '{}{}{}'.format(cur_path, '/files/', filename)


def generate_file(filename, size_mb=1):
    full_path = get_full_path(filename)
    with open(full_path, "wb") as out:
        out.truncate(size_mb * 1024 * 1024)


def list_buckets():
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        print(bucket.name)


def get_uuid():
    return str(uuid.uuid4())


def exp(num_files, file_size_MiB, bucket_name, parallel_clients=1):
    print('num files {}, file size {} MiB, bucket name {}, parallel clients {}'.format(num_files, file_size_MiB,
                                                                                       bucket_name,
                                                                                       parallel_clients))

    start = time.time()
    file_name = get_full_path('{}MiB.txt'.format(file_size_MiB))
    for i in range(num_files):
        upload_file(file_name=file_name, bucket=bucket_name, object_name=str(i))
    end = time.time()
    calc_time = end - start
    total_size_mebibyte = num_files * file_size_MiB
    size_in_megabits = (total_size_mebibyte * 1024 * 1024 * 8) / (1000 * 1000)
    print('Upload time: {} s'.format(calc_time))
    print('Throughput: {} MiB/s'.format(total_size_mebibyte / calc_time))
    print('Throughput: {} Mbps'.format(size_in_megabits / calc_time))
    print()

    start = time.time()
    for i in range(num_files):
        download_file(my_bucket_name, str(i), '/dev/null')
    end = time.time()
    calc_time = end - start
    total_size_mebibyte = num_files * file_size_MiB
    size_in_megabits = (total_size_mebibyte * 1024 * 1024 * 8) / (1000 * 1000)
    print('Download time: {} s'.format(calc_time))
    print('Throughput: {} MiB/s'.format(total_size_mebibyte / calc_time))
    print('Throughput: {} Mbps'.format(size_in_megabits / calc_time))

    clear_bucket(bucket_name)
    print("===============================")


def upload_single(prop):
    file_name = get_full_path('{}MiB.txt'.format(prop['file_size_MiB']))
    for i in range(prop['num_files']):
        upload_file(file_name=file_name, bucket=prop['bucket_name'], object_name=str(prop['index'] + i))


def download_single(prop):
    file_name = '/dev/null'
    for i in range(prop['num_files']):
        # file_name = get_full_path('/tmp/' + str(prop['index'] + i) + '.txt')
        download_file(bucket_name=prop['bucket_name'], object_name=str(prop['index'] + i), file_name=file_name)


def print_s(prop):
    time.sleep(prop['num'])
    print(str(prop['num']))
    return prop['num']


def exp2(num_files, file_size_MiB, bucket_name, parallel_clients=1, split_task=True):
    if split_task and num_files % parallel_clients != 0:
        raise Exception('num_files has to be divisible by parallel_clients')
    print('num files {}, file size {} MiB, bucket name {}, parallel clients {}'.format(num_files, file_size_MiB,
                                                                                       bucket_name,
                                                                                       parallel_clients))
    files_per_process = num_files
    if split_task:
        files_per_process = int(num_files / parallel_clients)
    print('files_per_process {}'.format(files_per_process))

    props = []
    for i in range(parallel_clients):
        props.append(
            {
                'num_files': files_per_process,
                'file_size_MiB': file_size_MiB,
                'bucket_name': bucket_name,
                'index': i * files_per_process
            }
        )
    p = Pool(parallel_clients)

    print("Test upload...")
    res = []
    for prop in props:
        res.append(p.apply_async(upload_single, (prop,)))

    start = time.time()
    [r.get() for r in res]
    end = time.time()
    calc_time = end - start
    total_size_mebibyte = num_files * file_size_MiB
    size_in_megabits = (total_size_mebibyte * 1024 * 1024 * 8) / (1000 * 1000)
    print('Upload time: {} s'.format(calc_time))
    print('Throughput: {} MiB/s'.format(total_size_mebibyte / calc_time))
    print('Throughput: {} Mbps'.format(size_in_megabits / calc_time))
    print()

    print("Test download...")

    res = []
    for prop in props:
        res.append(p.apply_async(download_single, (prop,)))

    start = time.time()
    [r.get() for r in res]
    end = time.time()
    calc_time = end - start
    total_size_mebibyte = num_files * file_size_MiB
    size_in_megabits = (total_size_mebibyte * 1024 * 1024 * 8) / (1000 * 1000)
    print('Download time: {} s'.format(calc_time))
    print('Throughput: {} MiB/s'.format(total_size_mebibyte / calc_time))
    print('Throughput: {} Mbps'.format(size_in_megabits / calc_time))

    clear_bucket(bucket_name)
    print("===============================")


def clear_bucket(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()


if __name__ == '__main__':
    my_bucket_name = 'damian.misows.agh'

    # list_buckets()
    for size in [1, 5, 10, 100, 200]:
        generate_file('{}MiB.txt'.format(str(size)), size_mb=size)
    # upload_file(file_name=get_full_path('1mb.txt'), bucket=bucket_name, object_name=get_uuid())

    exp(num_files=1, file_size_MiB=1, bucket_name=my_bucket_name)
    exp(num_files=1, file_size_MiB=5, bucket_name=my_bucket_name)
    exp(num_files=1, file_size_MiB=10, bucket_name=my_bucket_name)

    exp(num_files=100, file_size_MiB=1, bucket_name=my_bucket_name)
    exp(num_files=1, file_size_MiB=100, bucket_name=my_bucket_name)
    exp(num_files=1, file_size_MiB=200, bucket_name=my_bucket_name)

    exp2(num_files=100, file_size_MiB=1, bucket_name=my_bucket_name, parallel_clients=2, split_task=True)
    exp2(num_files=100, file_size_MiB=1, bucket_name=my_bucket_name, parallel_clients=4, split_task=True)
    exp2(num_files=100, file_size_MiB=1, bucket_name=my_bucket_name, parallel_clients=5, split_task=True)
    exp2(num_files=100, file_size_MiB=1, bucket_name=my_bucket_name, parallel_clients=10, split_task=True)

    exp2(num_files=1, file_size_MiB=100, bucket_name=my_bucket_name, parallel_clients=10, split_task=False)

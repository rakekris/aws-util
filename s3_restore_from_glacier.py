import boto3
import argparse

PARSER = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''\
s3_restore_from_glacier.py

This utility has 3 required options to run:
------------------------------------
1) bucket - bucket name
1) prefix - The prefix of the s3 key
2) suffix - The suffix of the s3 key

Example:
./s3_restore_from_glacier.py 

    '''
    )
PARSER.add_argument('-b', '--bucket', required=True, default="default", help='s3 bucket name')
PARSER.add_argument('-p', '--prefix', required=True, default="us-east-1", help='s3 prefix to be restored from glacier')
PARSER.add_argument('-s', '--suffix', required=True, default="ep-", help='s3 suffix to be restored from glacier')


ARGUMENTS = PARSER.parse_args()
SUFFIX = ARGUMENTS.suffix
PREFIX = ARGUMENTS.prefix
BUCKET = ARGUMENTS.bucket


def get_matching_s3_keys_by_prefix_suffix(prefix, suffix):
    s3_client = boto3.client('s3')
    list_objects_request = {'Bucket': BUCKET}
    if isinstance(prefix, str):
        list_objects_request['Prefix'] = prefix
    key_list = []
    next_page_exists = True
    while next_page_exists:
        list_objects_resp = s3_client.list_objects_v2(**list_objects_request)
        if 'Contents' in list_objects_resp:
            for obj in list_objects_resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    key_list.append(key)
        try:
            list_objects_request['ContinuationToken'] = list_objects_resp['NextContinuationToken']
        except KeyError:
            next_page_exists = False
    return key_list


def restore_obj_from_glacier(bucket, prefix, suffix):
    s3 = boto3.resource('s3')
    buck = s3.Bucket(bucket)
    obj_list = get_matching_s3_keys_by_prefix_suffix(prefix=prefix, suffix=suffix)
    for obj in obj_list:
        try:
            resp = buck.meta.client.restore_object(
                Bucket=bucket,
                Key=obj,
                RestoreRequest={'Days': 100, 'GlacierJobParameters': {'Tier': 'Standard'}})
            print("status code {}".format(resp['ResponseMetadata']['HTTPStatusCode']))
        except Exception:
            print("restore failed for key : {}".format(obj))
            pass


def main():
    """ Main program run """
    restore_obj_from_glacier(bucket=BUCKET, prefix=PREFIX, suffix=SUFFIX)


if __name__ == "__main__":
    main()

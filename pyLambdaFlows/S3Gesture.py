import botocore


"""
Disclaimer, the S3 isn't use yet but it is able to replace dynamodb.
Be aware that S3 is far away more expensive due to the write restriction.
"""

def create_bucket(name, max_idx, sess):
    """
    Create a specific bucket or reset if it already exists.
    """
    location = {'LocationConstraint': sess.region}
    S3Client = sess.getS3()
    bucketClient = sess.getBucket()

    try:
        S3Client.create_bucket(Bucket=name, CreateBucketConfiguration=location)
    except botocore.errorfactory.ClientError as e:
        if(e.response["Error"]["Code"]=="BucketAlreadyOwnedByYou"):

            buc = bucketClient.Bucket(name)
            buc.objects.all().delete()
            S3Client.delete_bucket(Bucket=name)

            S3Client.create_bucket(Bucket=name, CreateBucketConfiguration=location)
        else:
            raise e
    

def clearBucket(name, sess):
    """
    Reset a Specific bucket
    """
    S3Client = sess.getS3()
    bucketClient = sess.getBucket()
    try:
        buc = bucketClient.Bucket(name)
        buc.objects.all().delete()   
    except botocore.errorfactory.ClientError as e:
        pass

def removeBucket(name, sess):
    """
    Remove a specific bucket
    """
    S3Client = sess.getS3()
    bucketClient = sess.getBucket()
    try:
        buc = bucketClient.Bucket(name)
        buc.objects.all().delete()
        S3Client.delete_bucket(Bucket=name)
    except botocore.errorfactory.ClientError as e:
        pass
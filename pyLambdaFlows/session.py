from boto3 import client, resource
from botocore.exceptions import ClientError
from pandas import read_csv
from .error import BadAWSCredential
""" Default session pointer."""
_DEFAULT_SESSION = None

def set_default_session(session):
    """
    Set the default pointer to the given session.
    """
    global _DEFAULT_SESSION
    _DEFAULT_SESSION = session

def get_default_session(check_if_none=False):
    """
    Return the default Session.
    """
    global _DEFAULT_SESSION
    if _DEFAULT_SESSION is None and check_if_none:
        raise RuntimeError("No default session provided, please use kward or set default session !")
    return _DEFAULT_SESSION


class Session():
    """
    This class is used to load and share credentials to all pyLambdaFlow ressources.
    You must instantiate this class first before send or eval a computational graph. Please go take a look
    at the given example.
    You can deal with more than one Session but the default way to give Session isn't relevant in this case.
    """
    def __init__(self, region_name="eu-west-3", auto_purge=False, aws_access_key_id=None, aws_secret_access_key=None, credentials_csv=None):
        """The default constructor

        This constructor ask you for your credentials. You can provide then either
        with the AWS csv (credentials_csv) or 
        with the kwargs (aws_access_key_id, aws_secret_access_key). 
        This class is like a boto3 factory which store all instanciated class.

        You can change the credential but don't forget to clear boto3 objects with clear function.

        :param str region_name: AWS server to use (default Paris).
        """
        self.region = region_name
        if credentials_csv is None:
            self.aws_access_key_id = aws_access_key_id
            self.aws_secret_access_key = aws_secret_access_key
        else:
            csv_loaded = read_csv(credentials_csv)
            self.aws_access_key_id = csv_loaded.iloc[0]["Access key ID"]
            self.aws_secret_access_key = csv_loaded.iloc[0]["Secret access key"]
        self.clients = dict(IAM=None, Lambda=None, DynamoDb=None, S3=None, Bucket=None, DynamoDbRessource=None)
        self.to_purge = list()
        self.auto_purge = auto_purge

    def __enter__(self):
        set_default_session(self)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        set_default_session(None)
        if self.auto_purge:
            self.purge_all()

    def setCredential(self, aws_access_key_id=None, aws_secret_access_key=None):
        """Update current credential.

        You can provide credential after the constructor with this method.
        The credential validity will be cheacked later on.

        :param str aws_access_key_id: aws access key id
        :param str aws_secret_access_key: aws acces key
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_access_key_id

    def getIAM(self):
        """Provide the IAM client for binded AWS account.

        This method return the boto3 IAM client object using provided crendentials.

        :raises: :class:`BadAWSCredential` : Bad Credential.
        """
        if( self.aws_access_key_id is None or self.aws_secret_access_key is None):
            raise RuntimeError("Credentials must be provided !")
        if self.clients.get("IAM", None) is None:
            newIamClient = client('iam',
                                    aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key=self.aws_secret_access_key,
                                    region_name=self.region)
            try:
                newIamClient.get_account_summary()
            except ClientError:
                raise BadAWSCredential()

            self.clients["IAM"] = newIamClient
        return self.clients["IAM"]

    def getLambda(self):
        """Provide the Lambda client for binded AWS account.

        This method return the boto3 lambda client object using provided crendentials.

        :raises: :class:`BadAWSCredential` : Bad Credential.
        """
        if( self.aws_access_key_id is None or self.aws_secret_access_key is None):
            raise RuntimeError("Credentials must be provided !")
        if self.clients.get("Lambda", None) is None:
            newLambdaClient = client('lambda',
                                    aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key=self.aws_secret_access_key,
                                    region_name=self.region)

            try:
                newLambdaClient.list_functions()
            except ClientError:
                raise RuntimeError("Your credential isn't valid")

            self.clients["Lambda"] = newLambdaClient
        return self.clients["Lambda"]

    def getS3(self):
        """Provide the S3 client for binded AWS account.

        This method return the boto3 S3 client object using provided crendentials.

        :raises: :class:`BadAWSCredential` : Bad Credential.
        """
        if( self.aws_access_key_id is None or self.aws_secret_access_key is None):
            raise RuntimeError("Credentials must be provided !")
        if self.clients.get("S3", None) is None:
            newS3Client = client('s3',
                                    aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key=self.aws_secret_access_key,
                                    region_name=self.region)
            self.clients["S3"] = newS3Client
        return self.clients["S3"]

    def getBucket(self):
        """Provide the S3 ressource for binded AWS account.

        This method return the boto3 S3 ressource object using provided crendentials.

        :raises: :class:`BadAWSCredential` : Bad Credential.
        """
        if( self.aws_access_key_id is None or self.aws_secret_access_key is None):
            raise RuntimeError("Credentials must be provided !")
        if self.clients.get("Bucket", None) is None:
            newS3Client = resource('s3',
                                    aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key=self.aws_secret_access_key,
                                    region_name=self.region)
            self.clients["Bucket"] = newS3Client
        return self.clients["Bucket"]

    def getDynamoDb(self):
        """Provide the dynamodb client for binded AWS account.

        This method return the boto3 dynamodb client object using provided crendentials.

        :raises: :class:`BadAWSCredential` : Bad Credential.
        """
        if( self.aws_access_key_id is None or self.aws_secret_access_key is None):
            raise RuntimeError("Credentials must be provided !")
        if self.clients.get("DynamoDb", None) is None:
            newDynamoClient = client('dynamodb',
                                    aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key=self.aws_secret_access_key,
                                    region_name=self.region)
            try:
                newDynamoClient.list_tables()
            except ClientError:
                raise RuntimeError("Your credential isn't valid")

            self.clients["DynamoDb"] = newDynamoClient
        return self.clients["DynamoDb"]

    def getDynamoDbRessource(self):
        """Provide the dynamodb ressource for binded AWS account.

        This method return the boto3 dynamodb ressource object using provided crendentials.

        :raises: :class:`BadAWSCredential` : Bad Credential.
        """
        if( self.aws_access_key_id is None or self.aws_secret_access_key is None):
            raise RuntimeError("Credentials must be provided !")
        if self.clients.get("DynamoDbRessource", None) is None:
            newDynamoClient = resource('dynamodb',
                                    aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key=self.aws_secret_access_key,
                                    region_name=self.region)
            try:
                pass
                #newDynamoClient.list_tables()
            except ClientError:
                raise RuntimeError("Your credential isn't valid")

            self.clients["DynamoDbRessource"] = newDynamoClient
        return self.clients["DynamoDbRessource"]

        

    def clear(self):
        """Clear all boto3 objects.

        This method remove all boto3 objects in the Session memory. That means all new
        object will be created according to the current setted credential.

        Usefull if you want to change the Session's crendentials.
        """
        for key in self.clients.keys():
            self.clients[key] = None

    def add_func_to_purge(self, element):
        """Add function to the intern pruge list.
        """
        self.to_purge.append(element)

    def purge_all(self):
        """Remove all lambda from the purge list."""
        client = self.getLambda()
        for element in set(self.to_purge):
            client.delete_function(FunctionName=element)

if __name__ == "__main__":
    print(get_default_session())

    with Session() as sess:
        print(get_default_session())




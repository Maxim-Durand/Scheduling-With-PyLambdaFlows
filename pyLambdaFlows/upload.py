import os, sys
import tempfile
import zipfile
import botocore
import hashlib
import warnings

from .version import __version__, __aws_python_version__

def splitall(path):
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts


def getHash(fil, nb_digits=5):
    fil.seek(0)
    digest = hashlib.md5()
    digest.update(bytes(__version__, 'utf-8') + fil.read())
    res = digest.hexdigest()
    fil.seek(0)
    return res[0:nb_digits]

class Uploader:
    def __init__(self, sess):
        self.sess = sess
        iam_client = self.sess.getIAM()
        self.role = iam_client.get_role(RoleName='LambdaBasicExecution')
        self.already_pushed = {}

    def upload_lambda(self, op, purge=False):

        if op in self.already_pushed.keys():
            return self.already_pushed[op]
        
        funct_path = op.funct
        try:
            if os.path.basename(funct_path).split('.')[-1] != "py":
                raise RuntimeError("Your funct path must be a .py file")
        except Exception:
            raise RuntimeError("Your path isn't valid !")
        
        lambda_name = os.path.basename(funct_path).split('.')[0]

        
        f = tempfile.TemporaryFile()

        with zipfile.ZipFile (f, "a", compression=zipfile.ZIP_DEFLATED) as zipObj:
            for element in op.files:
                if not os.path.exists(element):
                    raise RuntimeError("File or directory doesn't exist")
                if os.path.isfile(element):
                    zipObj.write(element, os.path.basename(element))
                if os.path.isdir(element):

                    for root, _, files in os.walk(element):
                        # add directory (needed for empty dirs)
                        if "__pycache__" in splitall(root):
                            continue
                        zipObj.write(root, os.path.relpath(root, os.path.join(element,"..")))
                        for file in files:
                            filename = os.path.join(root, file)
                            if os.path.isfile(filename): # regular files only
                                arcname = os.path.join(os.path.relpath(root, os.path.join(element,"..")), file)
                                zipObj.write(filename, arcname)


            dir_path = os.path.dirname(os.path.realpath(__file__))
            zipObj.write(os.path.join(dir_path, "external","decorator.py"), os.path.join("pyLambdaFlows","decorator.py"))

        f.seek(0)

        lambda_name = lambda_name + '-' + getHash(f)

        lambda_client = self.sess.getLambda()
        #Look if lambda exist
        alreadyExist = False
        try:
            lambda_client.get_function(FunctionName=lambda_name)
            alreadyExist = True
        except lambda_client.exceptions.ResourceNotFoundException:
            pass
        
        if(alreadyExist):
            if(purge):
                warnings.warn("Lambda Already on AWS dataBase ({}), removing...".format(lambda_name), RuntimeWarning)
                lambda_client.delete_function(FunctionName=lambda_name)
            else:
                warnings.warn("Lambda Already on AWS dataBase ({})".format(lambda_name), RuntimeWarning)
                self.already_pushed[op] = lambda_name
                return lambda_name

        #TODO catch error
        lambda_client.create_function(
            FunctionName=lambda_name,
            Runtime=__aws_python_version__,
            Role=self.role['Role']['Arn'],
            Handler=lambda_name.split("-")[0]+".lambda_handler",
            Code=dict(ZipFile=f.read()),
            Timeout=300, # Maximum allowable timeout
            MemorySize=3008,
            )
        self.already_pushed[op] = lambda_name
        return lambda_name
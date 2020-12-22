import pyLambdaFlows

sess = pyLambdaFlows.Session(credentials_csv="./accessKeys.csv")

#a = pyLambdaFlows.upload.Uploader(sess)
#a.upload_lambda("./source/mean.py")
a = sess.getLambda()

elements = a.list_functions()["Functions"]
for element in elements:
    a.delete_function(FunctionName=element["FunctionName"])


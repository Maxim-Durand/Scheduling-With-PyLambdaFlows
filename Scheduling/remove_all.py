import pyLambdaFlows

# Permet de supprimer toutes les fonctions Lambda sur AWS

sess = pyLambdaFlows.Session(credentials_csv="./accessKeys.csv")

a = sess.getLambda()

elements = a.list_functions()["Functions"]
for element in elements:
    a.delete_function(FunctionName=element["FunctionName"])


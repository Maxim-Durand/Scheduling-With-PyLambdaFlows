
 ## **Planning algorithm NP-Hard using serverless with PyLambdaFlows

Concerning *pyLambdaFlows* installation, please refer to the ReadMe made by the students who developped this tool on their [Git](https://github.com/Enderdead/pyLambdaFlows.

This project requires Python 3.

- After cloning this repository, please install all the dependencies specified in *requirements.txt*. To do this open a terminal in *Scheduling_with_PyLambdaFlow* and type :
``` bash
pip3 install -r requirements.txt
```
- Afterward, run the installer :
``` bash
python3 setup.py install --user
```
- On the AWS website, create a role named *LambdaBasicExecution* ( more information [here](https://docs.aws.amazon.com/lambda/latest/dg/lambda-intro-execution-role.html)).
- Create an IAM account and export the *accessKeys.csv* file with all the connexion information ( more information [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html)).

- Copy *accessKeys.csv* into *Scheduling_with_PyLambdaFlow\Scheduling* directory.
- To run the parallelised planning application on AWS servers:
``` bash
python3 main_parallel_aws.py
```
- To compare the precedent results you can run the sequential version on AWS:
``` bash
python3 main_sequentiel_aws.py
```
- Or you can run the sequential on your own computer :
``` bash
python3 main_sequentiel_local.py
```

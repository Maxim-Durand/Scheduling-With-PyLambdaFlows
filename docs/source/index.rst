.. pyLambdaFlows documentation master file, created by
   sphinx-quickstart on Sat Dec  7 17:42:15 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pyLambdaFlows's documentation!
=========================================

PyLambdaFlows lets you run your program on AWS Lambda for a large scale execution. It makes you able to use AWS service without major alteration on your code. Moreover, you can define very complex dependencies between your step and create your own dependancie function ! 




.. toctree::
    :maxdepth: 1
    :caption: Guide

    Get started <GetStarted.rst>
    Create your operator <CreateYourOwnOperator.rst>



.. toctree::
   :maxdepth: 1
   :caption: API

    Operator <Operator.rst>
    Dynamodb <DynamoDb.rst>
    Session <Session.rst>
    Dispenser <Dispenser.rst>

Why this library isn't finished yet !
-------------------------------------
pyLambdaFlows is a proof of concept, that mean I can't handle a large scale project. 
It's meanly due to intern algorithm implementation and static behavior with a large request.
But many solutions exist to fix those issues (you send your question at francois@gauthier-clerc.fr is you want
to continue this development)
Here you can find a list of possible enhancement :  

- Better graph exploration to reduce local computing time. It can be done with a pre-computed layer estimation that provides the right lambda number before perform graph algorithm.
- Provide data using more json way than DynamoDb.
- Divide graph into some smaller pieces to reduce the initial json size.
- Create a virtual python environment to use numpy in lambda kernel.
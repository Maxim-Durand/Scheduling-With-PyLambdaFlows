####################
Get started
####################

Installation
-----------------
You can easily install this library using the setup.py script.

Firstly, you have to clone the `github repo <https://github.com/Enderdead/pyLambdaFlows>`_ .
Then go to the pyLambdaFlows directory and install all depandancies with this command : 

.. code-block:: text

    pip3 install -r requirements.txt

Finally you can install pyLambdaFlows.

.. code-block:: text

    python3 setup.py install --user


IAM setup
--------------------

In order to be able to send lambda request to AWS API, you have to create an IAM role.
This role must be named as ``LambdaBasicExecution``.
You can follow this official guide to create your own.

https://docs.aws.amazon.com/lambda/latest/dg/lambda-intro-execution-role.html

.. warning::

    The ``LambdaBasicExecution`` role must get all lambda permissions.


In order to generate  valid credentials, you can follow the tutorial bellow that explain you how to create a IAM account :

https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html

Then you can download your csv credential file, but be aware that this file is provided only once.

Basic example
--------------------

pyLambdaFlows works with three components; operator classes, kernel source files and main source file (with computational graph declaration). 
An *Operator* is a class that contains 2 things, a depandancie function (called  *Dispenser*) and a associeted kernel file. This operator is
a graph element and represent a lambda process. You can use predefined *Map*, *Reduce*, and *Source* operators.

Firstly, we will create 2 lambda process with 2 kernel file.
A kernel file is a basic python file with some requirements :

#. A *lambda_handler* function with a specific argument number.

#. A *kernel* decorator applied on the *lambda_handler* function.



.. code-block:: python3 

    from pyLambdaFlows.decorator import kernel

    @kernel 
    def lambda_handler(x):
        return x*x

This kernel take one element and return the squared value. You have to create one python file per kernel.
This file will be sent to AWS cloud.



.. note::

    You can find this example in the github source code in the example directory.

We can define an other one that compute a reduce mean. To do that, we can create a kernel file like this :

.. code-block:: python3 

    from pyLambdaFlows.decorator import kernel
    from statistics import mean

    @kernel
    def lambda_handler(inputData):
        result = mean(inputData)
        return result

It's a pretty simple code using the statistics library.
Now we are ready to define the computational graph and evaluate it !

Firstly, we create a source node. A source node is like an input to the computational graph.

.. code-block:: python3 

    import pyLambdaFlows

    source_node = pyLambdaFlows.op.Source(name="Source")


.. note::

    You have to specify all source nodes values at each graph evaluation.

Then we can add the map and reduce operator with our kernel file.


.. code-block:: python3 

    import pyLambdaFlows

    squared_node = pyLambdaFlows.op.Map(source_node, "./square.py", name="Square")
    result_node = pyLambdaFlows.op.Map(squared_node, "./mean.py", name="Mean")

Now we are ready to evaluate this with some values. In order to call AWS API,
we have to create a session object that cointains all AWS credentials.
The easy way is to download your access.csv and put is beside the main python file.

After the session creation, we can call firstly ``compile`` method from the *result_node* object and secondly
``eval`` method. Be sure to provide all source values into the *feed_dict* kwargs (go check the following source code).

.. code-block:: python3 

    session = pyLambdaFlows.session.Session(credentials_csv="./access.csv")
    result_node.compile(sess=session)
    result = result_node.eval(feed_dict={source_node : [1,2,3], sess=session)

    print(result)# 4.666666666666667

An other way to use session is use a *with* block like this.

.. code-block:: python3 

    with pyLambdaFlows.session.Session(credentials_csv="./access.csv") as sess:
        result_node.compile()
        result = result_node.eval(feed_dict={source_node : [1,2,3])
        print(result)# 4.666666666666667



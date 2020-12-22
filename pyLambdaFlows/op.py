from .dispenser import *
from .session import get_default_session, set_default_session, Session
from .tree import *
from .utils import isIterable, PromessResult
from .upload import Uploader
import os 
import progressbar
from .DynamoGesture import *
import json
from threading import Thread
import pickle
import traceback

"""
If you want to create your own pyLambdaOperator, please go check the documentation guide. 
"""


class pyLambdaElement:
    """An abstract class that describe a pyLambdaFlow element.

    This abstract class shall implement _send method.
    """
    def _send(self, uploader, purge=False):
        """ Visitor method to upload this operator and it childrens with uploader object.

        :param Uploader uploader: Uploader instance to use
        """
        raise NotImplementedError()


class Source(pyLambdaElement):
    """ This pyLambdaElement is a data source point.
    
    It makes you able to provide data on your computing graph. 
    """
    def __init__(self):
        """ Default constructor.
        """
        self.parent = None
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.func_path = os.path.join(os.path.join(dir_path, "external"),"source.py")
        self.funct = os.path.join(os.path.join(dir_path, "external"),"source.py")
        self.files = [self.funct,]

    def _send(self, uploader, purge=False):
        self.aws_lambda_name = uploader.upload_lambda(self, purge=purge)
        uploader.sess.add_func_to_purge(self.aws_lambda_name)




class Operation(pyLambdaElement):
    """The basic pyLambdaElement op.
    
    Operation is a generic operator using a topologie function and a parent operator.
    This class needs a topologie method to create dependancies with the parent node.
    """
    def __init__(self, parent, funct, topologie, name=None):
        if isIterable(parent):
            if len(list(filter(lambda x : isinstance(x, pyLambdaElement), parent)))!=len(parent):
                raise AttributeError("You must provide  a pyLambdaElement inherited class as a parent.")
        else:
            if (not isinstance(parent, pyLambdaElement)):
                raise AttributeError("You must provide  a pyLambdaElement inherited class as a parent.")


        if isIterable(topologie):
            if len(list(filter(lambda x : isinstance(x, Dispenser), topologie)))!=len(topologie):
                raise AttributeError("You must provide  a Dispenser inherited class as a topologie arg.")
        else:
            if (not isinstance(topologie, Dispenser)):
                raise AttributeError("You must provide  a Dispenser inherited class as a topologie arg.")
        
        if isIterable(parent) and not isIterable(topologie):
            topologie = (topologie,)*len(parent)


        self.parent = parent if isIterable(parent) else [parent]
        if isinstance(funct, list) or isinstance(funct, tuple):
            self.funct = funct[0]
            self.files = funct
        else:
            self.funct = funct
            self.files = [funct,]
        self.aws_lambda_name = None
        self.dispenser = topologie if isIterable(topologie) else [topologie]
        self.name = name

    def compile(self, sess=None, purge=False):
        """This function send this lambda op to AWS service.
        
        This function will send it own function to AWS but also all the dependancies one.
        You have to call this method before the eval one !"""
        if sess is None:
            sess = get_default_session(check_if_none=True)
        else:
            if not isinstance(sess, Session):
                raise RuntimeError("You must provide a Session object as sess kwarg.")
        
        # Upload functions
        upload = Uploader(sess)
        self._send(upload, purge=purge)



            
    def _send(self, uploader, purge=False):
        """
            send source code. (intern method)
        """
        for par in self.parent:
            par._send(uploader, purge=purge)
        
        self.aws_lambda_name = uploader.upload_lambda(self, purge=purge)

        uploader.sess.add_func_to_purge(self.aws_lambda_name)


    def eval(self, feed_dict=None, sess=None, wait=True):
        """Call this method to compute the result throw the computing graph.
        
        In order to use this function, you have to provide all Source operator default values.
        To do that, you just have use the kwarg feed_dict as follow : 
        
        Example : 

        .. code-block:: Python
        
            a = Source()
            b = Map(a)
            b.compile()
            b.eval(feed_dict={a:[1,2,3]})
        """

        if sess is None:
            sess = get_default_session(check_if_none=True)
        else:
            if not isinstance(sess, Session):
                raise RuntimeError("You must provide a Session object as sess kwarg.")
        
        tree = Tree(self)
        tree.compute(feed_dict)

        # Create dynamobd
        if not table_exists("pyLambda",sess):
            create_table("pyLambda",sess)
        fill_table("pyLambda", tree.gen_counter_values(), sess)
        put_entry("pyLambda", -1, [], 0, sess)

        # Create input json
        input_json = tree.generateJson(tableName="pyLambda")
        # Launch 
        print("Call AWS API")
        lambda_client = sess.getLambda()
        for _, request in progressbar.progressbar(input_json.items()):

            def lol():
                lambda_client.invoke(
                    FunctionName=request["func"],
                    InvocationType='Event',
                    Payload=json.dumps(request),
                )
            Thread(target=lol).start()

        if not wait:
            return PromessResult("pyLambda", tree.getResultIdx(), sess)
        print("Computation got started ! ")
        res = None
        for i in progressbar.progressbar(range(0,tree.max_idx)):

            receive= False
            error = False
            err_list = list()
            while not receive and not error:
                res = get_data("pyLambda", i, sess=sess)
                receive = not res is None
                _, remaining, err_list =  get_entry("pyLambda", -1, sess)
                if remaining == 1:
                    error = True

            if error and len(err_list)>0:
                idx, etype, value, tb = err_list[0]
                output = ('{2}\n' +
                      '\nThe above exception was first raised by a AWS lambda instance (number '+ str(idx) +' linked  to op : '+str(tree.getNode(idx))+' ): \n' +
                      'Distant traceback :\n' +
                      '{0}' +
                      '{1}: {2}''').format(''.join(traceback.format_list(tb)), etype.__name__, str(value))
                try:
                    raise etype(output)
                except:
                    raise Exception(output)
                
        table_data =  get_entries_group("pyLambda", min(tree.getResultIdx()), max(tree.getResultIdx()), sess=sess)
        return [element[1] for element in table_data]

    def __str__(self):
        return "<{} op, funct: {}, name: {}>".format(self.__class__.__name__, self.funct, self.name)


class Map(Operation):
    """The common map operator.

    This map operator will apply the given function to all parent elements (one by one),
    and return the result with the same input size.
    """
    def __init__(self, parent, funct, name=None):
        super().__init__(parent, funct, DMap(), name=name)


class Reduce(Operation):
    """The common reduce operator.

    This reduce operator will gather all incoming data using the given function.
    That's means you hate to provide a function able to deal with a dynamic input size.

    You can look at the example mean.
    """
    def __init__(self, parent, funct, name=None):
        super().__init__(parent, funct, DHardReduce(), name=name)


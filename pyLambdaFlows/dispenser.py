class Dispenser():
    """This class will explain data dependancies over two layers.
    
    For instance, if you have 3 lambda instance incoming, pylambdaflow will 
    use a dispenser class in order create the dependency graph.

    distribute(3) -> [[0,1], [2]]

    In this example the dependency graph will be like :

    .. code-block:: none
        
        X1----N1
              |
        X2----+

        X3----N2

    with Xn the previous op and N the new one.
    """
    def distribute(self, size):
        """Function computing the next layer dependancies.

        :param int size: Previous instance size.
        """
        raise NotImplementedError("Abstract class")

    def __call__(self, size):
        return self.distribute(size)


class DHardReduce(Dispenser):
    "Reduce dispenser that produce only one instance with all inputs."
    def distribute(self, size):
        return [list(range(size)),]

class DMap(Dispenser):
    "Map dispenser that create one lambda per previous instance."
    def distribute(self, size):
        return [ [i,] for i in range(size) ]

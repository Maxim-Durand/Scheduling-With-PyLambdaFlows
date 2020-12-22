from pyLambdaFlows.dispenser import Dispenser
from pyLambdaFlows.op import Operation

# Creation de l'operateur entre les fonctions Lambda 
class MapToMap(Dispenser):
    def distribute(self, size): 
        return [list(range(size)),]*size  # [[0, 1, 2, 3], [0, 1, 2, 3], [0, 1, 2, 3], [0, 1, 2, 3]] size=4

class MapToMapOp(Operation):
    def __init__(self, parent, funct, name=None ):
        Operation.__init__(self, parent, funct, MapToMap(), name=name)

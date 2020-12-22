from pyLambdaFlows.decorator import kernel

# Ce kernel prend en entree la liste des parametre envoye par le main
# Il cree une population initiale
# Il evalue ensuite chacun des individus (schedule)
# Enfin, il renvoie la population

from data import Data
from population import Population   

@kernel
def lambda_handler(param):
    pop_size=param[0]
    split_var=param[4]
    
    data=Data()
    _population = Population(size=int(pop_size/split_var),data=data)
    
    for schedule in _population.schedules:
        schedule._fitness = schedule.calculate_fitness()
    
    return _population.schedules

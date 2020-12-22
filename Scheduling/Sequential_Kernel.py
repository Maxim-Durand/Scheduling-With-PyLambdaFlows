from pyLambdaFlows.decorator import kernel

# Ce kernel prend en entree la liste des parametre envoye par le main
# Il cree une population initiale
# Il evalue ensuite chacun des individus (schedule) et fait evoluer cette population en local sur les serveurs d'AWS
# Il repete cette etape jusqu'a avoir un individus avec une fitness de 1

from data import Data
from genetic_algorithm import GeneticAlgorithm
from population import Population

@kernel
def lambda_handler(param):

    # Creation de la population initiale
    data = Data()
    _genetic_algorithm = GeneticAlgorithm(data=data, param=param)
    _population = Population(size=param[0], data=data)

    while _population.schedules[0]._fitness != 1.0:
        
        # Calcul la fitness de chaque emploi du temps dans la population
        for schedule in _population.schedules:
            schedule._fitness = schedule.calculate_fitness()
        
        # Tri la population et evolution de cette population
        _population.sort_by_fitness()
        _population = _genetic_algorithm.evolve(population=_population)
        
        for schedule in _population.schedules:
            schedule._fitness = schedule.calculate_fitness()
        
        _population.sort_by_fitness()
        
    return _population.schedules[0]

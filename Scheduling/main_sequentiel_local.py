import json
import time
POPULATION_SIZE           = 40
MUTATION_RATE             = 0.1
CROSSOVER_RATE            = 0.9
TOURNAMENT_SELECTION_SIZE = 3
NUMB_OF_ELITE_SCHEDULES   = 4
SPLIT_VAR                 = 4
NUMB_OF_GENERATION        = 35

def run():

  from data import Data
  from genetic_algorithm import GeneticAlgorithm
  from population import Population
  from utils import get_random_number

  generation_number = 0
  data = Data()
  param = []
  param.append([POPULATION_SIZE,MUTATION_RATE,CROSSOVER_RATE,TOURNAMENT_SELECTION_SIZE,0,NUMB_OF_ELITE_SCHEDULES])
  
  start = time.time()

  # Creation de la population initiale
  _genetic_algorithm = GeneticAlgorithm(data=data, param=param[0])
  _population = Population(size=POPULATION_SIZE, data=data)

  while _population.schedules[0]._fitness != 1.0:
    generation_number += 1
    
    # Calcul la fitness de chaque emploi du temps dans la population
    for schedule in _population.schedules:
        schedule._fitness = schedule.calculate_fitness()
    
    # Tri la population et evolution de cette population
    _population.sort_by_fitness()
    _population = _genetic_algorithm.evolve(population=_population)
    
    for schedule in _population.schedules:
        schedule._fitness = schedule.calculate_fitness()
    
    _population.sort_by_fitness()
    print("fitness : {}".format(_population.schedules[0]._fitness))

  end = time.time()
  print("Nombre de générations : {}   Temps d'exécution : {} s".format(generation_number, end-start))
  print(_population.schedules[0])
  return (generation_number,end-start)

# Methode pour obtenir un csv avec les resultats utiles a l'analyse de l'algorithme
def data_analysis(nb_of_exec,name_of_the_csv):
  with open("analyse/"+name_of_the_csv,"w+") as f:
      f.write("nbRun;nbGeneration;timeExec\n")
      for i in range(nb_of_exec):
          print(i)
          res_tuple = run()
          string = str(i)+";"+str(res_tuple[0])+";"+str(res_tuple[1])+"\n"
          f.write(string)    

if __name__ == '__main__' and __package__ is None:
  run()
import json
import pickle
import boto3 
from pyLambdaFlows.decorator import kernel

# Prend une liste de population en entree
# Genere une nouvelle population avec les meilleurs elites des populations precedentes
# Fait evoluer cette nouvelle population en local sur les serveurs d'AWS
# Evalue cette nouvelle population et envoie des elites a la prochaine couche

from data import Data
from population import Population
from genetic_algorithm import GeneticAlgorithm

@kernel 
def lambda_handler(list_elites_schedules,param):
   
    # Recupere tous les emplois du temps des fonctions Lambda precedentes
    all_elites = []
    for schedules_list in list_elites_schedules:
        for schedule in schedules_list:
            all_elites.append(schedule)  

    # Tri l'ensemble des emplois du temps par ordre decroissant de fitness
    sorted_list = sorted(all_elites, key= lambda schedule: schedule._fitness, reverse=True)

    # Creation de la population avec les elites des populations precedentes
    data = Data()
    newPop = Population(size=param[0][0],data=data, schedules=sorted_list)
    algo = GeneticAlgorithm(data=data,param=param[0]) 
      
    # Evolution de la population en local sur les serveurs d'AWS
    for _ in range(param[0][6]): # param[0][6]==NUMB_OF_GENERATION
        if(newPop.schedules[0]._fitness!=1.0):
            newPop = algo.evolve(population=newPop)

            for schedule in newPop.schedules:
                schedule._fitness=schedule.calculate_fitness()

            newPop.sort_by_fitness()
    
    # Recupere les elites pour les retourner
    elites=[]
    for i in range(param[0][5]): # param[0][5]==NUMB_OF_ELITES
        elites.append(newPop.schedules[i])
    
    return elites
    
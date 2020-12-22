import pyLambdaFlows
import time
import pandas as pd
import json
from MapToMap import MapToMapOp
from time import time

POPULATION_SIZE           = 40
MUTATION_RATE             = 0.1
CROSSOVER_RATE            = 0.9
TOURNAMENT_SELECTION_SIZE = 3
NUMB_OF_ELITE_SCHEDULES   = 4
SPLIT_VAR                 = 1 # Pour le sequentiel aws
NUMB_OF_GENERATION        = 35

def run():

    start=time()
    
    # Creation des sources pyLambdaFlow, cad, l'origine des invocations, on lui donne les donnees d'entrees a la ligne 41
    param = pyLambdaFlows.Source() 
    param.files.append("./population.py") # Il est important de passer a la source les fichiers necessaires pour l'execution des autres fonctions Lambda
    param.files.append("./schedule.py")
    param.files.append("./domain.py")
    param.files.append("./utils.py")
    param.files.append("./data.py")
    param.files.append("./genetic_algorithm.py")
    
    # Le kernel est appelle par un operateur Map mais la liste d'entree (param_list) ne comportant qu'un seul element, ce mapper ne sera appelle qu'une fois
    # Il faut lui passer en donnees la liste des fichiers necessaires a son execution
    b = pyLambdaFlows.op.Map(param, ["./Sequential_Kernel.py", "./population.py", "./schedule.py", "./domain.py", "./utils.py", "./data.py", "./genetic_algorithm.py"]) # dependances avec size=SPLIT_VAR = [[0]]

    # Creation de la liste de parametre envoyee en entree de la source
    param_list = []
    for _ in range (SPLIT_VAR):
        param_list.append([POPULATION_SIZE,MUTATION_RATE,CROSSOVER_RATE,TOURNAMENT_SELECTION_SIZE,SPLIT_VAR,NUMB_OF_ELITE_SCHEDULES,NUMB_OF_GENERATION])

    # Creation d'une nouvelle session Lambda avec les informations de connexion dans 'accessKeys.csv', et lancement de l'invocation des Kernels
    with pyLambdaFlows.Session(credentials_csv="./accessKeys.csv") as sess:
        b.compile(purge=False)
        result = b.eval(feed_dict={param:param_list})

    end=time()

    # Visualisation du resultat
    print(result)
    print("Temps d'exécution : {}".format(end-start))
    print("Schedule: {} avec une fitness = {}".format(result[0],result[0]._fitness))
    

    final_res = (result[0]._fitness,(end-start))
    return final_res

# Methode pour obtenir un csv avec les resultats utiles a l'analyse de l'algorithme
def data_analysis(nb_of_exec,name_of_the_csv):
    with open("analyse/"+name_of_the_csv,"w+") as f:
        for i in range(nb_of_exec):
            print(i)
            res_tuple = run()
            string = str(i)+";"+str(res_tuple[0])+";"+str(res_tuple[1])+"\n"
            f.write(string)
        

if __name__ == "__main__":
    run()
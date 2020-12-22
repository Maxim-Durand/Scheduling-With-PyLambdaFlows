import pyLambdaFlows
import time
import pandas as pd
import json
from MapToMap import MapToMapOp
from time import time

# Chaque parametre defini ici sera envoye aux differents kernels, leur impact respectif est evoque plus en details dans le rapport
POPULATION_SIZE           = 40
MUTATION_RATE             = 0.1
CROSSOVER_RATE            = 0.9
TOURNAMENT_SELECTION_SIZE = 3
NUMB_OF_ELITE_SCHEDULES   = 4
SPLIT_VAR                 = 4
NUMB_OF_GENERATION        = 35

def run():

    start=time()
    
    # Creation des sources pyLambdaFlow, cad, l'origine des invocations, on lui donne les donnees d'entrees a la ligne 48
    param = pyLambdaFlows.Source()
    param.files.append("./population.py") # Il est important de passer a la source les fichiers necessaires pour l'execution des autres fonctions Lambda
    param.files.append("./schedule.py")
    param.files.append("./domain.py")
    param.files.append("./utils.py")
    param.files.append("./data.py")
    param.files.append("./genetic_algorithm.py")

    # Le kernel est appelle par un operateur Map et la liste d'entree (param_list) comporte SPLIT_VAR element, le kernel Create_&_Evaluate sera donc appelle sur chaque element de param_list
    # Il faut lui passer en donnees la liste des fichiers necessaires a son execution
    b = pyLambdaFlows.op.Map(param, ["./Create_And_Evaluate.py", "./population.py", "./schedule.py", "./domain.py", "./utils.py", "./data.py", "./genetic_algorithm.py"]) # dependances size=SPLIT_VAR : [[0], [1], [2], [3]]

    # Les kernels suivant utilisent un operateur cree specialement pour realiser un Map mais pour que tous les resultats soient envoyes a tous les kernels de la layer suivante (voir schema du rapport)
    c = MapToMapOp([b, param], ["./Rate_And_Generate.py", "./population.py", "./schedule.py", "./domain.py", "./utils.py", "./data.py", "./genetic_algorithm.py"]) # dependances size = SPLIT_VAR : [[0, 1, 2, 3], [0, 1, 2, 3], [0, 1, 2, 3], [0, 1, 2, 3]]
    d = MapToMapOp([c, param], ["./Rate_And_Generate.py", "./population.py", "./schedule.py", "./domain.py", "./utils.py", "./data.py", "./genetic_algorithm.py"]) 
    e = MapToMapOp([d, param], ["./Rate_And_Generate.py", "./population.py", "./schedule.py", "./domain.py", "./utils.py", "./data.py", "./genetic_algorithm.py"]) 
    f = MapToMapOp([e, param], ["./Rate_And_Generate.py", "./population.py", "./schedule.py", "./domain.py", "./utils.py", "./data.py", "./genetic_algorithm.py"]) 

    # Creation de la liste de parametre envoyee en entree de la source
    param_list = []
    for _ in range (SPLIT_VAR):
        param_list.append([POPULATION_SIZE,MUTATION_RATE,CROSSOVER_RATE,TOURNAMENT_SELECTION_SIZE,SPLIT_VAR,NUMB_OF_ELITE_SCHEDULES,NUMB_OF_GENERATION])
    
    # Creation d'une nouvelle session Lambda avec les informations de connexion dans 'accessKeys.csv', et lancement de l'invocation des Kernels
    with pyLambdaFlows.Session(credentials_csv="./accessKeys.csv") as sess:
        f.compile(purge=False)
        result = f.eval(feed_dict={param:param_list})

    end=time()

    # Visualisation du resultat
    #print(result)
    res_list=[]
    for i in range (SPLIT_VAR):
        for s in range(NUMB_OF_ELITE_SCHEDULES):
            #print(result[i][s]._fitness)
            res_list.append(result[i][s])            
    print("Temps d'exécution : {}".format(end-start))
    res = sorted(res_list,key = lambda schedule: schedule._fitness,reverse=True)
    print("Schedule: {} avec une fitness = {}".format(res[0],res[0]._fitness))
    
    final_res = (res[0]._fitness,(end-start))
    return final_res

# Methode pour obtenir un csv avec les resultats utiles a l'analyse de l'algorithme
def data_analysis(nb_of_execution,name_of_the_csv):
    with open("analyse/"+name_of_the_csv,"w+") as f:
        for i in range(nb_of_execution):
            print(i)
            res_tuple = run()
            string = str(i)+";"+str(res_tuple[0])+";"+str(res_tuple[1])+"\n"
            f.write(string)
      

if __name__ == "__main__":
    run()
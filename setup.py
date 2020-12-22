from setuptools import setup, find_packages
 
import pyLambdaFlows
 
setup(
    name='pyLambdaFlows',
    version=pyLambdaFlows.__version__,
    packages=find_packages(),
    author="François 'Enderdead' Gauthier-Clerc, Antoine Leloirer, Erme Aydınlı",
 
    # Votre email, sachant qu'il sera publique visible, avec tous les risques
    # que ça implique.
    author_email="francois@gauthier-clerc.fr, aleloare@insa-rennes.fr , eaydinli@insa-rennes.fr",
 
    # Une description courte
    description="A serverless computing library.",
 
    # Une description longue, sera affichée pour présenter la lib
    # Généralement on dump le README ici
    long_description=open('README.md').read(),
    
    install_requires=["boto3",],

    include_package_data=True,
 
    # Une url qui pointe vers la page officielle de votre lib
    url='https://github.com/Enderdead/Pytorch_Quantize_impls',
 
    # Il n'y a pas vraiment de règle pour le contenu. Chacun fait un peu
    # comme il le sent. Il y en a qui ne mettent rien.
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 1 - Planning",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Topic :: Lambda processing",
    ],
    license="MIT",
)
########
# PATH #
########

Ajouter Mixer dans le python path:
C:\Users\jwurhtri\Projet\Python\Mixer\;

############
# SETTINGS #
############

DEBUG: afficher ou non des logs dans la console
db_type: "sql_server" ou "oracle"
osrm: uri du serveur osm
safe_mode: on commit seulement si il n'y a pas eu de problème lors des transactions sinon on rollback tout.

##########
# LAUNCH #
##########

Pour lancer le mixer, j'ai pas encore fait les trucs pour appeler en ligne de commande mais tu peux lancer comme ca pour l'instant:

cmd => ipython

from mixer.gtfs.mixer.controller import Controller
from mixer.settings import DATA_PATH
mix = Controller("ACSTransportationETL", DATA_PATH)
mix.insert_new_gtfs()

#########
# SETUP #
#########

Double clique sur setup.py et ca installe toutes les dépendances du projet
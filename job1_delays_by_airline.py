"""
JOB MAPREDUCE 1 : Comptage des retards par compagnie aérienne
Ce job compte le nombre total de retards pour chaque compagnie aérienne

Input : airlines.csv (id,Airline,Flight,AirportFrom,AirportTo,DayOfWeek,Time,Length,Delay)
Output : Airline \t Nombre_de_retards

Exemple de sortie :
AA    12453
CO    8921
US    15332
"""

import sys

# MAPPER
def mapper():
    """
    Lit chaque ligne du fichier CSV et emet :
    - Clé : Code de la compagnie aerienne (Airline)
    - Valeur : 1 si retard (Delay=1), 0 sinon
    """
    # Sauter la première ligne 
    next(sys.stdin)
    
    for line in sys.stdin:
        # Enlever les espaces et retours à la ligne
        line = line.strip()
        
        # Ignorer les lignes vides
        if not line:
            continue
        
        try:
            # Séparer les colonnes par virgule
            fields = line.split(',')
            
            # Extraire les colonnes necessaires
            airline = fields[1]  # Colonne 1 : Code compagnie 
            delay = int(fields[8])  # Colonne 8 : Retard
            # Émettre seulement si retard = 1
            if delay == 1:
                print(f"{airline}\t1")
        
        except (IndexError, ValueError):
            # Ignorer les lignes mal forme
            continue

# REDUCER
def reducer():
    """
    Age les retards par compagnie aerienne.
    Compte le nombre total de retards pour chaque compagnie.
    """
    current_airline = None
    current_count = 0
    
    for line in sys.stdin:
        # Enlever les espaces
        line = line.strip()
        
        try:
            # Separer la clé et la valeur
            airline, count = line.split('\t')
            count = int(count)
            
            # Si même compagnie, on accumule
            if current_airline == airline:
                current_count += count
            else:
                # Nouvelle compagnie : afficher le resultat précédent
                if current_airline:
                    print(f"{current_airline}\t{current_count}")
                
                # Réinitialiser pour la nouvelle compagnie
                current_airline = airline
                current_count = count
        
        except ValueError:
            continue
    
    # Afficher le dernier rÃ©sultat
    if current_airline:
        print(f"{current_airline}\t{current_count}")

# MAIN
if __name__ == '__main__':
    # Le script peut etre utilise comme mapper ou reducer
    # selon le parametre passe
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'mapper':
            mapper()
        elif sys.argv[1] == 'reducer':
            reducer()
    else:
        # Par dÃ©faut, on utilise mapper
        mapper()
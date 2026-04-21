"""
JOB MAPREDUCE 4 : Calcul de la durée moyenne de vol par route
Ce job calcule la durée moyenne de vol pour chaque route aérienne
Input : airlines.csv
Output : Route \t Durée_moyenne \t Nombre_vols

Exemple de sortie :
JFK-LAX    360.5    1523
SFO-IAH    205.3    892
"""

import sys

# MAPPER
def mapper():
    """
    Émet pour chaque vol :
    - Clé : Route (AirportFrom-AirportTo)
    - Valeur : Durée du vol en minutes
    """
    next(sys.stdin)  # Sauter l'en-tête
    
    for line in sys.stdin:
        line = line.strip()
        
        if not line:
            continue
        
        try:
            fields = line.split(',')
            
            airport_from = fields[3]   # Aéroport départ
            airport_to = fields[4]     # Aéroport arrivée
            length = int(fields[7])    # Durée en minutes
            
            # Créer la route
            route = f"{airport_from}-{airport_to}"
            
            # Émettre route et durée
            print(f"{route}\t{length}")
        
        except (IndexError, ValueError):
            continue

# REDUCER
def reducer():
    """
    Calcule la durée moyenne pour chaque route
    """
    current_route = None
    total_duration = 0
    count = 0
    
    for line in sys.stdin:
        line = line.strip()
        
        try:
            route, duration = line.split('\t')
            duration = int(duration)
            
            # Nouvelle route
            if current_route != route:
                # Afficher la moyenne de la route précédente
                if current_route:
                    avg_duration = total_duration / count if count > 0 else 0
                    print(f"{current_route}\t{avg_duration:.2f}\t{count}")
                
                # Réinitialiser
                current_route = route
                total_duration = 0
                count = 0
            
            # Accumuler
            total_duration += duration
            count += 1
        
        except ValueError:
            continue
    
    # Dernière route
    if current_route:
        avg_duration = total_duration / count if count > 0 else 0
        print(f"{current_route}\t{avg_duration:.2f}\t{count}")

# MAIN

if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == 'mapper':
            mapper()
        elif sys.argv[1] == 'reducer':
            reducer()
    else:
        mapper()

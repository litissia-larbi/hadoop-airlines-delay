"""
JOB MAPREDUCE 3 : Analyse des retards par jour de la semaine
Ce job calcule le pourcentage de retards pour chaque jour de la semaine

Input : airlines.csv
Output : Jour \t Total_vols \t Retards \t Pourcentage

Exemple de sortie :
1    75432    12453    16.5
2    78921    14332    18.2
...
"""

import sys

# MAPPER
def mapper():
    """
    Émet pour chaque vol :
    - Clé : Jour de la semaine (1-7)
    - Valeur : "total:1" OU "delay:1" selon si retard ou non
    """
    next(sys.stdin)  # Sauter l'en-tête
    
    for line in sys.stdin:
        line = line.strip()
        
        if not line:
            continue
        
        try:
            fields = line.split(',')
            
            day_of_week = fields[5]  # Jour 
            delay = int(fields[8])   # Retard 
            
            # Émettre le total de vols
            print(f"{day_of_week}\ttotal:1")
            
            # Émettre si retard
            if delay == 1:
                print(f"{day_of_week}\tdelay:1")
        
        except (IndexError, ValueError):
            continue

# REDUCER
def reducer():
    """
    Calcule le total de vols et le total de retards par jour
    """
    current_day = None
    total_flights = 0
    total_delays = 0
    
    for line in sys.stdin:
        line = line.strip()
        
        try:
            day, value = line.split('\t')
            
            # Nouvelle journée
            if current_day != day:
                # Afficher les résultats du jour précédent
                if current_day:
                    percentage = (total_delays / total_flights * 100) if total_flights > 0 else 0
                    print(f"{current_day}\t{total_flights}\t{total_delays}\t{percentage:.2f}")
                
                # Réinitialiser
                current_day = day
                total_flights = 0
                total_delays = 0
            
            # Compter
            if value.startswith('total:'):
                total_flights += 1
            elif value.startswith('delay:'):
                total_delays += 1
        
        except ValueError:
            continue
    
    # Dernier jour
    if current_day:
        percentage = (total_delays / total_flights * 100) if total_flights > 0 else 0
        print(f"{current_day}\t{total_flights}\t{total_delays}\t{percentage:.2f}")

# MAIN
if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == 'mapper':
            mapper()
        elif sys.argv[1] == 'reducer':
            reducer()
    else:
        mapper()

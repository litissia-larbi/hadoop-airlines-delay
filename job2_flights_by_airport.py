"""
JOB MAPREDUCE 2 : Comptage des vols par aéroport de départ
Ce job compte le nombre total de vols partant de chaque aéroport

Input : airlines.csv
Output : Code_Aeroport \t Nombre_de_vols

Exemple de sortie :
JFK    35420
LAX    42103
ORD    51234
"""

import sys

# MAPPER
def mapper():
    """
    Lit chaque ligne et émet :
    - Clé : Code de l'aéroport de départ (AirportFrom)
    - Valeur : 1 (pour compter)
    """
    # Sauter la première ligne (en-tête)
    next(sys.stdin)
    
    for line in sys.stdin:
        line = line.strip()
        
        if not line:
            continue
        
        try:
            fields = line.split(',')
            
            # Extraire l'aéroport de départ (colonne 3)
            airport_from = fields[3]
            
            # Émettre 1 pour chaque vol
            print(f"{airport_from}\t1")
        
        except IndexError:
            continue

# REDUCER
def reducer():
    """
    Compte le nombre total de vols par aéroport
    """
    current_airport = None
    current_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        
        try:
            airport, count = line.split('\t')
            count = int(count)
            
            if current_airport == airport:
                current_count += count
            else:
                if current_airport:
                    print(f"{current_airport}\t{current_count}")
                
                current_airport = airport
                current_count = count
        
        except ValueError:
            continue
    
    # Dernier aéroport
    if current_airport:
        print(f"{current_airport}\t{current_count}")

# MAIN
if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == 'mapper':
            mapper()
        elif sys.argv[1] == 'reducer':
            reducer()
    else:
        mapper()

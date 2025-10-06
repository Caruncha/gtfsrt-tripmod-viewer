# Checklist de validations TripModifications

## Règles (spec GTFS‑rt)
1) **Un trip ne doit pas être assigné à >1 TripModifications le même service_date** → erreur si collision sur (sélecteur de trip, service_date).  
   _Spec_ : MUST NOT assign a trip to more than one TripModifications on a given service date.  
   Réf : https://gtfs.org/documentation/realtime/feed-entities/trip-modifications/

2) **Spans d’une même `Modification` non chevauchants** → erreur si deux spans se recoupent ; fusion attendue si non contigus.  
   Réf : https://github.com/google/transit/blob/master/gtfs-realtime/spec/en/trip-modifications.md

3) **`ReplacementStop.stop_id` routable** (`location_type=0`) → erreur si `location_type ≠ 0`.  
   Réf (ReplacementStop) : https://docs.rs/gtfs-realtime/latest/gtfs_realtime/struct.ReplacementStop.html

4) **`ReplacementStop.travel_time_to_stop` monotone croissant** → erreur si la suite n’est pas croissante (sauf cas particulier ref = premier arrêt).  
   Réf : https://docs.rs/gtfs-realtime/latest/gtfs_realtime/struct.ReplacementStop.html

5) **ETA aux nouveaux arrêts** : présence d’un TripUpdate avec **`ModifiedTripSelector`** → **WARN** si absent (sinon pas d’ETA aux replacement stops).  
   Réf : https://gtfs.org/documentation/realtime/feed-entities/trip-modifications/

6) **Forme du détour** : si un `Shape` temps réel (encoded polyline) est fourni, l’utiliser ; sinon fallback par la chaîne des `replacement_stops`.  
   Réf : https://github.com/google/transit/blob/master/gtfs-realtime/proto/gtfs-realtime.proto (champ `shape`) et  
   https://docs.rs/gtfs-realtime/latest/gtfs_realtime/struct.Shape.html

## Heuristiques qualité (optionnelles)
- Distance max entre arrêts supprimés et arrêts de remplacement (ex : ≤ 300 m).  
- Ecart de durée : délai propagé vs. somme des `travel_time_to_stop`.  
- Tolérance si `shapes.txt` manquant : fallback via la chaîne d’arrêts.

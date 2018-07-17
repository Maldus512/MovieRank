# MovieRank

## Formato dataset
- Id film
- Id utente
- utilita'
- score
- timestamp
- titolo
- contenuto

## Possibili analisi sui dati
- **Pagerank Utente** : costruire un grafo per ogni film; i nodi del grafo sono gli utenti che l'hanno recensito e vengono collegati
 agli utenti che hanno dato uno score uguale (o simile). Partendo da una media delle utilita' degli utenti, si incrementa lo score
 degli utenti che danno giudizi simili ad altri con uno score piu' alto, e in modo simile lo si diminuisce.
 
- **Suggerimento contenuto**: in base allo score di utilita' di un utente, riceve suggerimenti in base a utenti simili
 (su film che non ha ancora visto).
 
- Correlazione tra review lunghe e utili
- Correlazione tra utente e score
- Correlazione tra film e score
- Correlazione tra film, score e data

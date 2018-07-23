# MovieRank

## Formato dataset
- Id film
- Id utente
- helpfulness
- score
- timestamp
- titolo
- contenuto

## Possibili analisi sui dati
- **Pagerank Utente - GLOBALE** : costruire un grafo globale per tutti i film; i nodi del grafo sono gli utenti e vengono collegati tra loro quelli che hanno dato uno score uguale (o simile) a, più o meno, gli stessi film. Partendo da una media delle utilita' degli utenti, si incrementa lo score degli utenti che danno giudizi simili ad altri con uno score piu' alto, e in modo simile lo si diminuisce.
 
 - **Pagerank Utente - LOCALE** : costruire un grafo per ogni film; i nodi del grafo sono gli utenti che l'hanno recensito e vengono collegati agli utenti che hanno dato uno score uguale (o simile). Partendo da una media delle utilita' degli utenti, si incrementa lo score degli utenti che danno giudizi simili ad altri con uno score piu' alto, e in modo simile lo si diminuisce.
 
- **Suggerimento contenuto**: in base allo score di utilita' di un utente, riceve suggerimenti in base a utenti simili
 (su film che non ha ancora visto).
 
- Correlazione tra review lunghe e utili -> UGO
- Correlazione tra utente e score        -> MATTIA
- Correlazione tra utente e helpfulness  -> FATTO
- Correlazione tra film e score          -> FATTO
- Correlazione tra film, score e data    -> FEFI

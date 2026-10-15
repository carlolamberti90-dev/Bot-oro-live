# Monitor visivo: distribuzione di prova

Directory autonoma: il main.py analitico e il suo servizio restano invariati.

Creare su Render un web service Docker FREE dal repository, usando
`deploy/visual/Dockerfile` e contesto `.`. Health check `/health`.
Impostare `SETUP_PASSWORD` a una password casuale di almeno 24 caratteri,
`TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_ID` nelle variabili del nuovo servizio.
Non inserire segreti nel repository. L'utente HTTP è `carlo`.

Aprire l'URL HTTPS del servizio, autenticarsi, quindi Apri TradingView.
Il browser è sul server. Toccare il campo nell'immagine e usare Inserisci testo,
Invio e Tab per il login. Aggiornare l'immagine per controllare il risultato.
Il profilo resta sul server, non viene esportato. In caso di verifica Google
o CAPTCHA usare direttamente una modalità di login disponibile per il proprio
account; il programma non aggira verifiche.

Verificare feed OANDA:XAUUSD, indicatore originale e parametri 3/22/4/88/2.5.
Nascondere indicatori aggiuntivi. Prima avviare il controllo senza notifiche.
Controllare /status: tutti gli otto timeframe devono avere catture recenti e
notification_ready=true. Un processo attivo non prova catture valide.
Solo dopo questo controllo avviare la prova Telegram (fermare prima il processo).
Il primo ciclo crea la baseline ed è silenzioso. Confrontare una nuova bubble
reale con notifica e latenza. Nessuna validazione live è dichiarata dal deploy.

La pagina è protetta da HTTP Basic su HTTPS, anche per screenshot e comandi;
i POST richiedono stessa origine e header dedicato. /health è pubblico e
riporta solo disponibilità della pagina, mai l'operatività del rilevatore.
Il browser di login viene chiuso prima di aprire il monitor sul medesimo profilo.

## Limiti del piano gratuito

Render Free può sospendersi e perde il filesystem ai riavvii. La directory
/tmp/bubble-monitor conserva profilo e SQLite soltanto durante la vita dell'istanza.
Non costituisce monitor persistente 24/7. Dopo una perdita del disco occorre
rifare login e baseline. Non viene attivato alcun artificio per tenere sveglio
il servizio. Per permanenza serve un host con disco persistente e accesso privato.
Non inviare segreti nella chat: usare i campi Environment del servizio Render.

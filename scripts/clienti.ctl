OPTIONS (
 SKIP=1,
 ROWS=1000,
 PARALLEL=true,
 DIRECT=true,
 SKIP_INDEX_MAINTENANCE=true
)
LOAD DATA
CHARACTERSET UTF8
infile "clienti\clienti*.csv"
APPEND
INTO TABLE SOGGETTI
	FIELDS TERMINATED BY ","
	OPTIONALLY ENCLOSED BY '"'
	TRAILING NULLCOLS
(
key_soggetti,
nome,
cognome,
comune_residenza,
nazione_nascita,
titolo_di_studio,
eta_cliente,
data_iscrizione_sportello_online date "yyyy-mm-dd",
canale_contatto_preferenziale,
rating_creditizio,
tipologia_cliente,
retention_value,
vas
)
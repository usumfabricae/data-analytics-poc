OPTIONS (
 SKIP=1,
 ROWS=1000,
 PARALLEL=true,
 DIRECT=true,
 SKIP_INDEX_MAINTENANCE=true
)
LOAD DATA
CHARACTERSET UTF8
infile "credito\credito*.csv"
APPEND
INTO TABLE PRODOTTI
	FIELDS TERMINATED BY ","
	OPTIONALLY ENCLOSED BY '"'
	TRAILING NULLCOLS
(
data_affido date "yyyy-mm-dd",
data_scadenza date "yyyy-mm-dd",
importo , 
key_soggetti
)
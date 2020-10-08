OPTIONS (
 SKIP=1,
 ROWS=1000,
 PARALLEL=true,
 DIRECT=true,
 SKIP_INDEX_MAINTENANCE=true
)
LOAD DATA
CHARACTERSET UTF8
infile "punti_di_fornitura\punti_di_fornitura*.csv"
APPEND
INTO TABLE PUNTI_DI_FORNITURA
	FIELDS TERMINATED BY ","
	OPTIONALLY ENCLOSED BY '"'
	TRAILING NULLCOLS
(
regione,
indirizzo ,
key_punti_di_fornitura 
)
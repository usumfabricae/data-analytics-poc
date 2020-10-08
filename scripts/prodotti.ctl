OPTIONS (
 SKIP=1,
 ROWS=1000,
 PARALLEL=true,
 DIRECT=true,
 SKIP_INDEX_MAINTENANCE=true
)
LOAD DATA
CHARACTERSET UTF8
infile "prodotto\prodotti*.csv"
APPEND
INTO TABLE PRODOTTI
	FIELDS TERMINATED BY ","
	OPTIONALLY ENCLOSED BY '"'
	TRAILING NULLCOLS
(
key_prodotti ,
nome_prodotto , 
F0 ,
F1,
F2,
F3,
GAS,
data_inizio_validita,
data_fine_validita
)
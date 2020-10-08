OPTIONS (
 SKIP=1,
 ROWS=1000,
 PARALLEL=true,
 DIRECT=true,
 SKIP_INDEX_MAINTENANCE=true
)
LOAD DATA
CHARACTERSET UTF8
infile "contratti\contratti*.csv"
APPEND
INTO TABLE CONTRATTI
	FIELDS TERMINATED BY ","
	OPTIONALLY ENCLOSED BY '"'
	TRAILING NULLCOLS
(
key_punti_di_fornitura ,
nome_commerciale , 
vettore ,
data_attivazione_fornitura date "yyyy-mm-dd",
anno_prima_attivazione_fornitura ,
canale_di_vendita  ,
key_soggetti ,
key_contratti ,
codice_contratto ,
data_cessazione_fornitura date "yyyy-mm-dd"
)
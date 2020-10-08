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
key_contratti ,
nome_commerciale , 
vettore ,
key_punti_di_fornitura ,
data_attivazione_fornitura date "yyyy-mm-dd",
data_cessazione_fornitura date "yyyy-mm-dd",
anno_prima_attivazione_fornitura ,
canale_di_vendita  ,
codice_contratto ,
key_soggetti 
)
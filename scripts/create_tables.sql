create table soggetti (
key_soggetti number (10) not null,
nome varchar2(128) not null, 
cognome varchar2(128) not null,
comune_residenza varchar2(128) not null,
nazione_nascita varchar2(128) not null,
titolo_di_studio varchar2(128) not null,
eta_cliente number(10) not null,
data_iscrizione_sportello_online date,
canale_contatto_preferenziale varchar2(128) not null,
rating_creditizio varchar2(128) not null,
tipologia_cliente varchar2(128) not null,
cas varchar2(128) not null,
primary key (key_soggetti)
);

create table contratti (
key_contratti number (10) not null,
nome_commerciale varchar2(128) not null, 
vettore varchar2(128) not null,
key_punti_di_fornitura number(10) not null,
data_attivazione_fornitura date not null,
data_cessazione_fornitura date not null,
anno_prima_attivazione_fornitura number(10) not null,
canale_di_vendita  varchar2(128) not null,
codice_contratto varchar2(128) not null,
key_soggetti number(10) not null,
primary key (key_contratti)
);
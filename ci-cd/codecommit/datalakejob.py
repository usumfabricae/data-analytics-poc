import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

import pyspark.sql.functions as F 
from  pyspark.sql import Window

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    dyf_soggetti = dfc.select("Soggetti")
    dyf_contratti = dfc.select("Contratti")
    dyf_credito = dfc.select("Credito")
    dyf_prodotti = dfc.select("Prodotti")
    dyf_punti_di_fornitura = dfc.select("PuntiDiFornitura")
    
#### Deduplica Soggetti    
    deduplica_soggetti=dyf_soggetti.toDF()
    windowSpec=Window.partitionBy(deduplica_soggetti.nome,deduplica_soggetti.cognome).\
    orderBy(F.col("key_soggetti").desc())
    deduplica_soggetti=deduplica_soggetti.withColumn("rank",F.row_number().over (windowSpec)).filter (F.col("rank")==1)
        
### Numero Contratti per soggetto
    df_contratti_per_soggetto=dyf_contratti.toDF()
    df_contratti_per_soggetto=df_contratti_per_soggetto.groupBy("key_soggetti").count()
    
### Debito Medio per soggetto    
    df_debito_medio_per_cliente=dyf_credito.toDF()   
    df_debito_medio_per_cliente=df_debito_medio_per_cliente.withColumn("d_importo", (F.col("importo").\
            substr(F.lit(1), F.instr(F.col("importo"), '€')-2)).cast('double'))
    df_debito_medio_per_cliente=df_debito_medio_per_cliente.groupBy("key_soggetti").agg (F.mean("d_importo"))
    df_debito_medio_per_cliente=df_debito_medio_per_cliente.withColumnRenamed("avg(d_importo)","debito_medio")
        
### ELE e Gas Medio, minimo anno_prima_attivazione_fornitura su contratti attivo   
    
    df_contratti=dyf_contratti.toDF()    
    df_contratti=df_contratti.withColumn("ts_data_attivazione_fornitura",F.to_timestamp (df_contratti.data_attivazione_fornitura)).\
        withColumn ("ts_data_cessazione_fornitura",F.to_timestamp (df_contratti.data_cessazione_fornitura)).\
        drop("key_punti_di_fornitura","data_cessazione_fornitura","codice_contratto","key_contratti",
            "canale_di_vendita","anno_prima_attivazione_fornitura")
    
    cd=F.current_timestamp()
    df_contratti=df_contratti.filter (df_contratti.ts_data_cessazione_fornitura >= cd).\
            filter (df_contratti.ts_data_attivazione_fornitura <= cd)
    
    df_prodotti=dyf_prodotti.toDF()
    df_prodotti=df_prodotti.withColumn("ts_data_inizio_validita",F.to_timestamp (df_prodotti.data_inizio_validita)).\
        withColumn ("ts_data_fine_validita",F.to_timestamp (df_prodotti.data_fine_validita)).\
        drop ('data_inizio_validita','data_fine_validita','key_prodotti')
    df_prodotti=df_prodotti.withColumnRenamed("nome_prodotto","nome_commerciale")
    df_prodotti=df_prodotti.withColumn("ELE", (df_prodotti.f0+ df_prodotti.f1+ df_prodotti.f2+ df_prodotti.f3)/4).\
        drop('f0','f1','f2','f3')
    
    df_tariffe_contratti=df_contratti.join (df_prodotti,"nome_commerciale").\
        withColumn ("realGas",F.when (F.col('vettore')=='GAS',df_prodotti.gas).otherwise (None)).\
        withColumn ("realEle",F.when (F.col('vettore')=='ELE',df_prodotti.ELE).otherwise (None)).\
        drop ('gas','ELE')
    
    df_tariffe_soggetti=df_tariffe_contratti.groupby (df_tariffe_contratti.key_soggetti).\
        agg(F.min("data_attivazione_fornitura"),F.mean("realGas"),F.mean("realEle"))
    
    df_tariffe_soggetti=df_tariffe_soggetti.withColumnRenamed("min(data_attivazione_fornitura)","data_attivazione_fornitura").\
        withColumnRenamed("avg(realGas)","media_GAS").\
        withColumnRenamed("avg(realEle)","media_ELE")

## Calcolo dell'indice di churn e del canale di contatto preferenziale
    df_contratti=dyf_contratti.toDF()
    df_soggetti_canale_vendita=df_contratti.groupBy (df_contratti.key_soggetti,df_contratti.canale_di_vendita).\
            agg (F.count(df_contratti.canale_di_vendita))
    
    windowSpec=Window.partitionBy("key_soggetti").\
            orderBy(F.col("count(canale_di_vendita)").desc())
    
    df_soggetti_canale_vendita=df_soggetti_canale_vendita.\
        withColumn("rank",F.row_number().over (windowSpec)).filter (F.col("rank")==1)
    
    df_soggetti_canale_vendita=df_soggetti_canale_vendita.drop ('count(canale_di_vendita)','rank')
    df_contratti_churn=df_contratti.withColumn ("hadChurn",F.when (F.col('data_cessazione_fornitura')<cd,1).otherwise (0))
    df_contratti_churn=df_contratti_churn.groupBy("key_soggetti").agg(F.sum("hadChurn"))
    df_contratti_churn=df_contratti_churn.\
        withColumn("Churn",F.when (F.col('sum(hadChurn)')>=1,1).otherwise (0)).\
        drop("sum(hadChurn)")
    
### Unpivot tabella regioni
    df_contratti=dyf_contratti.toDF()
    df_fornitura=dyf_punti_di_fornitura.toDF()
    df_fonitura_per_contratto=df_contratti.join(df_fornitura,"key_punti_di_fornitura").\
        groupBy ("key_soggetti","regione").\
        pivot("regione").\
        agg (F.count("key_punti_di_fornitura"))
    
### Preparazione Output    
    output=deduplica_soggetti.join (df_contratti_per_soggetto,"key_soggetti").\
        join(df_debito_medio_per_cliente,"key_soggetti").\
        join(df_tariffe_soggetti,"key_soggetti").\
        join(df_contratti_churn,"key_soggetti").\
        join (df_soggetti_canale_vendita,"key_soggetti").\
        join (df_fonitura_per_contratto,"key_soggetti")
    
    dyf_output = DynamicFrame.fromDF(output, glueContext, "output")
    return(DynamicFrameCollection({"CustomTransform0": dyf_output}, glueContext))   


## @params: [JOB_NAME]
#RIMUOVERE COMMENTO PER USARE IN GLUE
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME','sourcedatabase','targetbucket'])

sourcedatabase=args['sourcedatabase']
targetbucket=args['targetbucket']

print ("STARTING")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
#FINE RIMUOVERE COMMENTO PER USARE IN GLUE


## @type: DataSource
## @args: [database = "datalake", table_name = "l_orcl_admin_prodotti", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = sourcedatabase, table_name = "l_orcl_admin_prodotti", transformation_ctx = "DataSource0")
## @type: DataSource
## @args: [database = "datalake", table_name = "l_orcl_admin_credito", transformation_ctx = "DataSource4"]
## @return: DataSource4
## @inputs: []
DataSource4 = glueContext.create_dynamic_frame.from_catalog(database = sourcedatabase, table_name = "l_orcl_admin_credito", transformation_ctx = "DataSource4")
## @type: DataSource
## @args: [database = "datalake", table_name = "_temp_l_orcl_admin_soggetti", transformation_ctx = "DataSource3"]
## @return: DataSource3
## @inputs: []
DataSource3 = glueContext.create_dynamic_frame.from_catalog(database = sourcedatabase, table_name = "l_orcl_admin_soggetti", transformation_ctx = "DataSource3")
## @type: DataSource
## @args: [database = "datalake", table_name = "l_orcl_admin_punti_di_fornitura", transformation_ctx = "DataSource2"]
## @return: DataSource2
## @inputs: []
DataSource2 = glueContext.create_dynamic_frame.from_catalog(database = sourcedatabase, table_name = "l_orcl_admin_punti_di_fornitura", transformation_ctx = "DataSource2")
## @type: DataSource
## @args: [database = "datalake", table_name = "l_orcl_admin_contratti", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = sourcedatabase, table_name = "l_orcl_admin_contratti", transformation_ctx = "DataSource1")


## @type: DataSink
## @args: [connection_type = "s3", format = "parquet", connection_options = {"path": "s3://fede-analytics-694275606777/transformed/customer_view_churn_analys/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "parquet", connection_options = {"path": "s3://{}/customer_view_churn_analys/".format(targetbucket), "partitionKeys": []}, transformation_ctx = "DataSink0")

job.commit()

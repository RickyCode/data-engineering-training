#!/usr/bin/env python
# coding: utf-8

# ## Imports

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName('comando3').getOrCreate()


# ## Load files

dataframes_dict = { 
                'facturas'            : spark.read.parquet('./datasets/bkpf_vbrk.parquet'), 
                'facturas_x_clientes' : spark.read.parquet('./datasets/vbpa_kna1.parquet'),
                'materiales'          : spark.read.parquet('./datasets/vbrp_mara_marm.parquet'),
                'paises'              : spark.read.parquet('./datasets/T005T.parquet')
                }


# ### Select dataframes, filter and join

def set_filters_and_joins(dataframes_dict):
    
    tabla_factura = dataframes_dict['facturas'].select('Factura','Clase_factura','Ejercicio','Per_contable')
    facturas_x_clientes = dataframes_dict['facturas_x_clientes'].select('Factura','Posicion_de_Factura','Nombre','Pais','Tipo_de_Cliente','Cliente_Solicitante','Cliente_Pagador','Cliente_Destinatario_de_Mercancia','Cliente_Destinatario_de_factura')
    tabla_materiales = dataframes_dict['materiales'].select('Factura','Posicion','Material','Volumen','Unidad_de_medida_Volumen_Destino')
    maestro_paises = dataframes_dict['paises'].where((col('Idioma') == 'S') & (col('Mandante')==400))
    
    main_table = tabla_materiales.join(tabla_factura, 'Factura','left')
    main_table = main_table.join(facturas_x_clientes, 'Factura','left') 
    main_table = main_table.join(maestro_paises, 'Pais', 'Left')
    main_table = main_table.select('Factura','Clase_factura','Posicion','Material','Volumen','Unidad_de_medida_Volumen_Destino','Per_contable','Ejercicio','Pais','Pais_descr','Tipo_de_Cliente','Cliente_Solicitante','Cliente_Pagador','Cliente_Destinatario_de_Mercancia','Cliente_Destinatario_de_factura')
    
    return { 'main_table' : main_table }


# ## Realizar cuatro consultas
#   - Cantidad de facturas por tipo de cliente
#   - Top 10 Volumen total por país de cliente destinatario mercancía
#   - Top 5 Volumen total por cliente solicitante
#   - Volumen total por clase de factura, de mayor a menor volumen

# ### Consulta 1: Cantidad de facturas por tipo de cliente (numero_de_cliente)

def set_consulta1(dataframes_dict):
    
    main_table = dataframes_dict['main_table']
    
    consulta1 = main_table.groupBy('Tipo_de_Cliente').count()                     .withColumnRenamed('count','Cant_Factura')                     .orderBy(col('Cant_Factura').desc())
    
    return { 'consulta1' : consulta1 }


# ### Consulta 2: Top 10 Volumen total por país de cliente destinatario mercancía

def set_consulta2(dataframes_dict):
    
    main_table = dataframes_dict['main_table']
    
    # filtrando los nulos de Cliente_Destinatario_de_Mercancia
    consulta2 = main_table.where(col("Cliente_Destinatario_de_Mercancia").isNotNull())                 .groupBy("Pais","Cliente_Destinatario_de_Mercancia").sum("Volumen")                 .select("Pais","Cliente_Destinatario_de_Mercancia", round("sum(Volumen)",2).alias("Volumen_total"))                 .orderBy(col("Volumen_total").desc())                .withColumn("Top_10", monotonically_increasing_id()+1)                 .select("Top_10", "Pais","Cliente_Destinatario_de_Mercancia","Volumen_total").limit(10)
    
    # filtrando los nulos de Pais y Cliente_Destinatario_de_Mercancia
    consulta2_no_nulls = main_table.where((col('Cliente_Destinatario_de_Mercancia')                 .isNotNull()) & (col('Pais').isNotNull()))                 .groupBy('Pais','Cliente_Destinatario_de_Mercancia').sum('Volumen')                 .select("Pais","Cliente_Destinatario_de_Mercancia", round("sum(Volumen)",2).alias("Volumen_total"))                 .orderBy(col('Volumen_total').desc())                 .withColumn("Top_10", monotonically_increasing_id()+1)                 .select("Top_10", "Pais","Cliente_Destinatario_de_Mercancia","Volumen_total").limit(10)

    return {
        'consulta2'          : consulta2,
        'consulta2_no_nulls' : consulta2_no_nulls
        }


# ### Consulta 3: Top 5 Volumen total por cliente solicitante

def set_consulta3(dataframes_dict):
    
    main_table = dataframes_dict['main_table']
    
    consulta3 = main_table.where(col('Cliente_Solicitante').isNotNull())                 .groupBy('Pais','Cliente_Solicitante').sum('Volumen')                 .select("Pais", "Cliente_Solicitante", round("sum(Volumen)",2).alias("Volumen_total"))                 .orderBy(col('Volumen_total').desc())                 .withColumn("Top_5", monotonically_increasing_id()+1)                 .select("Top_5", "Pais","Cliente_Solicitante","Volumen_total").limit(5)
    
    consulta3_no_nulls = main_table.where((col('Cliente_Solicitante').isNotNull()) & (col('Pais').isNotNull()))                 .groupBy('Pais','Cliente_Solicitante').sum('Volumen')                 .select("Pais", "Cliente_Solicitante", round("sum(Volumen)",2).alias("Volumen_total"))                 .orderBy(col('Volumen_total').desc())                 .withColumn("Top_5", monotonically_increasing_id()+1)                 .select("Top_5", "Pais","Cliente_Solicitante","Volumen_total").limit(5)

    return {
        'consulta3'          : consulta3,
        'consulta3_no_nulls' : consulta3_no_nulls
        }


# ### Consulta 4: Volumen total por clase de factura, de mayor a menor volumen

def set_consulta4(dataframes_dict):
    
    main_table = dataframes_dict['main_table']
    
    consulta4 = main_table.groupBy('Clase_factura').sum('Volumen')                 .select('Clase_factura', round('sum(Volumen)',2).alias('Volumen_total'))                 .orderBy(col('Volumen_total').desc()).limit(5)
    
    consulta4_no_nulls = main_table.where(col('Clase_factura').isNotNull())                 .groupBy('Clase_factura').sum('Volumen')                 .select('Clase_factura', round('sum(Volumen)',2).alias('Volumen_total'))                 .orderBy(col('Volumen_total').desc()).limit(5)

    return {
        'consulta4'          : consulta4,
        'consulta4_no_nulls' : consulta4_no_nulls
        }


# ### Clean dataframes_dict to setup final dictionary

def set_delete_dfs(dataframes_dict):
    
    to_delete_list = [
        'facturas',
        'facturas_x_clientes',
        'materiales',
        'paises'
    ]
    
    for delete_df in to_delete_list:
        del dataframes_dict[delete_df]
    
    return dataframes_dict


# # Main Function:

def gen_comando3(dataframes_dict):
    
    functions_list = [
        set_filters_and_joins,
        set_consulta1,
        set_consulta2,
        set_consulta3,
        set_consulta4,
        set_delete_dfs
    ]

    for function in functions_list:
        print(function)
        result_element = function(dataframes_dict)
        dataframes_dict.update(result_element)
    
    return dataframes_dict

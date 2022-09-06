import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName('desafio-python3').getOrCreate()

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
spark.conf.set('spark.sql.repl.eagerEval.truncate', 30)
spark.conf.set('spark.sql.repl.eagerEval.maxNumRows', 100)

dataframes_dict = {}
dataframes_dict['metadata_de_facturas'] = spark.read.parquet(
    "/Users/tamara/Desktop/Principal/KRANIO/DESAFIO-PYTHON/dataset/bkpf_vbrk.parquet")
dataframes_dict['facturas_por_cliente'] = spark.read.parquet(
    "/Users/tamara/Desktop/Principal/KRANIO/DESAFIO-PYTHON/dataset/vbpa_kna1.parquet")
dataframes_dict['materiales'] = spark.read.parquet(
    "/Users/tamara/Desktop/Principal/KRANIO/DESAFIO-PYTHON/dataset/vbrp_mara_marm.parquet")
dataframes_dict['paises'] = spark.read.parquet(
    "/Users/tamara/Desktop/Principal/KRANIO/DESAFIO-PYTHON/dataset/T005T.parquet")

# Filter Mandante - Idioma Pais

df_metadata_facturas = dataframes_dict['metadata_de_facturas'].where(
    col('Mandante') == '400')

df_facturas_por_cliente = dataframes_dict['facturas_por_cliente'].where(
    col('Mandante') == '400')

df_materiales = dataframes_dict['materiales'].where(col('Mandante') == '400')

df_paises = dataframes_dict['paises'].where(
    col('Mandante') == '400').where(col('Idioma') == 'S')

# Select tables
df_metadata_facturas = df_metadata_facturas.select(
    'Factura', 'Clase_factura', 'Ejercicio', 'Per_contable').where(col('Factura').isNotNull())

df_facturas_por_cliente_destinatario = df_facturas_por_cliente.select('Factura', 'Posicion_de_Factura', 'Pais', 'Nombre', 'Numero_de_Cliente', 'Cliente_Destinatario_de_Mercancia')\
    .where(col('Cliente_Destinatario_de_Mercancia').isNotNull())\
    .withColumnRenamed('Pais', 'Pais_cliente')
df_materiales = df_materiales.select(
    'Factura', 'Posicion', 'Material', 'Unidad_de_medida_Volumen_Destino', 'Volumen')

df_cliente_solicitante_factura = df_facturas_por_cliente.select(
    'Factura', 'Cliente_solicitante').where(col('Cliente_solicitante').isNotNull())
df_paises = df_paises.select('Idioma', 'Pais', 'Pais_descr')

# Join tables


def set_posicion_destinatario_mercancia(dataframes_dict):

    df1 = df_materiales
    df2 = df_facturas_por_cliente_destinatario

    join_cond = [(df1.Factura == df2.Factura) & (
        df1.Posicion == df2.Posicion_de_Factura)]

    df_result = df1.join(df2, join_cond, 'left')\
        .drop(df2.Posicion_de_Factura)\
        .drop(df2.Factura)

    return {'add_posicion_destinatario_mercancia': df_result}


def set_clase_factura(dataframes_dict):

    df = dataframes_dict['add_posicion_destinatario_mercancia']

    df_result = df.join(df_metadata_facturas, 'Factura', 'left')

    return {'add_clase_factura': df_result}


def set_cliente_solitante(dataframes_dict):

    df = dataframes_dict['add_clase_factura']

    df_result = df.join(df_cliente_solicitante_factura, 'Factura', 'left')

    return {'add_cliente_solitante': df_result}


def set_paises_desc(dataframes_dict):

    df = dataframes_dict['add_cliente_solitante']

    df_result = df.join(df_paises, df.Pais_cliente == df_paises.Pais, 'left')\
        .drop(df_paises.Pais)

    return {'add_paises_descripcion': df_result}


def select_columns(dataframes_dict):

    df_final = dataframes_dict['add_paises_descripcion']\
                    .select('Factura', 'Clase_factura', 'Posicion',
                    'Material', 'Volumen', 'Unidad_de_medida_Volumen_Destino',
                    'Per_contable', 'Ejercicio','Nombre', 'Numero_de_Cliente','Cliente_solicitante', 'Cliente_Destinatario_de_Mercancia',
                    'Pais_cliente', 'Pais_descr')\
                    .withColumnRenamed('Nombre', 'Nombre_cliente')

    return{'add_select_columns' : df_final}

# dataframe final


def main(dataframes_dict):

    functions_list = [
        set_posicion_destinatario_mercancia,
        set_clase_factura,
        set_cliente_solitante,
        set_paises_desc,
        select_columns
    ]

    for function in functions_list:
        print(function)
        result_element = function(dataframes_dict)
        dataframes_dict.update(result_element)

    return dataframes_dict['add_select_columns']


dataframe_final = main(dataframes_dict)
print("\n")

# Cuadratura Final Volumen
print('materiales_original :', dataframes_dict['materiales'].agg(sum(col('Volumen'))))
print('add_select_columns :',  dataframes_dict['add_select_columns'].agg(sum(col('Volumen'))))

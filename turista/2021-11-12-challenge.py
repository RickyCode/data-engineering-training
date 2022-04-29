## FORMA 1 ********************************************************************************************/
## Utilizando sólo la biblioteca 'boto3' y su recurso 'client' para leer el archivo.
## *El método .decode('utf-8') lo que hace es transformar el contenido del objeto de bytes a texto.

import boto3


def lambda_handler(event, context):

    # Llamo al recurso y lo guardo en una variable
    s3_client = boto3.client("s3")

    # Nombres del bucket y del (ruta) objeto
    bucket_name = "data-engineering-training"
    key = "datalake/ingestion-layer/raw-zone/turista/2021-11-12-challenge/bikes.csv"

    # Tomo el objeto desde su ruta
    file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)

    # Leo el contenido del objeto (csv) y lo decodifico
    file_content = file_obj["Body"].read().decode("utf-8")

    print(file_content)


## FORMA 2 ********************************************************************************************/
## Utilizo 2 bibliotecas para leer el objeto:
## 'pandas', para usar pandas es necesario aplicar una capa (layer) que es la biblioteca de pandas.
## 'IO', para decodificar el objeto de bytes a texto o vice versa.

# import boto3
# import pandas as pd
# import io

# def lambda_handler(event, context):

#     # Llamo al recurso y lo guardo en una variable
#     s3_client = boto3.client("s3")

#     # Nombres del bucket y del (ruta) objeto
#     bucket_name = 'data-engineering-training'
#     key = 'datalake/ingestion-layer/raw-zone/turista/2021-11-12-challenge/bikes.csv'

#     # Tomo el objeto desde su ruta
#     file_obj = s3_client.get_object(Bucket= bucket_name, Key= key)

#     ## Utilizo pandas (pd) para leer el objeto.
#     ## (Comentar una de las opciones para que no falle)

#     ## OPCION 1
#     # Leo el objeto directamente con pandas
#     #df = pd.read_csv(file_obj['Body'], sep= ',', encoding='utf8')

#     ## OPCION 2
#     # Leo el objeto con pandas y con la biblioteca IO
#     df = pd.read_csv(io.BytesIO(file_obj['Body'].read()), sep= ',', encoding='utf8')

#     #* Como el objeto es un csv, este tipo de archivo es de texto por lo que
#     # se puede leer directamente con pandas y con IO. Si fuera un archivo de bytes
#     # sería necesario en este caso utilizar la biblioteca IO para que decodifique
#     # el contenido del archivo (objeto).

#     print(df)


## FORMA 3 ********************************************************************************************/
## Utilizando sólo la biblioteca 'boto3' y su recurso 'resourse' para leer el archivo.
## *El método .decode('utf-8') lo que hace es transformar el contenido del objeto de bytes a texto.

# import boto3

# def lambda_handler(event, context):

#     # Llamo al recurso y lo guardo en una variable
#     s3 = boto3.resource('s3')

#     # Nombres del bucket y del (ruta) objeto
#     bucket_name = 'data-engineering-training'
#     key = 'datalake/ingestion-layer/raw-zone/turista/2021-11-12-challenge/bikes.csv'

#     # Leo el contenido del csv y lo imprimo
#     content = s3.Object(bucket_name, key).get()['Body'].read().decode('utf-8')

#     print(content)


## FORMA 4 ********************************************************************************************/
## Utilizando sólo la biblioteca 'boto3' y su recurso 'resourse' para leer el archivo.
## *El método .splitlines() lo que hace es entregar una lista que contiene a cada tupla como string.
## (No es necesario usar .splitlines(); en la 'FORMA 3' se muestra el mismo código más eficiente).

# import boto3

# def lambda_handler(event, context):

#     # Llamo al recurso y lo guardo en una variable
#     s3 = boto3.resource('s3')

#     # Nombres del bucket y del (ruta) csv
#     bucket_name = 'data-engineering-training'
#     key = 'datalake/ingestion-layer/raw-zone/turista/2021-11-12-challenge/bikes.csv'

#     # Leo el contenido del csv
#     csv_file = s3.Object(bucket_name, key).get()['Body'].read().decode('utf-8').splitlines()

#     # Recorro por tuplas del csv y las ordeno como arreglo para que las tuplas se impriman en orden hacia abajo.
#     for line in csv_file:
#         arr = line.split()
#         print(arr)

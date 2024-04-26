"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #
    # df = pd.read_csv(
    #     "clusters_report.txt",
    #     sep='\t'
    # )
    archivo = "clusters_report.txt"
    df = pd.read_fwf(
        archivo,
        skiprows = 4,
        index_col = False,
        names = [
            "cluster",
            "cantidad_de_palabras_clave",
            "porcentaje_de_palabras_clave",
            "principales_palabras_clave",
        ]
    )

    df.cluster = df.cluster.ffill().astype(int)

    df.principales_palabras_clave = df.groupby(["cluster"])["principales_palabras_clave"].transform(lambda x: " ".join(x)).str.replace(".","")

    df.drop_duplicates(subset=["cluster"], inplace=True)

    df.reset_index(drop=True, inplace=True)

    df.cantidad_de_palabras_clave = df.cantidad_de_palabras_clave.astype(int)

    df.porcentaje_de_palabras_clave = df.porcentaje_de_palabras_clave.str.replace("%","")
    
    df.porcentaje_de_palabras_clave = df.porcentaje_de_palabras_clave.str.replace(",",".")
    
    df.porcentaje_de_palabras_clave = df.porcentaje_de_palabras_clave.astype(float)

    df.principales_palabras_clave = df.principales_palabras_clave.replace(r"\s+"," ",regex=True)


    return df

print(ingest_data())

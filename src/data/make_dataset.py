# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import glob
import json
import hashlib

import pandas as pd


def anonimizar_ruts(columna_ruts: pd.Series) -> pd.Series:
    """
    Anonimiza una serie de RUTs utilizando un valor de sal almacenado en un archivo JSON. El RUT
    debe estar sin puntos, guiones ni DV.

    Parámetros:
    columna_ruts (pd.Series): Serie de pandas que contiene los RUTs a anonimizar.

    Retorna:
    pd.Series: Serie de pandas con los RUTs anonimizados.
    """
    try:
        # Cargar el archivo de sales
        with open("data/external/salts.json", encoding="utf-8") as file:
            sales = json.load(file)
            sal_rut = sales["Rut Paciente"]

    except FileNotFoundError:
        print(
            "Debes tener el archivo de las sales actualizado para anonimizar los RUTs. La base "
            "de datos NO pudo ser procesada."
        )
        exit()

    # Convertir la sal a bytes y combinar con los RUTs
    sal_bytes = bytes.fromhex(sal_rut)
    ruts_bytes = columna_ruts.astype(str).str.strip().str.encode(encoding="utf-8")

    # Concatena sal y rut
    ruts_con_sal = sal_bytes + ruts_bytes

    # Aplicar SHA-256 para anonimizar
    ruts_anonimizados = ruts_con_sal.apply(lambda x: hashlib.sha256(x).hexdigest())

    return ruts_anonimizados


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    # Lee los archivos en raw y los une
    archivos_sabana = glob.glob(f"{input_filepath}/*.txt")
    df = pd.concat(
        pd.read_csv(archivo, sep="\t", encoding="utf-16le") for archivo in archivos_sabana
    )

    # Anonimiza los RUTS segun la sal
    df["RUT"] = anonimizar_ruts(df["RUT"])

    # Exporta base de datos
    df.to_csv(f"{output_filepath}/df_procesada.csv")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

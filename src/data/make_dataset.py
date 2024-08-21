# -*- coding: utf-8 -*-
import glob
import hashlib
import json
import logging
from pathlib import Path

import click
import pandas as pd
from dotenv import find_dotenv, load_dotenv


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


def unificar_formato_ruts(columna_ruts: pd.Series, digito_verificador=True) -> pd.Series:
    """
    Funcion que consolida el formato de los RUTs de una persona. Elimina puntos, guiones y deja
    sin digito verificador. Los RUTs entrantes DEBEN tener el digito verificador si o si.

    Parámetros:
    columna_ruts (pd.Series): Serie de pandas que contiene los RUTs a anonimizar.

    Retorna:
    pd.Series: Serie de pandas con los RUTs anonimizados.
    """
    # Elimina puntos, guiones, espacios y 0s al inicio. Ademas convierte las letras a mayuscula
    ruts_limpios = (
        columna_ruts.astype(str)
        .str.replace(r"\.|-|\s", "", regex=True)
        .str.strip()
        .str.upper()
        .str.lstrip("0")
    )

    if digito_verificador:
        # Elimina el digito verificador si es que lo tiene
        ruts_limpios = ruts_limpios.str[:-1]

    # Elimina las K que podrian haber quedado al final luego de la limpiza (por haber imputado -Kk)
    ruts_limpios = ruts_limpios.str.rstrip("K")

    return ruts_limpios


def formatear_fechas_ingreso_y_egreso(df):
    tmp = df.copy()

    # Formatea las fechas de ingreso, dejando el dia primero, luego el mes y el anio
    fecha_ingreso = (
        tmp["DIA_ING"].astype(str)
        + "-"
        + tmp["MES_ING"].astype(str)
        + "-"
        + tmp["ANO_ING"].astype(str)
    )
    fecha_ingreso = pd.to_datetime(fecha_ingreso, dayfirst=True)

    # Formatea las fechas de egreso, dejando el dia primero, luego el mes y el anio
    fecha_egreso = (
        tmp["DIA_EGR"].astype(str)
        + "-"
        + tmp["MES_EGR"].astype(str)
        + "-"
        + tmp["ANO_EGR"].astype(str)
    )
    fecha_egreso = pd.to_datetime(fecha_egreso, dayfirst=True)

    tmp["FECHA_INGRESO"] = fecha_ingreso
    tmp["FECHA_EGRESO"] = fecha_egreso

    return tmp


def clean_column_names(df):
    """
    Cleans the column names of a DataFrame by converting to lowercase, replacing spaces with
    underscores, ensuring only a single underscore between words, and removing miscellaneous symbols.

    :param df: The input DataFrame.
    :type df: pandas DataFrame

    :return: The DataFrame with cleaned column names.
    :rtype: pandas DataFrame
    """
    tmp = df.copy()

    # Clean and transform the column names
    cleaned_columns = (
        df.columns.str.lower()
        .str.normalize("NFD")
        .str.encode("ascii", "ignore")
        .str.decode("utf-8")
        .str.replace(
            r"[^\w\s]", "", regex=True
        )  # Remove all non-alphanumeric characters except spaces
        .str.replace(r"\s+", "_", regex=True)  # Replace spaces with underscores
        .str.replace(r"_+", "_", regex=True)  # Ensure only a single underscore between words
        .str.strip("_")
    )

    # Assign the cleaned column names back to the DataFrame
    tmp.columns = cleaned_columns

    return tmp


def leer_grd_interno(input_filepath):
    print("> Leyendo GRD Interno")
    # Lee todos los archivos Excel
    bases_grd = glob.glob(f"{input_filepath}/grd/Egresos*.xlsx")

    # Une todos los archivos Excel, y elimina fila con total de egresos
    df = pd.concat(pd.read_excel(archivo, header=2) for archivo in bases_grd)

    # Limpia el nombre de las columnas
    df = clean_column_names(df)

    # Elimina fila con total de egresos
    df = df.query("hospital_codigo != 'Suma Total'").copy()

    # Limpia y Anonimiza RUTs
    df["RUT_LIMPIO"] = unificar_formato_ruts(df["rut"])
    # df["rut"] = anonimizar_ruts(df["rut"])

    # Agrega el anio del egreso
    df["ano_de_egreso"] = pd.to_datetime(df["fecha_de_egreso"]).dt.year

    # Ordena por el anio de egreso
    df = df.sort_values("ano_de_egreso")

    return df


def leer_grd_sabanas(input_filepath):
    print("> Leyendo GRD Sabanas")
    # Lee los archivos en raw y los une
    archivos_sabana = glob.glob(f"{input_filepath}/grd_sabanas/*.txt")
    df = pd.concat(
        pd.read_csv(archivo, sep="\t", encoding="utf-16le") for archivo in archivos_sabana
    )

    # Anonimiza los RUTS segun la sal
    # df["RUT"] = anonimizar_ruts(df["RUT"])

    # Limpia los RUTs con la rutina usual, a pesar de que estos ya esten en formato requerido
    ruts_con_dv = df["RUT"].astype(str) + df["DV"].astype(str)
    df["RUT_LIMPIO"] = unificar_formato_ruts(ruts_con_dv)

    # Formatea las fechas de ingreso y egreso
    df = formatear_fechas_ingreso_y_egreso(df)

    # Ordena por la fecha de egreso
    df = df.sort_values("FECHA_EGRESO")

    return df


def leer_grd_con_hmd(input_filepath):
    print(f"> Leyendo GRD con Produccion de HMD")
    archivos_grd = glob.glob(f"{input_filepath}/grd_hmd/*.csv")

    # Lee los archivos en raw y los une
    df = pd.concat(pd.read_csv(archivo, sep="|") for archivo in archivos_grd)

    # Limpia los nombres de las columnas
    df = clean_column_names(df)

    # Limpia los RUTs, quitando puntos, espacios, etc y el digito verificador
    df["RUT_LIMPIO"] = unificar_formato_ruts(df["rut"])

    # Ordena la base de datos segun fecha de egreso
    df = df.sort_values("fech_alta")

    return df


@click.command()
@click.argument("input_filepath", type=click.Path(exists=True))
@click.argument("output_filepath", type=click.Path())
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    grd_sabanas = leer_grd_sabanas(input_filepath)
    grd_interno = leer_grd_interno(input_filepath)
    grd_hmd = leer_grd_con_hmd(input_filepath)

    # Exporta base de datos
    grd_sabanas.to_csv(f"{output_filepath}/grd_sabanas.csv")
    grd_interno.to_csv(f"{output_filepath}/grd_interno.csv")
    grd_hmd.to_csv(f"{output_filepath}/grd_hmd.csv")


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

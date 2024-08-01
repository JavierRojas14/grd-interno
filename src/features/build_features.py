import pandas as pd


# Función para cargar datos de GRD desde un archivo CSV
def cargar_datos_grd(ruta_archivo, columnas=None):
    df = pd.read_csv(ruta_archivo, usecols=columnas)
    return df


# Función para formatear columnas de fecha
def formatear_columnas_fecha(df, columnas):
    for col in columnas:
        df[col] = pd.to_datetime(df[col])
    return df


# Función para limpiar datos de operaciones quirúrgicas
def limpiar_datos_operaciones_quirurgicas(df):
    df_limpio = df.copy()
    df_limpio = df_limpio.query("suspendida == 'NO' and duracion.notna()")
    return df_limpio


# Función para fusionar datos de GRD con datos de operaciones quirúrgicas
def fusionar_grd_con_operaciones_quirurgicas(
    grd_df, operaciones_df, rut_grd, fecha_ingreso_grd, fecha_egreso_grd
):
    # Une por el RUT del paciente
    df_fusionado = grd_df.merge(
        operaciones_df, how="inner", left_on=rut_grd, right_on="ID_PACIENTE"
    )
    # Filtra las int q que esten fuera del periodo de hospitalizacion
    df_fusionado = df_fusionado.query(
        f"fecha >= {fecha_ingreso_grd} and fecha <= {fecha_egreso_grd}"
    )
    return df_fusionado


# Función para calcular estadísticas resumidas de las duraciones de las operaciones
def calcular_estadisticas_resumidas(df, columnas_agrupacion, columna_duracion):
    df_resumen = df.groupby(columnas_agrupacion)[columna_duracion].describe().reset_index()
    return df_resumen


# Función para guardar dataframes en Excel
def guardar_en_excel(dataframes, ruta_archivo):
    with pd.ExcelWriter(ruta_archivo) as writer:
        for nombre_hoja, df in dataframes.items():
            df.to_excel(writer, sheet_name=nombre_hoja, index=False)


def buscar_nombre_procedimiento_pabellon(df, operaciones):
    df_de_operacion = df[
        df["nombre_de_la_operacion"].fillna("").str.contains(operaciones, regex=True)
    ]

    resumen_operacion = pd.DataFrame(
        {
            "totales": df.groupby(df.fecha.dt.year).size(),
            "operacion": df_de_operacion.groupby(df_de_operacion.fecha.dt.year).size(),
        }
    )
    resumen_operacion["porcentaje"] = (
        resumen_operacion["operacion"] / resumen_operacion["totales"]
    ) * 100

    return df_de_operacion, resumen_operacion


def buscar_nombre_diagnosticos_pabellon(df, diagnosticos):
    df_de_diags = df[
        (df["primer_diagnostico"].fillna("").str.contains(diagnosticos, regex=True))
        | (df["segundo_diagnostico"].fillna("").str.contains(diagnosticos, regex=True))
    ]

    resumen_diags = pd.DataFrame(
        {
            "totales": df.groupby(df.fecha.dt.year).size(),
            "diags": df_de_diags.groupby(df_de_diags.fecha.dt.year).size(),
        }
    )
    resumen_diags["porcentaje"] = (resumen_diags["diags"] / resumen_diags["totales"]) * 100

    return df_de_diags, resumen_diags


def iterar_en_operaciones_a_buscar(df, dict_textos_a_buscar):
    resultados = {}
    # Itera por el diccinoario de busqueda y guarda los resultados
    for tipo_complicacion, textos_a_buscar in dict_textos_a_buscar.items():
        busqueda = buscar_nombre_procedimiento_pabellon(df, textos_a_buscar)
        resultados[tipo_complicacion] = busqueda

    return resultados


def iterar_en_diagnosticos_a_buscar(df, dict_diags_a_buscar):
    resultados = {}
    # Itera por el diccinoario de busqueda y guarda los resultados
    for tipo_complicacion, textos_a_buscar in dict_diags_a_buscar.items():
        busqueda = buscar_nombre_diagnosticos_pabellon(df, textos_a_buscar)
        resultados[tipo_complicacion] = busqueda

    return resultados

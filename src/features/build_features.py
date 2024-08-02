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


def obtener_resumen_ocurrencia_complicacion(df, df_filtrada):
    # Obtiene el resumen de la cantidad de ocurrencias del DataFrame total y el filtrado
    resumen = pd.DataFrame(
        {
            "totales": df.groupby(["ano_de_intervencion", "especialidad"]).size(),
            "ocurrencia_filtrado": df_filtrada.groupby(
                ["ano_de_intervencion", "especialidad"]
            ).size(),
        }
    )

    # Obtiene el resumen acumulado en el periodo
    resumen_acumulado = resumen.sum()

    # Obtiene los porcentajes de ocurrencia desglosados y acumulados
    resumen["fraccion"] = resumen["ocurrencia_filtrado"] / resumen["totales"]
    porcentaje_acumulado = resumen_acumulado["ocurrencia_filtrado"] / resumen_acumulado["totales"]

    # Obtiene el resumen por especialidad acumulado
    resumen_acumulado_por_especialidad = (
        resumen.reset_index().groupby("especialidad")[["totales", "ocurrencia_filtrado"]].sum()
    )
    resumen_acumulado_por_especialidad["fraccion"] = (
        resumen_acumulado_por_especialidad["ocurrencia_filtrado"]
        / resumen_acumulado_por_especialidad["totales"]
    )

    return resumen, resumen_acumulado, resumen_acumulado_por_especialidad, porcentaje_acumulado


def buscar_nombre_operacion_pabellon(df, operaciones):
    # Filtra la base de datos segun el nombre de la operacion
    return df[df["nombre_de_la_operacion"].fillna("").str.contains(operaciones, regex=True)]


def buscar_nombre_diagnosticos_pabellon(df, diagnosticos):
    # Filtra la base de datos segun el nombre del diagnostico 1 y 2
    return df[
        (df["primer_diagnostico"].fillna("").str.contains(diagnosticos, regex=True))
        | (df["segundo_diagnostico"].fillna("").str.contains(diagnosticos, regex=True))
    ]


def iterar_en_complicaciones_a_buscar(df, dict_textos_a_buscar, tipo_complicacion):
    # Decide que parametro a buscar en la base de datos
    busqueda_a_realizar = {
        "intervencion_quirurgica": buscar_nombre_operacion_pabellon,
        "diagnostico": buscar_nombre_diagnosticos_pabellon,
    }
    funcion_a_ocupar_para_buscar = busqueda_a_realizar[tipo_complicacion]

    # Itera por el diccionario de busqueda y guarda los resultados
    resultados = {}
    df_resultado = pd.DataFrame()
    for nombre_complicacion, textos_a_buscar in dict_textos_a_buscar.items():
        df_filtrada = funcion_a_ocupar_para_buscar(df, textos_a_buscar)
        resumen_filtrado = obtener_resumen_ocurrencia_complicacion(df, df_filtrada)
        resultados[nombre_complicacion] = resumen_filtrado

        # print(f"> {nombre_complicacion}: {resumen_filtrado[2]}")

        # Concatena resultados acumulados en el periodo por complicacion
        resultado_acumulado = resumen_filtrado[2].reset_index()
        resultado_acumulado["complicacion"] = nombre_complicacion
        df_resultado = pd.concat([df_resultado, resultado_acumulado])

    return resultados, df_resultado

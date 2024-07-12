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
def fusionar_grd_con_operaciones_quirurgicas(grd_df, operaciones_df, columnas_fusion):
    df_fusionado = grd_df.merge(operaciones_df, how="inner", left_on="RUT", right_on="ID_PACIENTE")
    df_fusionado = df_fusionado.query("fecha >= FECHA_INGRESO and fecha <= FECHA_EGRESO")
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

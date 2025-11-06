import pandas as pd
import numpy as np
# Se eliminó la importación de statsmodels

# --- 1. Definición de Rutas de Archivos ---
# (Ajusta estas rutas si tus archivos están en otra carpeta)
PATHS = {
    "gasto": "/home/lalo/2025-02-trabajos/datasets/TODO_ME/S1_05_GASTO_TELECOMUNICACIONES.csv",
    "demografia": "/home/lalo/2025-02-trabajos/datasets/TODO_ME/S1_01_INF_DEMOGRAFICA.csv",
    "infraestructura": "/home/lalo/2025-02-trabajos/datasets/TODO_ME/S1_03_SERVICIOS.csv",
    "adopcion": "/home/lalo/2025-02-trabajos/datasets/TODO_ME/S2_BAF_POR_TECONOLOGIA.csv",
    "disponibilidad": "/home/lalo/2025-02-trabajos/datasets/TODO_ME/S1_06_DISPONIBILIDAD_TIC.csv"
}

# --- 2. Funciones de Procesamiento (Etapa 3 del Planteamiento) ---

def process_gasto(path):
    """
    Procesa el gasto (Sección 3.1).
    Agrupa por entidad, calcula el gasto promedio de fijos y móviles, 
    y los suma en GASTO_TOTAL_PROMEDIO.
    """
    try:
        df = pd.read_csv(path)
        # Agrupar por entidad y calcular el promedio de gasto (a través de deciles)
        df_gasto = df.groupby('ENTIDAD')[['GASTO_PM_FIJAS', 'GASTO_PM_MOVILES']].mean()
        # Crear la variable dependiente final
        df_gasto['GASTO_TOTAL_PROMEDIO'] = df_gasto['GASTO_PM_FIJAS'] + df_gasto['GASTO_PM_MOVILES']
        return df_gasto
    except FileNotFoundError:
        print(f"\n--- ERROR CRÍTICO (Archivo No Encontrado) ---")
        print(f"  Función: process_gasto")
        print(f"  Ruta esperada: {path}")
        print(f"  Por favor, verifica que el archivo exista en esa ubicación.")
        return None
    except Exception as e:
        print(f"\n--- ERROR CRÍTICO (Error de Procesamiento) ---")
        print(f"  Función: process_gasto")
        print(f"  Archivo: {path}")
        print(f"  Error: {e}")
        print(f"  Esto puede ser un problema de columnas (KeyError) o datos (ValueError).")
        return None

def process_demografia(path):
    """
    Procesa los datos demográficos (Sección 3.2).
    Filtra por total de entidad (ZONA == 'Nacional') y elimina el total del país.
    Limpia las columnas numéricas.
    """
    try:
        df = pd.read_csv(path)
        # Filtrar por el total de la entidad (ZONA == 'Nacional')
        df_dem = df[df['ZONA'] == 'Nacional'].copy()
        # Eliminar el agregado del país (ENT == 'Nacional')
        df_dem = df_dem[df_dem['ENT'] != 'Nacional']
        # Limpiar números (quitar comas)
        df_dem['HOGARES'] = pd.to_numeric(df_dem['HOGARES'].replace(',', '', regex=True))
        df_dem['HABITANTES'] = pd.to_numeric(df_dem['HABITANTES'].replace(',', '', regex=True))
        # Renombrar columna clave para unir
        df_dem.rename(columns={'ENT': 'ENTIDAD'}, inplace=True)
        return df_dem.set_index('ENTIDAD')[['HOGARES', 'HABITANTES']]
    except FileNotFoundError:
        print(f"\n--- ERROR CRÍTICO (Archivo No Encontrado) ---")
        print(f"  Función: process_demografia")
        print(f"  Ruta esperada: {path}")
        print(f"  Por favor, verifica que el archivo exista en esa ubicación.")
        return None
    except Exception as e:
        print(f"\n--- ERROR CRÍTICO (Error de Procesamiento) ---")
        print(f"  Función: process_demografia")
        print(f"  Archivo: {path}")
        print(f"  Error: {e}")
        print(f"  Esto puede ser un problema de columnas (KeyError) o datos (ValueError).")
        return None

def process_infraestructura(path):
    """
    Procesa los datos de infraestructura (Sección 3.3).
    Pivota la tabla para tener servicios como columnas.
    """
    try:
        df = pd.read_csv(path)
        # Pivotar la tabla
        df_infra = df.pivot(index='ENTIDAD', columns='SERVICIO', values='LINEAS_ACCESOS')
        return df_infra
    except FileNotFoundError:
        print(f"\n--- ERROR CRÍTICO (Archivo No Encontrado) ---")
        print(f"  Función: process_infraestructura")
        print(f"  Ruta esperada: {path}")
        print(f"  Por favor, verifica que el archivo exista en esa ubicación.")
        return None
    except Exception as e:
        print(f"\n--- ERROR CRÍTICO (Error de Procesamiento) ---")
        print(f"  Función: process_infraestructura")
        print(f"  Archivo: {path}")
        print(f"  Error: {e}")
        print(f"  Esto puede ser un problema de columnas (KeyError) o datos (ValueError).")
        return None

def process_adopcion(path):
    """
    Procesa la adopción de tecnología (Sección 3.4).
    Filtra por el período más reciente y pivota la tabla.
    """
    try:
        df = pd.read_csv(path)
        # Encontrar el período más reciente
        periodo_reciente = df['PERIODO'].max()
        print(f"Filtrando adopción por período más reciente: {periodo_reciente}")
        df_reciente = df[df['PERIODO'] == periodo_reciente].copy()
        # Pivotar
        df_adopcion = df_reciente.pivot(index='ENTIDAD', columns='TECNOLOGIA', values='PORCENTAJE')
        # Llenar NaNs con 0 (asumiendo que si no hay reporte es 0%)
        return df_adopcion.fillna(0)
    except FileNotFoundError:
        print(f"\n--- ERROR CRÍTICO (Archivo No Encontrado) ---")
        print(f"  Función: process_adopcion")
        print(f"  Ruta esperada: {path}")
        print(f"  Por favor, verifica que el archivo exista en esa ubicación.")
        return None
    except Exception as e:
        print(f"\n--- ERROR CRÍTICO (Error de Procesamiento) ---")
        print(f"  Función: process_adopcion")
        print(f"  Archivo: {path}")
        print(f"  Error: {e}")
        print(f"  Esto puede ser un problema de columnas (KeyError) o datos (ValueError).")
        return None

def process_disponibilidad(path):
    """
    Procesa la disponibilidad de TICs (Sección 3.5).
    Filtra por total de entidad y pivota.
    """
    try:
        df = pd.read_csv(path)
        # Filtrar por el total de la entidad (ZONA == 'Nacional')
        df_disp = df[df['ZONA'] == 'Nacional'].copy()
        # Eliminar el agregado del país (ENT == 'Nacional')
        df_disp = df_disp[df_disp['ENT'] != 'Nacional']
        # Pivotar
        df_disp_pivot = df_disp.pivot(index='ENT', columns='EQUIPO', values='PORCENTAJE')
        # Renombrar índice para unir
        df_disp_pivot.index.name = 'ENTIDAD'
        return df_disp_pivot
    except FileNotFoundError:
        print(f"\n--- ERROR CRÍTICO (Archivo No Encontrado) ---")
        print(f"  Función: process_disponibilidad")
        print(f"  Ruta esperada: {path}")
        print(f"  Por favor, verifica que el archivo exista en esa ubicación.")
        return None
    except Exception as e:
        print(f"\n--- ERROR CRÍTICO (Error de Procesamiento) ---")
        print(f"  Función: process_disponibilidad")
        print(f"  Archivo: {path}")
        print(f"  Error: {e}")
        print(f"  Esto puede ser un problema de columnas (KeyError) o datos (ValueError).")
        return None

def clean_column_names(df):
    """Limpia los nombres de las columnas para statsmodels."""
    df.columns = df.columns.str.replace(' ', '_', regex=True)
    df.columns = df.columns.str.replace('ó', 'o', regex=True)
    df.columns = df.columns.str.replace('í', 'i', regex=True)
    return df

# --- 3. Ejecución de la Construcción del DataFrame (Etapa 3.6) ---

print("Iniciando construcción de la base de datos maestra...")

df_gasto = process_gasto(PATHS['gasto'])
df_demografia = process_demografia(PATHS['demografia'])
df_infraestructura = process_infraestructura(PATHS['infraestructura'])
df_adopcion = process_adopcion(PATHS['adopcion'])
df_disponibilidad = process_disponibilidad(PATHS['disponibilidad'])

# Lista de DataFrames procesados
dfs_to_merge = [
    df_gasto, 
    df_demografia, 
    df_infraestructura, 
    df_adopcion, 
    df_disponibilidad
]

# Verificar que todos los DFs tengan contenido
if any(df is None for df in dfs_to_merge):
    print("\n--- ERROR: Fallo en la carga de datos ---")
    print("Uno o más archivos no se pudieron procesar (ver mensajes de error arriba).")
    print("El script no puede continuar.")
else:
    # Unir todos los DataFrames usando el índice (ENTIDAD)
    df_master = pd.concat(dfs_to_merge, axis=1)

    # Limpiar nombres de columnas para el modelo
    df_master = clean_column_names(df_master)

    # Eliminar filas con valores NaN (si alguna entidad no está en todos los archivos)
    df_master_clean = df_master.dropna()

    print("\n--- Base de Datos Maestra (df_master_clean) Creada ---")
    print(df_master_clean.head())
    print("\nColumnas disponibles:", df_master_clean.columns.tolist())

    # --- 4. Modelado Eliminado ---
    # (El código de modelado econométrico se ha eliminado según la solicitud)

print("\n--- Procesamiento de datos completado ---")
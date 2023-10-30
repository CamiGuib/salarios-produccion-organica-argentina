#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
  Archivo: desarrollo.py
  Proyecto para Laboratorio de datos, Facultad de Ciencias Exactas y Naturales, UBA
  Integrantes del grupo: Ariel Dembling, Antony Suárez Caina, Camila Guibaudo 
  Fecha: 27/06/23
  Descripción: este es el archivo principal del proyecto. Acá se cargan, exploran
  y curan los datos, se normalizan las tablas, se responden las preguntas 
  propuestas y se generan los gráficos solicitados. 
"""

# =============================================================================
# LIBRERÍAS
# =============================================================================
import funciones as fn
import pandas as pd
import numpy as np
from inline_sql import sql, sql_val
import sys
import seaborn as sns
import matplotlib.pyplot as plt
import os

# Establecer rootdir al directorio actual desde donde se ejecuta el script
rootdir = os.getcwd()
# Agregamos el root_dir del directorio donde está ubicado nuestro módulo a importar
sys.path.append(rootdir)

rootdir += '/TablasOriginales'
pd.options.display.max_columns = None
pd.options.display.max_rows = 25


# Por cada fuente, armamos un diccionario donde metemos el dataframe junto
# con su metadata.
# Nota: Pandas no soporta nombrar ni guardar metadata sobre DataFrames.
# Si bien se puede agregar atributos a un DataFrame, no es conveniente porque
# no todas las operaciones (ej., copy()) los prevén; también existe el método
# .attrs pero es experimental. A fin de poder manipular los DataFrames de
# manera homogénea, optamos por esta solución. Tiene el inconveniente de que
# al momento de utilizar inline_sql es necesario hacerlo sobre una variable
# separada (inline_sql no parsea la referencia al diccionario), pero dado
# el tamaño de estas bases y que esto solo sucede una tabla por vez, no
# tiene efecto significativo en la performance.

# Creamos los dictionaries con metadata.
# Fuente: https://datos.magyp.gob.ar/dataset/padron-de-operadores-organicos-certificados
# Encoding: WINDOWS-1252
# OPERADORES(pais_id, pais, provincia_id, provincia, departamento, localidad, rubro, productos, categoria_id, categoria_desc, Certificadora_id, certificadora_deno, razón social, establecimiento)
operadores = {'name': 'operadores', 'csv': rootdir + '/' +
              'padron-de-operadores-organicos-certificados.csv', 'encoding': 'windows-1252'}

# Fuente: https://cdn.produccion.gob.ar/cdn-cep/datos-por-departamento/salarios/w_median_depto_priv_clae2.csv
# Encoding: UTF-8
# SALARIOS(fecha, codigo_departamento_indec, id_provincia_indec, clae2, w_median)
salarios = {'name': 'salarios', 'csv': rootdir + '/' +
            'w_median_depto_priv_clae2.csv', 'encoding': 'utf-8'}

# Fuente: https://infra.datos.gob.ar/catalog/modernizacion/dataset/7/distribution/7.29/download/localidades-censales.csv
# Encoding: UTF-8
# LOCALIDADES(categoria, centroide_lat, centroide_lon, departamento_id, departamento_nombre, fuente, funcion, id, municipio_id, municipio_nombre, nombre, provincia_id, provincia_nombre)
localidades = {'name': 'localidades', 'csv': rootdir +
               '/' + 'localidades-censales.csv', 'encoding': 'utf-8'}

# Fuente: https://datos.produccion.gob.ar/dataset/puestos-de-trabajo-por-departamento-partido-y-sector-de-actividad/archivo/125bdc76-0205-417a-bf20-76d34dbe184b
# Encoding: UTF-8
# DEPTOS(codigo_departamento_indec, nombre_departamento_indec, id_provincia_indec, nombre_provincia_indec)
deptos = {'name': 'deptos', 'csv': rootdir + '/' +
          'diccionario_cod_depto.csv', 'encoding': 'utf-8'}

# Fuente: https://www.datos.gob.ar/fa_IR/dataset/produccion-salarios-por-departamentopartido-sector-actividad/archivo/produccion_8c7e4f21-750e-4298-93d1-55fe776ed6d4
# Encoding: UTF-8
# CLAE(clae2, clae2_desc, letra, letra_desc)
clae = {'name': 'clae', 'csv': rootdir + '/' +
        'diccionario_clae2.csv', 'encoding': 'utf-8'}

# Fuente: esta tabla fue creada manualmente. Ver detalles en la documentación.
# Encoding: UTF-8
# RUBRO_CATEGORIA_CLAE2(rubro, categoria_id, clae2)
rubro_categoria_clae2 = {'name': 'rubro_categoria_clae2',
                         'csv': rootdir[:-16] + 'TablasLimpias/' + 'rubro_categoria_clae2.csv', 'encoding': 'utf-8'}

# Les agregamos los Dataframes
operadores['df'] = pd.read_csv(
    operadores['csv'], encoding=operadores['encoding'])
salarios['df'] = pd.read_csv(salarios['csv'], encoding=salarios['encoding'])
localidades['df'] = pd.read_csv(
    localidades['csv'], encoding=localidades['encoding'])
deptos['df'] = pd.read_csv(deptos['csv'], encoding=deptos['encoding'])
clae['df'] = pd.read_csv(clae['csv'], encoding=clae['encoding'])
rubro_categoria_clae2['df'] = pd.read_csv(rubro_categoria_clae2['csv'])

# Aplicamos correcciones a nombres de columnas para que todas adhieran
# a la convención snake_case:
operadores['df'] = operadores['df'].rename(
    columns={"Certificadora_id": "certificadora_id", "razón social": "razon_social"})

# Definimos un conjunto de todas las tablas para luego poder iterar sobre ellas:
tablas = (operadores, salarios, localidades, deptos, clae)

# ==============================================================================
# EXPLORACIÓN DE DATOS EN TABLAS Y ANÁLISIS GQM
# ==============================================================================

print("")
print('=========== Exploración de datos en tablas y análisis GQM ===========')
print("")

# %%
######
# Goal 1: analizar presencia de tuplas repetidas en las tablas
# Question 1: dada una tabla o subconjunto de columnas de una tabla,
#             ¿cuántas tuplas repetidas sobrantes hay?
# Metric 1: porcentaje de tuplas duplicadas sobrantes con respecto a las tuplas
#           totales útiles
######

print("* Porcentajes de tuplas duplicadas en cada tabla")
print("")

for i in tablas:
    fn.porcentajeTuplasRepetidasSobrantes(i)

# Observamos que solo la tabla operadores tiene tuplas repetidas
# Corregimos ese problema eliminando dichas tuplas

fn.limpiezaTuplasRepetidas(operadores)

# Validamos la corrección realizada, volviendo a chequar el porcentaje de tuplas
# repetidas sobre la tabla operadores

fn.porcentajeTuplasRepetidasSobrantes(operadores)

# %%
######
# Goal 2: analizar presencia de valores NULL
# Question 2: dada una tabla, ¿cuántos valores NULL hay en cada columna?
# Metric 2.1: porcentaje de valores NULL con respecto a la cantidad de valores
#             totales, para alguna columna de las dadas de una tabla
# Metric 2.2: porcentaje de valores NULL con respecto a la cantidad de valores
#             totales, para todas las columnas dadas de una tabla
######

print("")
print("* Porcentajes de tuplas con valores NULL en alguna de las columnas y en todas sus columnas")
print("")

for i in tablas:
    fn.porcentajeDeTuplasConValoresNullAny(i)
    fn.porcentajeDeTuplasConValoresNullAll(i)

# Por el momento ignoraremos a todos esos valores NULL, ya que, la mayoría de
# ellos no interferirán con nuestro trabajo. De todos modos algunos de ellos si
# lo harán. Para solucionar esos problemas que ocasionan, más adelante
# incorporaremos nuevas columnas a las tablas. Ver más información sobre el
# tratado de valores NULL en la documentación.

# %%
######
# Goal 3: analizar presencia de valores indefinidos (no NULLs)
# Question 3: dada una tabla, ¿cuántos valores indefinidos pero no NULLs hay en
#             cada columna?
# Metric 3: porcentaje de valores indefinidos distintos de NULL con respecto a
#           la cantidad de valores totales, para cada columna de una tabla.
######

# Después de analizar las tablas, asuminmos que los nombres para designar un valor indefinido fueron los contenidos en esta lista.
valores_indefinidos = ["INDEFINIDO", "INDEFINIDA", "SIN DEFINIR", "NC"]

print("")
print("* Porcentaje de tuplas con valores indefinidos " + str(valores_indefinidos) + " de cada tabla")
print("")

for dict_df in tablas:
    for col in dict_df['df'].columns:
        fn.porcentajeDeValoresIndefinidos(
            dict_df, col, lista_de_indefinidos=valores_indefinidos)

# Al igual que en el item anterior, si llegamos a requerir el uso de una columna con valores indefinidos
# incorporaremos una nuevas columnas.

# %%
######
# Goal 4: verificar la correspondencia entre los valores únicos de dos columnas
#         dadas de un dataframe (ej., una columna de IDs y otra de descripciones).
# Question 4: dado un par de columnas ID y Descripción que se correspondan,
#             ¿se cumple que cada ID y Descripción se corresponden unívocamente?
# Metric 4: valor absoluto de la diferencia de cantidades de elementos únicos de
#           ambas columnas.
######

print("")
print("* Diferencia entre la cantidad de ID's y la cantidad de sus correspondientes descripciones para cada tabla")
print("")

fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(operadores, 'pais_id', 'pais')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(
    operadores, 'provincia_id', 'provincia')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(
    operadores, 'categoria_id', 'categoria_desc')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(
    operadores, 'certificadora_id', 'certificadora_deno')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(
    localidades, 'departamento_id', 'departamento_nombre')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(
    localidades, 'provincia_id', 'provincia_nombre')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(
    localidades, 'municipio_id', 'municipio_nombre')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(localidades, 'id', 'nombre')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(
    deptos, 'codigo_departamento_indec', 'nombre_departamento_indec')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(
    deptos, 'id_provincia_indec', 'nombre_provincia_indec')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(clae, 'clae2', 'clae2_desc')
fn.diferenciaDeCantidadesDeValoresUnicosUnsigned(clae, 'letra', 'letra_desc')


# %%
######
# Goal 5: verificar que cada columna tenga un tipo de valor único.
# Question 5: para cada columna, ¿todos sus valores pertenecen a un único tipo?
# Metric 5: proporción de filas correspondiente al tipo de dato más frecuente 
#           en la columna.
######
# Para esta métrica asumiremos que cada columna le corresponde un tipo string o
# numérico, desestimamos los valores NULL, para ello usaremos un diccionario
# donde almacenaremos la cantidad de tipos que aparecen en la columna, ademas la
# función nos retorna dicho diccionario para su uso.

print("")
print("* Porcentaje de valores del mismo tipo en cada columna de cada tabla")
print("")

for dict_df in tablas:
    for col in dict_df['df'].columns:
        fn.proporcionDeTipoMayoritarioPorColumna(dict_df, col)


# %%
######
# Goal 6: evaluar la consistencia de los valores entre columnas análogas de dos 
#         dataframes, basándose en columnas de identificación única.
#         Deseamos entender cuán consistentes son los datos entre dos fuentes
#         diferentes. Cada fuente (dataframe) tiene una columna de 
#         identificación única, y otras columnas relacionadas que deseamos 
#         comparar en términos de consistencia.
# Question 6: para un conjunto dado de columnas relacionadas entre dos 
#             dataframes, ¿cuál es la proporción de datos inconsistentes?
# Metric 6: porcentaje de valores inconsistentes en cada columna relacionada 
#           entre los dos dataframes.    
######

print("")
print("* Porcentajes de inconsistencias entre columnas análogas de dataframes")

# operadores: provincia_id, provincia, departamento
# salarios: codigo_departamento_indec, id_provincia_indec, clae2
# localidades: departamento_id, departamento_nombre, provincia_id, provincia_nombre
# deptos: codigo_departamento_indec, nombre_departamento_indec, id_provincia_indec, nombre_provincia_indec
# clae: clae

columnas_a_evaluar = {
    'caso 1': {'col_id': ('departamento_nombre', 'departamento'),                        # localidades, operadores
               'col_dep': (['provincia_id', 'provincia_nombre'], ['provincia_id', 'provincia'])},
    'caso 2': {'col_id': ('codigo_departamento_indec', 'departamento_id'),               # deptos, localidades
               'col_dep': (['nombre_departamento_indec', 'id_provincia_indec', 'nombre_provincia_indec'], ['departamento_nombre', 'provincia_id', 'provincia_nombre'])},
    'caso 3': {'col_id': ('nombre_departamento_indec', 'departamento'),                  # deptos, operadores
               'col_dep': (['id_provincia_indec', 'nombre_provincia_indec'], ['provincia_id', 'provincia'])},
    'caso 4': {'col_id': ('codigo_departamento_indec', 'codigo_departamento_indec'),     # deptos, salarios
               'col_dep': (['id_provincia_indec'], ['id_provincia_indec'])},
    'caso 5': {'col_id': ('departamento', 'departamento_nombre'),                        # operadores, localidades
               'col_dep': (['provincia_id', 'provincia'], ['provincia_id', 'provincia_nombre'])},
    'caso 6': {'col_id': ('departamento', 'nombre_departamento_indec'),                  # operadores, deptos
               'col_dep': (['provincia_id', 'provincia'], ['id_provincia_indec', 'nombre_provincia_indec'])}
}

fn.consistenciaExtendida(localidades, operadores,
                         dicc_columnas=columnas_a_evaluar['caso 1'])
fn.consistenciaExtendida(
    deptos, localidades, dicc_columnas=columnas_a_evaluar['caso 2'])
fn.consistenciaExtendida(
    deptos, operadores, dicc_columnas=columnas_a_evaluar['caso 3'])
fn.consistenciaExtendida(
    deptos, salarios, dicc_columnas=columnas_a_evaluar['caso 4'])
fn.consistenciaExtendida(operadores, localidades,
                         dicc_columnas=columnas_a_evaluar['caso 5'])
fn.consistenciaExtendida(
    operadores, deptos, dicc_columnas=columnas_a_evaluar['caso 6'])


# %%
# Goal 7: verificar consistencia de datos entre distintas tablas.
# Question 7: dado un subconjunto de atributos A que aparece en una tabla 1 y 
#             otra tabla 2, ¿todos los valores que aparecen en el subconjunto de 
#             atributos A en la tabla 1 también aparecen en el subconjunto de 
#             atributos A de la tabla 2?
# Metric 7: dado un subconjunto de atributos A que aparece en una tabla 1 y otra 
#           tabla 2, porcentaje de valores contenidos en el subconjunto de 
#           atributos A de la tabla 1 que no aparecen en el subconjunto de atributos A de la tabla 2.
######

print("")
print("* Porcentajes de subconjuntos de atributos inconsistentes entre dos dataframes")
print("")

columnas_a_evaluar = {
    'caso 1': {'departamento': ['departamento_nombre']},   # Join flexible
    # Flexibilizamos join
    'caso 1.1': {'departamento': ['departamento_nombre', 'municipio_nombre', 'nombre']},
    'caso 1.2': {'departamento': ['departamento_nombre', 'municipio_nombre', 'nombre'],
                 'provincia_id': ['provincia_id'],
                 'provincia': ['provincia_nombre']},         # añadimos restricciones
    # Flexibilizamos el primer join
    'caso 2': {('departamento', 'localidad'): ['departamento_nombre', 'municipio_nombre', 'nombre']},
    'caso 2.1': {('departamento', 'localidad'): ['departamento_nombre', 'municipio_nombre', 'nombre'],
                 'provincia_id': ['provincia_id'],
                 'provincia': ['provincia_nombre']},         # añadimos restricciones al join anterior
    'caso 3': {'provincia_id': ['provincia_id'],
               'provincia': ['provincia_nombre']},  # cubrimiento enfocado en provincias -- operadores/localidades
    # join flexible -- localidades/deptos
    'caso 4': {'departamento': ['nombre_departamento_indec']},
    'caso 4.1': {'departamento': ['nombre_departamento_indec'],    # añadimos resttricciones al join anterior
                 'provincia_id': ['id_provincia_indec'],
                 'provincia': ['nombre_provincia_indec']},
    # Flexibilizamos el caso 3
    'caso 5': {('departamento', 'localidad'): ['nombre_departamento_indec']},
    'caso 5.1': {('departamento', 'localidad'): ['nombre_departamento_indec'],  # añadimos restricciones al join anterior
                 'provincia_id': ['id_provincia_indec'],
                 'provincia': ['nombre_provincia_indec']},
    'caso 6': {'provincia_id': ['id_provincia_indec'],   # Nos enfocamos en provincias
               'provincia': ['nombre_provincia_indec']},   # operadores/deptos
    'caso 7': {'departamento_nombre': ['nombre_departamento_indec'],
               'departamento_id': ['codigo_departamento_indec'],
               'provincia_id': ['id_provincia_indec'],
               'provincia_nombre': ['nombre_provincia_indec']},    # localidades/deptos
    'caso 8': {'codigo_departamento_indec': ['codigo_departamento_indec'],
               'id_provincia_indec': ['id_provincia_indec']},     # salarios/deptos
    # salarios/clae
    'caso 9': {'clae2': ['clae2']}
}

# Las fuentes suministradas se asocian entre si mediante algunas columnas clave, por ejemplo:
# -> operadores se asocia con localidades mediante las columnas: departamentos (operadores) y departamento_nombre (localidades), id_provincia (operadores) y id_provincia (localidades)
# -> operadores se asocia con deptos mediante  departamentos (operadores)
# Vamos a separar los casos importantes
fn.porcentajeDeValoresInexistentes(
    operadores, localidades, dict_columnas=columnas_a_evaluar['caso 1'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    operadores, localidades, dict_columnas=columnas_a_evaluar['caso 1.1'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    operadores, localidades, dict_columnas=columnas_a_evaluar['caso 1.2'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    operadores, localidades, dict_columnas=columnas_a_evaluar['caso 2'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    operadores, localidades, dict_columnas=columnas_a_evaluar['caso 2.1'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    operadores, localidades, dict_columnas=columnas_a_evaluar['caso 3'], lista_de_indefinidos=valores_indefinidos)

fn.porcentajeDeValoresInexistentes(
    operadores, deptos, dict_columnas=columnas_a_evaluar['caso 4'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    operadores, deptos, dict_columnas=columnas_a_evaluar['caso 4.1'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    operadores, deptos, dict_columnas=columnas_a_evaluar['caso 5'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    operadores, deptos, dict_columnas=columnas_a_evaluar['caso 5.1'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    operadores, deptos, dict_columnas=columnas_a_evaluar['caso 6'], lista_de_indefinidos=valores_indefinidos)

fn.porcentajeDeValoresInexistentes(
    localidades, deptos, dict_columnas=columnas_a_evaluar['caso 7'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    salarios, deptos, dict_columnas=columnas_a_evaluar['caso 8'], lista_de_indefinidos=valores_indefinidos)
fn.porcentajeDeValoresInexistentes(
    salarios, clae, dict_columnas=columnas_a_evaluar['caso 9'], lista_de_indefinidos=valores_indefinidos)

# %%
######
# Goal 8: verificar que los valores númericos de cada atributo estén dentro del
#         rango correcto
# Question 8: qué porcentaje de valores de una columna cuyo tipo de dato es 
#             númerico, no está dentro del rango esperado
# Metric 8: porcentaje de valores del tipo númerico que está fuera del rango 
#           esperado.
######

print("")
print("* Porcentajes de valores de tipo numérico que están fuera del rango esperado")
print("")


# Vamos a analizar las columnas de las tablas que serán importantes para las 
# consultas y análisis de datos
fn.porcentajeDeValoresFueraDeRango(salarios, columna='w_median')

# Existen 22.42% de valores w_median fuera de rango, en este caso paticular, son 
# números negativos.
# Para que estos valores no repercutan en nuestos análisis, la solución es 
# descartar estos valores en las consultas de SQL colocando como condicional que 
# el valor de w_median > 0.

# %%
# =============================================================================
# NORMALIZACIÓN
# =============================================================================

print("")
print("")
print('=========== Creación de la nueva tabla: rubro_categoria_clae2 ===========')
print("")

# Añadimos nuevas columnas id_rubro y id_razon_social_establecimiento a operadores
fn.añadirIDs(operadores, ['rubro'])
fn.añadirIDs(operadores, ['razon_social', 'establecimiento'])

# reasignamos ids de municipio, al der id_municipio una PK de la tabla 
# municipios, es importante que la columna id_municipio no tenga valores NULL
fn.reasignarIDs(localidades, tupla_columnas=(
    'municipio_id', 'municipio_nombre'), lista_de_indefinidos=valores_indefinidos)


# Definimos los dataframes para mayor comodidad
padron = operadores['df']
loc = localidades['df']
salario = salarios['df']
clae2 = clae['df']
depto = deptos['df']
rcc = rubro_categoria_clae2['df']

# Dejamos constancia del proceso que da origen a la tabla rubro_categoria_clae2.
# Antes de hacerlo, es importante notar que no existen rubros que se
# correspondan con más de 1 categoría (y de hecho todos tienen una, incluso
# el rubro NULL):
consultaSQL = '''
        SELECT rubro, categoria_id, COUNT(categoria_id) AS cantidad
        FROM (SELECT DISTINCT rubro, categoria_id
              FROM padron)
        GROUP BY rubro, categoria_id
        HAVING cantidad <> 1
    '''
if len(sql ^ consultaSQL) == 0:
    # La tabla rubro_categoria_clae2 fue creada manualmente a partir de:
    lista_de_rubros = padron[['rubro', 'categoria_id',
                              'categoria_desc']].drop_duplicates()
    print("La tabla rubro_categoria_clae2 se crea manualmente a partir del dataframe lista_de_rubros:", lista_de_rubros)
    # La asignación se realizó en forma manual, mediante una planilla de cálculo,
    # en función de las claes disponibles en esta lista, y en base
    # a la semántica de cada rubro y su categoría provista.
    # Se asignó a cada rubro una clae2 tomada de:
    lista_de_claes = clae2[['clae2', 'clae2_desc']].drop_duplicates()
    print("A cada rubro allí se le asigna manualmente un par de valores clae2 y clae2_desc de los disponibles en el dataframe lista_de_claes:", lista_de_claes)
    # En muchos casos, la asignación resultó obvia. En varios otros casos resultó
    # algo trabajosa ya que la asignación podía realizarse potencialmente a más de
    # una clae. Por ejemplo, un rubro podía incluir la elaboración de productos de
    # alimentación (aceite de girasol) y de uso industrial (aceite de colza). En
    # tales casos, se optó por tomar la decisión de elegir la clae de manera de
    # que la mayoría de los productos mencionados se correspondieran con la clae.
    # En ciertos casos, la decisión era igualmente ambigua y se optó de manera
    # arbitraria pero razonable en base a la descripción del rubro y su categoría.
    # Todas las asignaciones quedaron documentadas en el archivo CSV de esta tabla
    # manual.
    # En algunos casos, el rubro no era lo suficientemente descriptivo como para
    # elegir una clae. En estos casos se desambiguó qué clae
    # correspondía al rubro en base a listar los productos que los
    # establecimientos de dicho rubro producen según consta en la propia tabla
    # de operadores.
    print("Para desambiguar, en algunos rubros es necesario consultar los productos que los establecimientos de ese rubro producen:")
    print("----------------")
    lista_de_rubros_para_desambiguar = ['ELABORACION', 'ELABORACION ', 'ELABORACION Y ENVASADO', 'ELABORACION, FRACCIONAMIENTO Y EMPAQUE', 'ELABORACION, FRACCIONAMIENTO, ALMACENAMIENTO, CONGELADO',
                                        'ELABORACION; FRACCIONAMIENTO; EMPAQUE; ACOPIO', 'FRACCIONAMIENTO', 'OTROS', 'PROCESAMIENTO PRODUCTOS ORGANICOS', 'SECADO - DESPALILLADO - EMBOLSADO', 'SECADO; PELADO; ENVASADO; ALMACENAMIENTO', 'SIN DEFINIR', np.nan]
    for rub in lista_de_rubros_para_desambiguar:
        if type(rub) is not str:
            consultaSQL = "SELECT DISTINCT productos FROM padron WHERE rubro IS NULL"
            print("Rubro: NULL")
        else:
            consultaSQL = "SELECT DISTINCT productos FROM padron WHERE rubro = '" + rub+"'"
            print("Rubro:", rub)
        prods = sql ^ consultaSQL
        texto_prods = ""
        for index, row in prods.iterrows():
            if texto_prods != "":
                texto_prods += " ; "
            texto_prods += str(row['productos'])
        print("Productos:", texto_prods)
        print("----------------")
else:
    print("ASSERTION ERROR: existe más de una categoría para un mismo rubro, y nuestro análisis asume lo contrario.")


# Añadimos columna id_rubro a la tabla rubro_categoria_clae2, de manera
# consistente con los id_rubro que se añadieron a la tabla operadores
consultaSQL = """ 
                SELECT DISTINCT padron.id_rubro, rcc.categoria_id, clae2 
                FROM rcc, padron
                WHERE rcc.rubro = padron.rubro AND rcc.categoria_id = padron.categoria_id
            """
rcc = sql ^ consultaSQL

# Normalización

### localidades ###

consultaSQL = fn.genConsulta(['categoria', 'centroide_lat', 'centroide_lon', 'departamento_id',
                             'fuente', 'funcion', 'id', 'municipio_id', 'nombre', 'provincia_id']).format('loc')
localidades_norm = sql ^ consultaSQL

consultaSQL = fn.genConsulta(
    ['municipio_id', 'municipio_nombre']).format('loc')
municipios = sql ^ consultaSQL

consultaSQL = fn.genConsulta(
    ['departamento_id', 'departamento_nombre']).format('loc')
departamentos_loc = sql ^ consultaSQL

consultaSQL = fn.genConsulta(
    ['provincia_id', 'provincia_nombre']).format('loc')
provincias_loc = sql ^ consultaSQL

### operadores ###

consultaSQL = fn.genConsulta(['provincia_id', 'provincia']).format('padron')
provincias_padron = sql ^ consultaSQL

consultaSQL = fn.genConsulta(['pais_id', 'pais']).format('padron')
paises = sql ^ consultaSQL

consultaSQL = fn.genConsulta(['pais_id', 'provincia_id', 'departamento',
                             'localidad', 'id_razon_social_establecimiento']).format('padron')
locacion_establecimientos = sql ^ consultaSQL

consultaSQL = fn.genConsulta(
    ['certificadora_id', 'certificadora_deno']).format('padron')
certificadoras = sql ^ consultaSQL

consultaSQL = fn.genConsulta(['id_razon_social_establecimiento', 'establecimiento', 'razon_social',
                             'certificadora_id', 'categoria_id', 'productos', 'id_rubro']).format('padron')
establecimientos_datos = sql ^ consultaSQL

consultaSQL = fn.genConsulta(['id_rubro', 'categoria_id']).format('padron')
rubro_categoria = sql ^ consultaSQL

consultaSQL = fn.genConsulta(['id_rubro', 'rubro']).format('padron')
rubros = sql ^ consultaSQL

consultaSQL = fn.genConsulta(
    ['categoria_id', 'categoria_desc']).format('padron')
categorias_organicas = sql ^ consultaSQL

consultaSQL = fn.genConsulta(
    ['id_razon_social_establecimiento', 'establecimiento']).format('padron')
establecimientos = sql ^ consultaSQL

consultaSQL = fn.genConsulta(['provincia_id', 'pais']).format('padron')
prov_pais_padron = sql ^ consultaSQL

### clae ###

consultaSQL = fn.genConsulta(['clae2', 'clae2_desc', 'letra']).format('clae2')
clases = sql ^ consultaSQL

consultaSQL = fn.genConsulta(['letra', 'letra_desc']).format('clae2')
clae2_letra = sql ^ consultaSQL

### deptos ###

consultaSQL = fn.genConsulta(
    ['codigo_departamento_indec', 'nombre_departamento_indec', 'id_provincia_indec']).format('depto')
depto_prov_indec = sql ^ consultaSQL

consultaSQL = fn.genConsulta(
    ['id_provincia_indec', 'nombre_provincia_indec']).format('depto')
provincias_indec = sql ^ consultaSQL


### salarios ###

consultaSQL = fn.genConsulta(
    ['fecha', 'codigo_departamento_indec', 'clae2', 'w_median']).format('salario')
salarios_norm = sql ^ consultaSQL
# fix para hacer su tipo compatible con depto_prov_indec
salarios_norm['codigo_departamento_indec'] = salarios_norm['codigo_departamento_indec'].astype(
    'Int64')

consultaSQL = fn.genConsulta(
    ['codigo_departamento_indec', 'id_provincia_indec']).format('salario')
depto_prov_sal_norm = sql ^ consultaSQL


# Curado de datos

# Hay 3 tablas de provincias, que surgen de otras 3
# tablas originales (operadores, deptos y localidades).
# Tienen nombres de columna diferentes, y los nombres
# de las provincias son todos distintos (llevan o no acento,
# mayúscula o no, y algunos nombres varían), haciendo imposible
# traducir un matcheo con una tabla distinta a la prevista.
# A priori, resultaría erróneo usar una tabla para matchear
# por código o por texto contra tablas de origen distinto.
# El código que corresponde a CABA en tablas de operadores
# podría corresponder a otra provincia en tablas de depto, por
# ejemplo. Y el matcheo de texto por nombre de provincia
# podría fallar.
# Sin embargo, se observa que los nombres de las provincias,
# aunque diferentes, corresponden a las mismas provincias, y que
# sus códigos en las 3 tablas coinciden.
# Esta circunstancia, bastante inusual, permite que se utilice
# cualquiera de ellas para matchear sobre cualquier tabla
# derivada de las 3 tablas originales sin tener que curar
# las tablas.
#
# provincias_loc: ['provincia_id', 'provincia_nombre']
# provincias_indec: ['id_provincia_indec', 'nombre_provincia_indec']
# provincias_padron: ['provincia_id', 'provincia']
consultaSQL = """
            SELECT p1.provincia_id, p1.provincia_nombre, p2.nombre_provincia_indec, p3.provincia
            FROM provincias_loc AS p1, provincias_indec AS p2, provincias_padron AS p3
            WHERE p1.provincia_id = p2.id_provincia_indec
                AND p1.provincia_id = p3.provincia_id
            """
provincias_comparadas = sql ^ consultaSQL
#provincias_comparadas.to_csv("provincias_comparadas.csv", index=False)


# %%
# ==============================================================================
# ANÁLISIS DE DATOS Y RESPUESTAS A PREGUNTAS
# ==============================================================================

print("")
print("")
print('=========== Análisis de datos y respuestas a preguntas ===========')
print("")

# ******************************************************************************
# Pregunta (i): ¿Existen provincias que no presentan Operadores Orgánicos
# Certificados? ¿En caso de que sí, cuántas y cuáles son?
# ******************************************************************************

consultaSQL = """
                SELECT id_provincia_indec
                FROM provincias_indec
                EXCEPT 
                SELECT provincia_id
                FROM locacion_establecimientos;
              """
provSinOperadoresOrganicos = sql ^ consultaSQL

provSinOperadoresOrganicos = len(provSinOperadoresOrganicos)


print("Pregunta (i). ¿Existen provincias que no presentan Operadores Orgánicos Certificados? ¿En caso de que sí, cuántas y cuáles son?")
print("")
print("- El número de provincias sin Operadores Orgánicos Certificados es " + str(provSinOperadoresOrganicos))
print("")
print("")

# ******************************************************************************
# Pregunta (ii): ¿Existen departamentos que no presentan Operadores Orgánicos
# Certificados? ¿En caso de que sí, cuántos y cuáles son?
# ******************************************************************************
print('Atención: las siguientes queries demoran unos 5 minutos. El saber tal vez no ocupe lugar pero sí tiempo.')
print("")
# Primero unimos mediante INNER JOIN las tablas localidades_norm con municipios
consultaSQL = """
                SELECT DISTINCT loc.departamento_id,
                                loc.nombre,
                                mun.municipio_nombre,
                                loc.departamento_id,
                                loc.provincia_id
                FROM localidades_norm AS loc
                INNER JOIN municipios AS mun
                ON loc.municipio_id = mun.municipio_id;                        
                """
localidades_municipios = sql ^ consultaSQL  # 3043

# Ahora obtenemos en una tabla los diferentes nombres que pueden tomar los 
# departamentos en la base de datos pues los valores de la columna departamento
# en la fuente operadores, tiene mejor cubrimiento en la fuente localidades (60%)
# mientras en el diccionario departamentos el cubrimiento es de 40%
consultaSQL = """
                SELECT DISTINCT dpi.codigo_departamento_indec,
                                dpi.nombre_departamento_indec,
                                dep.departamento_nombre,
                                dpi.id_provincia_indec
                FROM departamentos_loc AS dep
                INNER JOIN depto_prov_indec AS dpi
                ON dep.departamento_id = dpi.codigo_departamento_indec AND
                    LOWER(dep.departamento_nombre) = LOWER(dpi.nombre_departamento_indec);
                """
deptos_loc = sql ^ consultaSQL  # 494

# En esta consulta hacemos un join donde lo valores de la columna departamento 
# de la tabla locacion_establecimientos coincide con algún valor de las columnas 
# departamento_nombre, nombre o municipio_nombre de la tabla localidades 
# municipios el porqué de esta desiscion esta relacionado con el GQM 7
consultaSQL = """
                SELECT DISTINCT dl.codigo_departamento_indec,
                                dl.nombre_departamento_indec
                FROM locacion_establecimientos AS le,
                    deptos_loc AS dl,
                    localidades_municipios AS lm
                WHERE LOWER(le.departamento) = LOWER(dl.departamento_nombre) OR
                LOWER(le.departamento) = LOWER(lm.nombre) OR
                LOWER(le.departamento) = LOWER(lm.municipio_nombre) AND
                UPPER(le.departamento) NOT IN ('INDEFINIDO', 'INDEFINIDA', 'NC', 'SIN DEFINIR') AND
                le.departamento IS NOT NULL;
"""
deptos_con_operadores = sql ^ consultaSQL  # 494


# Ahora que tenemos la tabla de departamentos con operadores, el siguiente paso 
# es restar estos departmaentos del diccionario de departamentos
consultaSQL = """
                SELECT DISTINCT *
                FROM (
                    SELECT DISTINCT codigo_departamento_indec,
                                    nombre_departamento_indec
                    FROM depto_prov_indec
                    ) AS cod_dep
                EXCEPT 
                SELECT DISTINCT *
                FROM deptos_con_operadores;
                """
deptos_sin_operadores = sql ^ consultaSQL  # 16


consultaSQL = """
                SELECT COUNT(*) AS cant_deptos
                FROM deptos_sin_operadores;
"""
cantidad_deptos_sin_operadores = sql ^ consultaSQL


print("")
print("Pregunta (ii). ¿Existen departamentos que no presentan Operadores Orgánicos Certificados? ¿En caso de que sí, cuántos y cuáles son?")
print("")
print("- Los departamentos sin operadores orgánicos son:")
for i in range(len(deptos_sin_operadores)):
    print(deptos_sin_operadores.loc[i]["nombre_departamento_indec"])
print("")
print("- La cantidad de departamentos sin operadores orgánicos es " +
      str(cantidad_deptos_sin_operadores.iat[0, 0]))
print("")
print("")

# ******************************************************************************
# Pregunta (iii): ¿Cuál es la actividad que más operadores tiene?
# ******************************************************************************

# Obtenemos la cantidad de operadores por cada clae2

consultaSQL = """
                SELECT c.clae2_desc, COUNT(rcc.clae2) AS cant_clae2, rcc.clae2 
                FROM establecimientos_datos AS ed,
                     rcc,
                     clases AS c
                WHERE ed.id_rubro = rcc.id_rubro AND c.clae2 = rcc.clae2
                GROUP BY c.clae2_desc,
                         rcc.clae2;
            """
cant_operadores_por_actividad = sql ^ consultaSQL
# Obtenemos su valor máximo

consultaSQL = """
                SELECT clae2_desc AS Actividad,
                       cant_clae2 AS cant_operadores,
                       clae2
                FROM cant_operadores_por_actividad
                WHERE cant_clae2 >= ALL(
                                        SELECT DISTINCT cant_clae2
                                        FROM cant_operadores_por_actividad
                                        );
            """
consulta = sql ^ consultaSQL


print("")
print("Pregunta (iii). ¿Cuál es la actividad que más operadores tiene?")
print("")
print('- {} es la actividad con mayor numero de operadores (clae2 {}), con un total de {} operadores.'.format(
    consulta.iat[0, 0], consulta.iat[0, 2], consulta.iat[0, 1]))
print("")
print("")

# ******************************************************************************
# Pregunta (iv): ¿Cuál fue el salario promedio de esa actividad en 2022? (si hay
# varios registros de salario, mostrar el más actual de ese año)
# ******************************************************************************

# Primero seleccionaremos todos los salarios correspondientes a la clae2 1 y al
# año 2022. Ordenamos los resultados por fecha.

consultaSQL = """ 
                SELECT fecha, w_median
                FROM salarios_norm
                WHERE fecha LIKE '2022-%' AND clae2 = 1 AND w_median > 0
                ORDER BY fecha DESC
            """
consulta = sql ^ consultaSQL

# De todos esos salarios promedios, nos quedamos con el primero, puesto que este
# corresponde a la fecha más actual del 2022

salario_promedio_clae2_1_2022 = consulta.at[0, 'w_median']


print("")
print("Pregunta (iv). ¿Cuál fue el salario promedio de esa actividad en 2022? (si hay varios registros de salario, mostrar el más actual de ese año)")
print("")
print("- El salario promedio de la actividad con más Operadores Orgánicos Certificados (clae2 1) en 2022 fue de: " +
      str(salario_promedio_clae2_1_2022) + "$")
print("")
print("")

# ******************************************************************************
# Pregunta (v): ¿Cuál es el promedio anual de los salarios en Argentina y cual
# es su desvío?, ¿Y a nivel provincial? ¿Se les ocurre una forma de que sean
# comparables a lo largo de los años? ¿Necesitarían utilizar alguna fuente de
# datos externa secundaria? ¿Cuál?
# ******************************************************************************

# Comenzamos viendo el promedio anual de salarios en Argentina.
# Para ello primero seleccionamos todos aquellos registros de la tabla
# salarios_norm, correspondientes al año 2022 y nos quedamos solo con aquellos
# salarios positivos.


consultaSQL = """ 
                SELECT DISTINCT *
                FROM salarios_norm 
                WHERE fecha LIKE '2022-%' AND w_median > 0;
            """
salarios_2022 = sql ^ consultaSQL

# Ahora calculamos el promedio de salarios, para cada clae2, en 2022

consultaSQL = """
                SELECT DISTINCT clae2, AVG(w_median) AS salario_promedio_por_clae2
                FROM salarios_2022 
                GROUP BY clae2;
            """
salarios_prom_por_clae2 = sql ^ consultaSQL

# Ahora calculamos el promedio de todos esos promedios y su desviación estándar

consultaSQL = """ 
                SELECT DISTINCT AVG(salario_promedio_por_clae2) AS salario_promedio, STDDEV(salario_promedio_por_clae2) AS salarios_desviacion_estandar
                FROM salarios_prom_por_clae2;
            """
promedio_salarios = sql ^ consultaSQL


print("")
print("Pregunta (v). ¿Cuál es el promedio anual de los salarios en Argentina y cuál es su desvío?, ¿y a nivel provincial? ¿se les ocurre una forma de que sean comparables a lo largo de los años? ¿necesitarían utilizar alguna fuente de datos externa secundaria? ¿cuál?")
print("")
print('- El salario promedio anual en Argentina en 2022 fue de: {}$. \n- Su desviación estándar fue de {}$.' .format(
    int(promedio_salarios.iat[0, 0]), int(promedio_salarios.iat[0, 1])))

# Calculamos ahora el promedio anual de salarios por provincia
# Para ello primero combinamos la tabla salarios_2022 con la tabla provincias y
# y la tabla depto_prov_indec, para recuperar los nombres de las provincias

consultaSQL = """ 
                SELECT DISTINCT fecha, clae2, w_median, prov.nombre_provincia_indec
                FROM salarios_2022 AS salarios, provincias_indec AS prov, depto_prov_indec AS dp
                WHERE salarios.codigo_departamento_indec = dp.codigo_departamento_indec AND dp.id_provincia_indec = prov.id_provincia_indec;
            """

salarios_2022_prov = sql ^ consultaSQL

# Ahora calculamos, para cada provincia, el salario promedio anual para cada
# clae2

consultaSQL = """ 
                SELECT DISTINCT nombre_provincia_indec, clae2, AVG(w_median) AS salario_promedio_por_prov
                FROM salarios_2022_prov
                GROUP BY nombre_provincia_indec, clae2; 
            """
salarios_prom_por_prov_y_clae2 = sql ^ consultaSQL

# Finalmente, calculamos el promedio de esos promedios, para cada provincia

consultaSQL = """ 
                SELECT DISTINCT nombre_provincia_indec, AVG(salario_promedio_por_prov) AS salario_promedio, STDDEV(salario_promedio_por_prov) AS desvio_estandar
                FROM salarios_prom_por_prov_y_clae2
                GROUP BY nombre_provincia_indec
            """
salario_promedio_y_sd = sql ^ consultaSQL

# Redondeamos los valores calculados

salario_promedio_y_sd['salario_promedio'] = salario_promedio_y_sd['salario_promedio'].astype(
    int)
salario_promedio_y_sd['desvio_estandar'] = salario_promedio_y_sd['desvio_estandar'].astype(
    int)

# Ya podemos imprimir la respuesta

print("- Salario promedio para cada provincia argentina en el año 2022, y sus correspondientes desviaciones estándar: \n" +
      salario_promedio_y_sd[['nombre_provincia_indec', 'salario_promedio', 'desvio_estandar']].to_string(index=False))
print("")


# %%
# ==============================================================================
# ANÁLISIS DE DATOS - VISUALIZACIÓN
# ==============================================================================


# ******************************************************************************
# Consigna (i): Cantidad de Operadores por provincia.
# ******************************************************************************

# Primero combinamos las tablas establecimientos_datos,
# locacion_establecimientos y provincias_indec para recuperar la información
# de a qué provincia pertenece cada Operador.

consultaSQL = """
                SELECT DISTINCT ed.id_razon_social_establecimiento AS establecimiento, nombre_provincia_indec AS provincia
                FROM establecimientos_datos AS ed, locacion_establecimientos AS le, provincias_indec AS prov
                WHERE ed.id_razon_social_establecimiento = le.id_razon_social_establecimiento AND le.provincia_id = prov.id_provincia_indec;
            """
operadores_prov = sql ^ consultaSQL

# Ahora contabilizamos cuántos Operadores hay para cada provincia

consultaSQL = """
                SELECT DISTINCT provincia, COUNT(*) AS cantidad_operadores
                FROM operadores_prov
                GROUP BY provincia;
            """
cant_operadores_por_prov = sql ^ consultaSQL

# Guardamos esos valores

provincias = cant_operadores_por_prov['provincia']
cant_operadores = cant_operadores_por_prov['cantidad_operadores']

# Los ordenamos

indices_ordenados = np.argsort(cant_operadores)
provincias_ordenadas = [provincias[i] for i in indices_ordenados]
cant_operadores_ordenados = [cant_operadores[i] for i in indices_ordenados]

# Y ahora sí, ya podemos graficar

plt.bar(provincias_ordenadas, cant_operadores_ordenados)
plt.xlabel('Provincia')
plt.ylabel('Cantidad de Operadores')
plt.title('Cantidad de Operadores por provincia')
plt.xticks(rotation='vertical')
plt.show()
plt.close()

# ******************************************************************************
# Consigna (ii): Boxplot, por cada provincia, donde se pueda observar la
# cantidad de productos por operador.
# ******************************************************************************

# Primero tomamos la tabla establecimientos_datos y le agregamos una nueva
# columna informe, para cada establecimiento, cuántos productos se producen ahí

# Convertimos valores de la columna 'productos' a cadenas de texto ¡¡¡¡REVISAR,
# se supone que no habría que hacer esto!!!
establecimientos_datos['productos'] = establecimientos_datos['productos'].astype(
    str)

establecimientos_datos['cant_de_prod'] = establecimientos_datos['productos'].apply(
    fn.cant_trozos_texto_separados_por_comas_e_yes)

# Ahora combinamos esta tabla con la tabla provincias_indec

consultaSQL = """
                SELECT DISTINCT establ.id_razon_social_establecimiento AS establecimiento, nombre_provincia_indec AS provincia, cant_de_prod
                FROM provincias_indec AS prov, establecimientos_datos AS establ, locacion_establecimientos AS loc
                WHERE establ.id_razon_social_establecimiento = loc.id_razon_social_establecimiento AND loc.provincia_id = prov.id_provincia_indec;
            """
establ_prov_cant_prod = sql ^ consultaSQL

# Ya podemos graficar el boxplot

sns.boxplot(data=establ_prov_cant_prod, x='provincia', y='cant_de_prod')
plt.xlabel('Provincia')
plt.ylabel('Cantidad de productos por Operador')
plt.title('Cantidad de productos por operador, para cada provincia')
plt.xticks(rotation='vertical')
plt.show()
plt.close()


# ******************************************************************************
# Consigna (iii): Relación entre cantidad de emprendimientos certificados de
# cada provincia y el salario promedio en dicha provincia (para la actividad)
# en el año 2022. En caso de existir más de un salario promedio para ese año,
# mostrar el último del año 2022.
# ******************************************************************************

# Analizaremos la relación entre cantidad de Operadores y salario promedio,
# para cada provincia, para las clae2 que nos interesan. Para ello haremos un
# scatterplot, donde el eje x representará cantidad de Operadores, el eje y
# representará el salario promedio y colorearemos con diferentes colores
# según la provincia.

# Interpretamos "emprendimientos" como sinónimo de establecimientos.
# Los "emprendimientos certificados" son los que figuran en el frame padron,
# ya que todos los establecimientos tienen certificadora:
consultaSQL = """
            SELECT DISTINCT certificadora_id
            FROM padron
            WHERE certificadora_id IS NULL
        """
if len(sql ^ consultaSQL) > 0:
    print("ASSERTION ERROR: hemos asumido que todo establecimiento tiene certificadora, pero los datos actuales muestran que no es así.")

# Por cada clae2 que nos interesa, calculamos y graficamos:
lista_de_claes_de_operadores = rcc['clae2'].drop_duplicates()
for clae_buscado in lista_de_claes_de_operadores:
    ## "el salario promedio en dicha provincia (para la actividad) en el año 2022"
    # Todos los promedios de w_median por cada provincia, para el clae dado
    # (descarto los w_median negativos por ser inválidos, y solo tomo valores
    # de w_median correspondientes a diciembre de 2022).
    consultaSQL = """
                SELECT p.id_provincia_indec, p.nombre_provincia_indec, AVG(w_median) AS salario_promedio_provincial
                FROM salarios_norm AS s, depto_prov_sal_norm AS d, provincias_indec AS p
                WHERE fecha = '2022-12-01'
                    AND s.w_median > 0               
                    AND s.clae2 = $clae_buscado
                    AND s.codigo_departamento_indec = d.codigo_departamento_indec
                    AND d.id_provincia_indec = p.id_provincia_indec
                GROUP BY p.id_provincia_indec, p.nombre_provincia_indec
                ORDER BY salario_promedio_provincial
            """
    prov_clae_w_median_avg = sql ^ consultaSQL
    # Agrupados por provincia y para una clae dada, hay establecimientos
    # en las siguientes cantidades:
    consultaSQL = """
                    SELECT l.provincia_id, COUNT(l.id_razon_social_establecimiento) AS cantidad_establecimientos
                    FROM rcc, establecimientos_datos AS e, locacion_establecimientos AS l
                    WHERE e.id_razon_social_establecimiento = l.id_razon_social_establecimiento
                        AND rcc.id_rubro = e.id_rubro
                        AND rcc.clae2 = $clae_buscado
                    GROUP BY l.provincia_id
                """
    cant_op_clae_por_prov = sql ^ consultaSQL
    # Cruzo lo obtenido en ambos dataframes:
    consultaSQL = """
                SELECT DISTINCT p.nombre_provincia_indec AS provincia, c.cantidad_establecimientos, p.salario_promedio_provincial
                FROM cant_op_clae_por_prov AS c, prov_clae_w_median_avg AS p
                WHERE c.provincia_id = p.id_provincia_indec
                """
    prov_cant_op_y_salario_prom = sql ^ consultaSQL

    # Ya tenemos todo lo necesario para poder hacer el gráfico.
    sns.scatterplot(data=prov_cant_op_y_salario_prom, x='cantidad_establecimientos',
                    y='salario_promedio_provincial', hue='provincia',
                    palette='bright')
    plt.xlabel('Cantidad de operadores')
    plt.ylabel('Salario promedio provincial ($)')
    plt.title('Relación entre cantidad de operadores y salario promedio, para cada provincia, para la actividad clae2 ' + str(clae_buscado))
    plt.legend(bbox_to_anchor=(1, 1), loc='upper left')
    plt.show()
    plt.close()


# ******************************************************************************
# Consigna (iv): ¿Cuál es la distribución de los salarios promedio en Argentina?
# Realicen un violinplot de los salarios promedio por provincia. Grafiquen el
# último ingreso medio por provincia.
# ******************************************************************************

# Primero creamos una tabla con las columnas provincia y w_median (salario
# medio) quedándonos solo con los registros de la fecha 22-12-01 y de salario
# positivo. Esta tabla será similar a prov_cant_op_w_median pero no filtraremos
# solo aquellos cuya actividad sea la clae2 1, en esta tabla consideraremos
# todas las claes.

consultaSQL = """
                SELECT DISTINCT provincia, cantidad_operadores, w_median
                FROM cant_operadores_por_prov, provincias_indec, depto_prov_indec, salarios_norm
                WHERE cant_operadores_por_prov.provincia = provincias_indec.nombre_provincia_indec AND provincias_indec.id_provincia_indec = depto_prov_indec.id_provincia_indec AND depto_prov_indec.codigo_departamento_indec = salarios_norm.codigo_departamento_indec AND fecha = '2022-12-01' AND w_median > 0
            """
prov_salarios_prom = sql ^ consultaSQL

# Ya podemos graficar

sns.violinplot(data=prov_salarios_prom, x='provincia', y='w_median')
plt.xlabel('Provincia')
plt.ylabel('Salario promedio ($)')
plt.title('Salario promedio para cada provincia en diciembre de 2022')
plt.xticks(rotation='vertical')
plt.show()
plt.close()


# ******************************************************************************
#  Resultado final
# ******************************************************************************

############
# Nota: estas queries utilizan fuertemente la propiedad de que
# los códigos de provincia en las distintas tablas son iguales.
# Ver sección Curado de datos en este archivo para más detalles.
###

# Promedio de las medianas de salario relevadas por departamento,
# diciembre de 2022. Estima la media del salario por departamento.
consultaSQL = """
            SELECT codigo_departamento_indec, AVG(w_median) AS prom_salario
            FROM salarios_norm
            WHERE fecha = '2022-12-01'
                AND w_median > 0
            GROUP BY codigo_departamento_indec
            """
sal_prom_por_depto = sql ^ consultaSQL
# Cantidad de establecimientos por cada par (departamento, provincia) existente
# en la tabla locacion_establecimientos, que proviene de la normalización
# de la tabla del padrón de operadores orgánicos. Así, este dataframe
# permite estimar el "desarrollo de la actividad (orgánica)" en cada ubicación
# dada por dicho par.
consultaSQL = """
            SELECT departamento, provincia_id, COUNT(id_razon_social_establecimiento) AS cant_establecimientos
            FROM locacion_establecimientos
            GROUP BY departamento, provincia_id
            """
cant_est_por_depto = sql ^ consultaSQL
# Para poder compararlo, lo pasamos a lowercase.
cant_est_por_depto['departamento'] = cant_est_por_depto['departamento'].str.lower()

# Vamos a necesitar saber en qué provincia está el departamento del
# cual evaluamos su salario. Para ello, cruzamos la tabla con
# DEPTO_PROV_INDEC (que actuará como tabla auxiliar).
depto_prov_indec_lower = depto_prov_indec.copy()
# Para poder compararlo, lo pasamos a lowercase.
depto_prov_indec_lower['nombre_departamento_indec'] = depto_prov_indec_lower['nombre_departamento_indec'].str.lower()

# provincias_loc: ['provincia_id', 'provincia_nombre']
# provincias_indec: ['id_provincia_indec', 'nombre_provincia_indec']
# provincias_padron: ['provincia_id', 'provincia']

# hacemos un join de las dos tablas obtenidas, junto con la recién
# mencionada, más la tabla provincias_padron.
# Usaremos esta tabla para matchear la tabla auxiliar, y así saber
# el nombre de la provincia que corresponde a cada registro.
# Así obtenemos una estimación de los salarios medios de cada departamento,
# conocemos la provincia de ese departamento, y tenemos la cantidad de
# establecimientos en ese departamento de esa misma provincia.
# Nota: debido a que muchos departamentos no tienen w_median reportado
# o porque su nombre en la tabla auxiliar de la primera varía respecto
# del nombre en la segunda tabla, se desecha una gran cantidad de datos
# que no podemos interpretar. Pero al menos obtenemos más de 200 datos
# que podemos vincular, resultando en 117 pares de datos.
consultaSQL = """
            SELECT s.prom_salario, c.cant_establecimientos, p.provincia
            FROM sal_prom_por_depto AS s, cant_est_por_depto AS c, depto_prov_indec_lower AS d, provincias_padron AS p
            WHERE s.codigo_departamento_indec = d.codigo_departamento_indec
                AND d.nombre_departamento_indec = c.departamento
                AND p.provincia_id = d.id_provincia_indec
                AND p.provincia_id = c.provincia_id
                --========= Opciones =============
                --== Quitamos outliers altos para que se vea tendencia, 
                --== si la hay, en las cantidades más bajas:
                --AND c.cant_establecimientos < 35
                --== Quitamos los valores en el rango inferior
                --== porque las cantidades pequeñas pueden no tener un
                --== efecto visible sobre el salario y la mayor cantidad de
                --== casos en este rango confunde la señal de tendencia
                --== que podemos, tal vez, percibir cuando no están.
                --== Dejamos, eso sí, los valores mínimos para ver qué
                --== rango de valores toman esos casos que descartamos.
                --AND (c.cant_establecimientos > 6
                --       OR c.cant_establecimientos <= 1)
                --== Podemos analizar por provincia específica
                --== descomentando esta línea:
                --AND p.provincia <> 'NEUQUEN'
            """
titulo_extra = ""
#titulo_extra="\n (Excluyendo la provincia de Neuquén)"
salario_vs_cant_estab = sql ^ consultaSQL
sns.scatterplot(data=salario_vs_cant_estab, x='cant_establecimientos',
                y='prom_salario', hue='provincia',
                palette='bright')
plt.xlabel('Desarrollo act. orgánicos (cant. de operadores)')
plt.ylabel('Salario promedio por departamento ($)')
plt.title('Relación entre cantidad de operadores y salario promedio, para cada departamento del país.'+titulo_extra)
plt.legend(bbox_to_anchor=(1, 1), loc='upper left')
plt.show()
plt.close()

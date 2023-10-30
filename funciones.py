#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
  Archivo: funciones.py
  Proyecto para Laboratorio de datos, Facultad de Ciencias Exactas y Naturales, UBA
  Integrantes del grupo: Ariel Dembling, Antony Suárez Caina, Camila Guibaudo 
  Fecha: 27/06/23
  Descripción: aquí están los códigos correspondientes a todas las funciones 
  utilizadas en desarrollo.py
"""

# =============================================================================
# LIBRERÍAS
# =============================================================================
import pandas as pd
import numpy as np
from inline_sql import sql, sql_val

# =============================================================================
# FUNCIONES PARA EXPLORACIÓN DE DATOS
# =============================================================================

### El código resulta más legible abstrayendo la construcción,
### proyección y selección de los datos, siendo así menos dependiente 
### de las idiosincracias sintácticas de Python. Se podría mejorar aun
### más implementando una clase, pero con esto alcanza para nuestros fines.

###### Constructor
def crearDictdf(df, name, lista_de_columnas=[]):
    if lista_de_columnas == []:
        lista_de_columnas = list(df.columns)
    return {"df": pd.DataFrame(df[lista_de_columnas]), "name": name}
    

###### Proyectores y selectores
### Proyector y selector general
def getTuplas(dict_df_o_df, lista_atrib_proyeccion=[], filtro_seleccion=[]):
    """
    Operación de selección y proyección sobre el dataframe.
    
    Argumentos:
        dict_df_o_df = metadata correspondiente al dataframe, o bien
            el dataframe mismo.
        lista_atrib_proyeccion = lista de atributos a proyectar.
            También puede ser un string si se trata de un único atributo.
            Si es vacía, se proyectan todos los atributos.
        filtro = Pandas Series a utilizar como filtro sobre el 
            dataframe, condicionando las tuplas que resultan
            seleccionadas. Debe contener tantas tuplas como el
            dataframe. Si es una lista vacía, se ignora la 
            condición (se seleccionan todas las tuplas del
            dataframe)
    """
    if isinstance(dict_df_o_df, dict):
        df = dict_df_o_df['df']
    elif type(dict_df_o_df) is pd.core.series.Series or type(dict_df_o_df) is pd.core.frame.DataFrame:
        df = dict_df_o_df
    else:
        print("ERROR: tipo erróneo en dict_df_o_df")
        return []
    if lista_atrib_proyeccion==[]:
        lista_atrib_proyeccion = list(df.columns)
    if type(filtro_seleccion) == pd.core.series.Series:
        return pd.DataFrame(df[filtro_seleccion][lista_atrib_proyeccion])
    else:
        return pd.DataFrame(df[lista_atrib_proyeccion])


### Estos selectores particionan el dataframe en dos partes:
### las tuplas desechables ("repetidas") y las valiosas ("únicas").
### Ambos devuelven todas las columnas del dataframe provisto
### (es decir, no hacen proyección).
### Cuando las columnas de la lista provista resultan en 
### más de una tupla existente en el dataframe, la primera
### encontrada se asume como valiosa y las demás como desechables. 
def selTuplasUnicas(dict_df, lista_de_columnas=[], filtro=[]):
    if lista_de_columnas == []:
        lista_de_columnas = list(dict_df['df'].columns)
    tuplas_unicas = getTuplas(dict_df,[],filtro).drop_duplicates(subset=lista_de_columnas, keep='first')
    return tuplas_unicas

def selTuplasRepetidasSobrantes(dict_df, lista_de_columnas=[]):
    if lista_de_columnas == []:
        lista_de_columnas = list(dict_df['df'].columns)
    # Aquí df.duplicated() da un filtro que pone False en las tuplas que no 
    # están duplicadas. Con el parámetro keep='first', a la primera
    # copia de las que sí están duplicadas también le pone False.
    # El parámetro subset especifica sobre cuáles columnas se realiza el
    # chequeo de duplicación. El filtro es aplicado sobre el dataframe
    # original, devolviendo solo aquellas tuplas a las cuales el filtro
    # les pone True, que son las copias sobrantes.
    filtro=dict_df['df'].duplicated(subset=lista_de_columnas, keep='first')
    tuplas_sobrantes = getTuplas(dict_df, [], filtro)
    return tuplas_sobrantes

### Propiedades:

def columnas(dict_df_o_df):
    if isinstance(dict_df_o_df, dict):
        df = dict_df_o_df['df']
    elif type(dict_df_o_df) is pd.core.series.Series or type(dict_df_o_df) is pd.core.frame.DataFrame:
        df = dict_df_o_df
    else:
        print("ERROR: tipo erróneo en dict_df_o_df")
        return []
    return list(df.columns)

def cantidadDeTuplasUnicas(dict_df, lista_de_columnas):
    return len(selTuplasUnicas(dict_df, lista_de_columnas))

def conjuntoDeValoresDeColumna(dict_df, col, verbose=True):
    res = set(selTuplasUnicas(dict_df, col)[col])
    if verbose:
        print("Valores de la columna " + col + " del dataframe " + dict_df['name'] + ": " + str(res))
    return res


# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

# Retorna una cadena con una consulta simple
def genConsulta(lista_de_columnas=[]):
    p = 'SELECT DISTINCT '
    if lista_de_columnas == []:
        p+='* '
    else:
        for col in lista_de_columnas:
            p += col+', '
        p= p[:-2]
    p +=' FROM {};'
    return p    
        

# Retorna una consulta en dos tablas con nombre asignado df1 y df2
def genConsultaExt(dict_columnas, tipos_df1, lista_de_indefinidos=[]):    
    consultaSQL ='SELECT DISTINCT '
    aux_consulta_or = ''
    aux_consulta_and = ''
    clave = ''
    for k in dict_columnas:
        if len(clave)==0:
            clave = k
        if isinstance(k, str) :
            consultaSQL+='df1.'+k+', '
            aux_consulta_and += 'df1.'+k+' IS NOT NULL AND '
            if tipos_df1[k][1] is str:
                for col in dict_columnas[k]:
                    aux_consulta_or+='LOWER(df1.'+k+') = LOWER(df2.'+col+') OR '
                if len(lista_de_indefinidos) > 0:
                    aux_consulta_and += 'df1.'+k+' NOT IN '+str(tuple(lista_de_indefinidos))+' AND '                
            else:
                for col in dict_columnas[k]:
                    aux_consulta_or +='df1.'+k+' = '+'df2.'+col+' OR '            
        elif isinstance(k,tuple):
            for columna in k:
                consultaSQL+='df1.'+columna+', '
                aux_consulta_and += 'df1.'+columna+' IS NOT NULL AND '
                if tipos_df1[columna][1] is str:
                    for col in dict_columnas[k]:
                        aux_consulta_or+='LOWER(df1.'+columna+') = LOWER(df2.'+col+') OR '
                    if len(lista_de_indefinidos)> 0:
                        aux_consulta_and += 'df1.'+columna+' NOT IN '+str(tuple(lista_de_indefinidos))+' AND '
                else:
                    for col in dict_columnas[k]:
                        aux_consulta_or +='df1.'+columna+' = '+'df2.'+col+' OR '
        if aux_consulta_or[-3:] == 'OR ':
            aux_consulta_or = aux_consulta_or[:-3]
        aux_consulta_or+='AND '        
            
    consultaSQL = consultaSQL[:-2] +' FROM df1, df2 WHERE '+ aux_consulta_or + aux_consulta_and[:-5] +';'
                            
    return (consultaSQL, clave)


# =============================================================================
# FUNCIONES PARA LA EXPLORACIÓN DE DATOS EN TABLAS Y ANÁLISIS
# =============================================================================

# FUNCIONES DE METRICAS

def porcentajeTuplasRepetidasSobrantes(dict_df, lista_de_columnas=[], verbose=True):
    if lista_de_columnas == []:
        lista_de_columnas = list(dict_df['df'].columns)
        cols_text = "en el "
    else:
        cols_text = "en las columnas " + str(lista_de_columnas) + " del "
    # Aquí df.duplicated() da un filtro que pone False en las tuplas que no 
    # están duplicadas. Con el parámetro keep='first', a la primera
    # copia de las que sí están duplicadas también le pone False.
    # El parámetro subset especifica sobre cuáles columnas se realiza el
    # chequeo de duplicación. El filtro es aplicado sobre el dataframe
    # original, devolviendo solo aquellas tuplas a las cuales el filtro
    # les pone True, que son las copias sobrantes.
    tuplas_sobrantes = selTuplasRepetidasSobrantes(dict_df, lista_de_columnas)
    cant_tuplas_sobrantes = len(tuplas_sobrantes)
    metrica = 100 * cant_tuplas_sobrantes / len(dict_df['df'])
    if verbose:
        print("Del total de tuplas existentes " + cols_text + "dataframe "+ dict_df['name'] + ", el " + str(round(metrica,2)) + " % son tuplas repetidas.")
    return metrica



def porcentajeDeTuplasConValoresNullAny(dict_df, lista_de_columnas=[], verbose=True):
    df = dict_df['df']
    if len(lista_de_columnas) == 0:
        lista_de_columnas = df.columns
    consultaSQL = ""
    col_SQL_spec = ""
    for col in lista_de_columnas:
        if consultaSQL == "":
            consultaSQL +=  " " + col + " IS NULL"
            col_SQL_spec += col
        else:
            consultaSQL += " OR " + col + " IS NULL"
            col_SQL_spec += ", " + col
    consultaSQL = "SELECT " + col_SQL_spec + " FROM df WHERE" + consultaSQL
    consulta = sql^ consultaSQL
    metrica =  100 * len(consulta)/len(df)
    if verbose:
        print("El porcentaje de valores nulos en alguna(s) de la(s) columna(s) del dataframe " + dict_df['name'] + " es " + str(round(metrica,2)) + " %.")
    return metrica


def porcentajeDeTuplasConValoresNullAll(dict_df, lista_de_columnas=[], verbose=True):
    df = dict_df['df']
    if len(lista_de_columnas) == 0:
        lista_de_columnas = df.columns
    consultaSQL = ""
    col_SQL_spec = ""
    for col in lista_de_columnas:
        if consultaSQL == "":
            consultaSQL +=  " " + col + " IS NULL"
            col_SQL_spec += col
        else:
            consultaSQL += " AND " + col + " IS NULL"
            col_SQL_spec += ", " + col
    consultaSQL = "SELECT " + col_SQL_spec + " FROM df WHERE" + consultaSQL
    consulta = sql^ consultaSQL
    metrica =  100 * len(consulta)/len(df)
    if verbose:
        print("El porcentaje de valores nulos en todas la(s) columna(s) del dataframe " + dict_df['name'] + " es " + str(round(metrica,2)) + " %.")
    return metrica


def porcentajeDeValoresIndefinidos(dict_df, col, lista_de_indefinidos, verbose=True):
    df = dict_df['df']
    seleccionados = df[col].dropna().isin(lista_de_indefinidos)
    metrica = 100 * seleccionados.sum()/len(df)
    if verbose:
        print("El porcentaje de valores indefinidos en la columna " + col + " de la tabla " + dict_df['name'] + " es " + str(round(metrica,2)) + " %.")
    return metrica


def diferenciaDeCantidadesDeValoresUnicosUnsigned(dict_df, col1, col2, verbose=True):
    df = dict_df['df']
    contar_col1 = cantidadDeTuplasUnicas(dict_df, col1)
    contar_col2 = cantidadDeTuplasUnicas(dict_df, col2)
    res = abs(contar_col1 - contar_col2)
    if verbose:
        print('Valor absoluto de la diferencia de cantidad de valores únicos de las columnas ' + col1 + ' y ' + col2 + ' del dataframe ' + dict_df['name'] + ': '+ str(res))
    return res

def evaluacionDeTiposPorColumna(dict_df, columna, verbose=True):
    df = dict_df['df']
    d = dict()
    if type(df[columna]) is pd.core.series.Series or type(df[columna]) is pd.core.frame.DataFrame:
        tam = df[columna].size
    else:
        tam = len(df[columna])
    if tam > 0:
        for val in df[columna]:
            if not pd.isna(val):
                if type(val) in d.keys():
                    d[type(val)] += 1
                else:
                    d[type(val)] = 1

        maximo_valor = max(d.values())
        clave_maximo_valor = [clave for clave,
                              valor in d.items() if valor == maximo_valor][0]
        metrica = round(maximo_valor/tam*100, 2)
        return (metrica, clave_maximo_valor)
    else:
        return (np.nan, np.nan)


def proporcionDeTipoMayoritarioPorColumna(dict_df, columna):
    metrica, tipo = evaluacionDeTiposPorColumna(dict_df, columna)
    print("El porcentaje de valores del tipo mayoritario " + dict_df['name'] + " en la columna " + columna + " es " + str(metrica) + '%')
    return metrica


# --
## OUTPUT:
# dicc_columnas: 
#    'columna_i' = (metrica_columna_i, tipo_dominante_columna_i), para 0 < i <= dict_df['df'].columns.size
# donde tipo_dominante_columna_i corresponde al tipo de dato con mayor cantidad de apariciones en una columna dada
def evaluacionDeTipos(dict_df, lista_de_columnas=[], verbose=True):
    if verbose:
        print('')
        print('Evaluación de tipos para la tabla '+dict_df['name'])
        print('-----------------------------------------------------------')
    df = dict_df['df']
    if lista_de_columnas == []:
        lista_de_columnas = df.keys()
    dicc_columnas = dict()
    for columna in lista_de_columnas:
        dicc_columnas[columna] = evaluacionDeTiposPorColumna(dict_df, columna, verbose)
    return dicc_columnas


def consistencia(dicc_df1, dicc_df2, atributos_base=[], atributos_comp=[], caso=0, verbose=True):

    if len(atributos_base) == 2 and len(atributos_comp) == 2:
        if verbose:
            print('Base :'+dicc_df1['name']+', a comparar: '+dicc_df2['name'])
            print('-------------------------------------------------------------------')
        dfB = dicc_df1['df']
        dfC = dicc_df2['df']

        if caso == 1:
            if verbose:
                print(atributos_base[0] + ' == ' + atributos_comp[0] +
                      ' Entonces --> ' + atributos_base[1] + ' == ' + atributos_comp[1])
            consultaSQL = 'SELECT COUNT(t.{}) FROM dfC AS u INNER JOIN dfB AS t ON t.{} = u.{} WHERE t.{} <> u.{};'.format(
                atributos_base[0], atributos_base[0], atributos_comp[0], atributos_base[1], atributos_comp[1])
        elif caso == 0:
            if verbose:
                print('pertenencia de '+atributos_comp[0]+' en ' +
                      atributos_comp[1]+' de la tabla '+dicc_df2['name'])
            consultaSQL = 'SELECT DISTINCT t.{} FROM dfB AS t INNER JOIN dfC  AS u ON t.{} = u.{} AND t.{} <> u.{};'.format(
                atributos_base[0], atributos_base[0], atributos_comp[0], atributos_base[1], atributos_comp[1])

        consulta = sql ^ consultaSQL
        metrica = round(consulta.shape[0]/dfC.shape[0]*100, 2)
        if verbose:
            print('la columna {} tiene {}% de valores inconsitentes'.format(
                atributos_comp[caso], metrica))
        return metrica
    else:
        print('ERROR')
        return -1

## ESTRUCUTURA:
# dicc_columnas: 
#    'col_id' = (columna_id_df1, columna_id_df2)
#    'col_dep' = (lista_de_columnas_df1, lista_de_columnas_df2) 
# los elementos de las listas de columnas están relacionados así:
# para todo e : lista_de_columnasdf1, e' : lista_de_columnasdf2 --> e es columna análoga de e', donde la analogía está dada por la semántica de los dfs    
def consistenciaExtendida(dicc_df1, dicc_df2, dicc_columnas, epsilon = 0.05, verbose=True):
    # Comprobamos pertenencia de columnas a sus respectivos df
    if {dicc_columnas['col_id'][0]} | set(dicc_columnas['col_dep'][0]) <= set(dicc_df1['df'].columns) and {dicc_columnas['col_id'][1]} | set(dicc_columnas['col_dep'][1]) <= set(dicc_df2['df'].columns):
        
        nulls_df1 = dicc_df1['df'][dicc_columnas['col_id'][0]].isnull().sum()
        nulls_df2 = dicc_df2['df'][dicc_columnas['col_id'][1]].isnull().sum()
        size_df1 = dicc_df1['df'][dicc_columnas['col_id'][0]].shape[0]
        size_df2 = dicc_df2['df'][dicc_columnas['col_id'][1]].shape[0]
        
        if nulls_df1/size_df1 < epsilon and nulls_df2/size_df2 < epsilon:
            # definimos variables
            d_id = dict()
            long = len(dicc_columnas['col_dep'][0])
            col_id = dicc_columnas['col_id'][0]
            df = dicc_df1['df'][[col_id] +
                                dicc_columnas['col_dep'][0]].drop_duplicates()

            for index, row in df.iterrows():
                if not pd.isna(row[col_id]):
                    d_id[row[col_id]] = row[1:]

            col_id = dicc_columnas['col_id'][1]
            df = dicc_df2['df'][[col_id] +
                                dicc_columnas['col_dep'][1]].drop_duplicates()
            # Usamos distancia discreta
            counter = [0]*long
            for index, row in df.iterrows():
                k = row[col_id]
                if k in d_id.keys():
                    for i in range(long):
                        if pd.isna(d_id[k][dicc_columnas['col_dep'][0][i]]) or pd.isna(row[dicc_columnas['col_dep'][1][i]]):
                            if not(pd.isna(d_id[k][dicc_columnas['col_dep'][0][i]]) and pd.isna(row[dicc_columnas['col_dep'][1][i]])):
                                counter[i] += 1
                        elif d_id[k][dicc_columnas['col_dep'][0][i]] != row[dicc_columnas['col_dep'][1][i]]:
                            counter[i] += 1
            if verbose:
                print()
                print('- Comparación entre ' + dicc_df1['name'] + ' y ' + dicc_df2['name'])
                for i in range(long):
                    if counter[i] > 0:
                        print('La columna {} tiene {}% de valores inconsistentes'.format(
                            dicc_columnas['col_dep'][1][i], round(counter[i]*100 / df.shape[0], 2)))
                    else:
                        print('La columna {} tiene {}% de valores inconsistentes'.format(
                            dicc_columnas['col_dep'][1][i], counter[i]))
            return counter

        else:
            if nulls_df1/size_df1 >= epsilon and nulls_df2/size_df2 >= epsilon:
                print('dfs no validos, columnas {} con {} y {} valores NULL'.format(
                dicc_columnas['col_id'],nulls_df1, nulls_df2))
                print("Porcentaje de valores NULL : {} -> {}%, {} -> {}%".format(
                    dicc_df1['name'], 100*nulls_df1/size_df1, dicc_df2['name'], 100*nulls_df2/size_df2))
            elif nulls_df1/size_df1 >= epsilon:
                print('df {} no valido, columna {} con {} valores NULL'.format(
                    dicc_df1['name'], dicc_columnas['col_id'][0], nulls_df1))
                print("Porcentaje de valores NULL :  {}%".format(
                    100*nulls_df1/size_df1))
            else:
                print('df {} no valido, columna {} con {} valores NULL'.format(
                    dicc_df2['name'], dicc_columnas['col_id'][1], nulls_df2))
                print("Porcentaje de valores NULL : {}%".format(
                    100*nulls_df2/size_df2))
            return -1

    else:
        print('No coincide nombres de columnas de los df propietarios')
        return -1

## ESTRUCTURA:
# dicc_columnas: 
#    lista_columnas_df1 : lista_columnas_df2
#    'col_dep' = (lista_de_columnas_df1, lista_de_columnas_df2) 
# los elementos de las listas de columnas están relacionados así:
# para todo e : lista_de_columnasdf1, e' : lista_de_columnasdf2 --> e es columna análoga de e', donde la analogía está dada por la semántica de los dfs 
def porcentajeDeValoresInexistentes(dict_df1,dict_df2, dict_columnas, lista_de_indefinidos=[], verbose=True):
    df1 = dict_df1['df']
    df2 = dict_df2['df']
    # generamos consulta
    lista_columnas = []
    for k in dict_columnas.keys():
        if isinstance(k, tuple) :
            lista_columnas += list(k)
        elif isinstance(k, str):
            lista_columnas.append(k)  
    tipos_df1 = evaluacionDeTipos(dict_df1, lista_columnas, verbose=False)
    consultaSQL = genConsultaExt(dict_columnas, tipos_df1, lista_de_indefinidos=lista_de_indefinidos)
    consulta = sql^consultaSQL[0]
    clave = consultaSQL[1]
    consultaSQL = genConsulta(lista_columnas)
    consulta_aux = sql^consultaSQL.format('df1')
    metrica = round(100-100*consulta.shape[0]/consulta_aux.shape[0],4)
    if verbose:
        print("La(s) columna(s) {} de la tabla {} contiene {}% de valores que no están en la tabla {}".format(clave, dict_df1['name'],metrica,dict_df2['name']))
    return metrica 

def porcentajeDeValoresFueraDeRango(dict_df, columna='', minimo_valor=0, maximo_valor=-1, verbose=True):
    if columna in dict_df['df'].keys():
        serie = dict_df['df'][columna].dropna()
        count = 0
        for valor in serie:
            if maximo_valor == minimo_valor-1:
                if valor < minimo_valor:
                    count += 1
            else:
                if valor < minimo_valor or valor > maximo_valor:
                    count += 1
        metrica = round(count/serie.size*100,2)
        if verbose:
            print("La columna {} de la tabla {} tiene {}% de valores fuera del rango esperado".format(columna,dict_df['name'],metrica))
        return metrica       
        
    else:
        print("Nombre de columna no valida")
        return None

# =============================================================================
# FUNCIONES PARA LA CORRECCIÓN DE DATOS EN TABLAS
# =============================================================================

# Función tipo void, modifica la referencia(?), no retorna copia
def limpiezaTuplasRepetidas(dict_df, lista_de_columnas=[]):
    if lista_de_columnas == []:
        lista_de_columnas = []

    consultaSQL = genConsulta(lista_de_columnas)
    df_consulta = dict_df['df']
    df_consulta = sql ^ consultaSQL.format('df_consulta')
    dict_df['df'] = df_consulta

# Opción 1
# Aplicamos la función limpiezaTuplasRepetidas a todas las tablas
def correccionTuplasRepetidas(lista_tablas):
    for tabla in lista_tablas:
        metrica = porcentajeTuplasRepetidasSobrantes(tabla)
        if metrica > 0:
            limpiezaTuplasRepetidas(tabla)

# Validación        
def validacionCorreccionTuplasRepetidas(lista_tablas):
    tablasValidas = True
    i = 0
    while tablasValidas and i < len(lista_tablas):
        metrica = porcentajeTuplasRepetidasSobrantes(lista_tablas[i],verbose=False)
        tablasValidas = metrica == 0
    
    if tablasValidas:
        print('Ninguna tabla tiene tuplas repetidas')
    else:
        print('Hay tuplas repetidas')

# Opción 2
# Aplicamos la funcion limpiezaTuplasRepetidas a todas las tablas y simultaneamente validamos
def correccionYValidacionTuplasRepetidas(lista_tablas):
    for tabla in lista_tablas:
        metrica = porcentajeTuplasRepetidasSobrantes(tabla)
        if metrica > 0:
            limpiezaTuplasRepetidas(tabla)
    print('Tablas sin tuplas repetidas')

def añadirIDs(dict_df, lista_de_columnas=[]):
    df = dict_df['df']
    if lista_de_columnas == []:
        lista_de_columnas = df.keys()
    i,j,k,l = 0,0,0,0
    dicc = {}
    nombre_nueva_columna = 'id_' + '_'.join(lista_de_columnas)
    for fila in range(0,len(df)): 
        flagI, flagN = False,False
        instancia = []
        for index in range(0,len(lista_de_columnas)):          
            if df.at[fila,lista_de_columnas[index]] is np.nan:
                flagN = True
                instancia.append('valorNULL')
            else:
                if df.at[fila,lista_de_columnas[index]] in {'SIN DEFINIR', 'INDEFINIDO', 'INDEFINIDA', 'NC'}:                    
                    flagI = True
                instancia.append(df.at[fila,lista_de_columnas[index]])
        tupla_instancia = tuple(instancia)
        if flagI and flagN:
            if tupla_instancia in dicc.keys():
                df.at[fila,nombre_nueva_columna] = dicc[tupla_instancia]
            else:
                df.at[fila,nombre_nueva_columna] = int(100000) + i
                dicc[tupla_instancia] = int(100000) + i
                i += 1
            flagI,flagN = False,False
        elif flagN:
            df.at[fila,nombre_nueva_columna] = int(80000) + j
            j += 1
            flagN = False
        elif flagI: 
            df.at[fila,nombre_nueva_columna] = int(90000) + k
            k += 1
            flagI = False
        else:
            if tupla_instancia in dicc.keys():
                df.at[fila,nombre_nueva_columna] = dicc[tupla_instancia]
            else:
                df.at[fila,nombre_nueva_columna] = l
                dicc[tupla_instancia] = l
                l += 1
    dict_df['df'] = df
    
## Estructura
# tupla_columnas : (columna_ID, columna_string)
# Reasigna de forma perezosa
def reasignarIDs(dict_df, tupla_columnas=(), lista_de_indefinidos=[]):
    # Nos aseguramos que los datos de entrada cumplan con el tipo de dato necesario para el funcionamoiento de la función.
    if isinstance(dict_df, dict) and isinstance(tupla_columnas, tuple) and len(tupla_columnas) == 2 and isinstance(lista_de_indefinidos,list):
        df = dict_df['df']
        # Creamos diccionario de id existentes, usando el valor string como clave, y el valor numerico como valor 
        d = dict()
        set_ids_prohibidos = set()
        set_valores_sin_clave = set()
        for i in range(df.shape[0]):
            tupla_valores = (df.at[i,tupla_columnas[0]],df.at[i,tupla_columnas[1]])
            if tupla_valores[1] is np.nan or tupla_valores[1] in lista_de_indefinidos:
                if isinstance(tupla_valores[0], int) or isinstance(tupla_valores[0], float):
                    set_ids_prohibidos.add(tupla_valores[0])
            elif isinstance(tupla_valores[1], str) :
                if isinstance(tupla_valores[0], int) or isinstance(tupla_valores[0], float) and not pd.isna(tupla_valores[0]):
                    d[tupla_valores[1]] = tupla_valores[0]
                elif not tupla_valores[1] in d.keys() :
                    set_valores_sin_clave.add(tupla_valores[1])
        
        maximo_valor = 0
        if max(d.values()) > max(set_ids_prohibidos):
            maximo_valor = max(d.values())
        else:
            maximo_valor = max(set_ids_prohibidos)            
        # Reasignamos ids
        maximo_valor += 10000 #10⁴
        j = 0
        for i in range(df.shape[0]):
            tupla_valores = (df.at[i,tupla_columnas[0]],df.at[i,tupla_columnas[1]])
            if not(tupla_valores[1] is np.nan or tupla_valores[1] in lista_de_indefinidos) and isinstance(tupla_valores[1], str):
                if tupla_valores[1] in d.keys():
                    df.at[i,tupla_columnas[0]] = d[tupla_valores[1]]
                elif tupla_valores[1] in set_valores_sin_clave:
                    d[tupla_valores[1]] = maximo_valor + j
                    df.at[i,tupla_columnas[0]] = maximo_valor + j
                    set_valores_sin_clave.remove(tupla_valores[1])
                    j += 1
        dict_df['df'] = df
    else:
        print("Parametros de entrada incorrectos para el funcionamiento de la función.")
        return None
 


 
# =============================================================================
# FUNCIONES PARA LA EXPLORACIÓN DE DEPENDENCIAS FUNCIONALES
# =============================================================================


def agregarColumnaMantieneCantidadDeTuplasUnicas(dict_df1, lcols, colname, debug=False):
    ttemp = crearDictdf(dict_df1['df'], 'df1', list(dict_df1['df'].columns))
    ttemp_unicas = selTuplasUnicas(ttemp, lcols)
    cant_unicas = len(ttemp_unicas)
    lista_ampliada = lcols.copy()
    lista_ampliada.append(colname)
    ttemp_unicas_ampliada = selTuplasUnicas(ttemp, lista_de_columnas=lista_ampliada)
    cant_unicas_ampliada = len(ttemp_unicas_ampliada)
    if debug:
        print("DEBUG: cant_unicas: " + str(cant_unicas) + " cant_unicas_ampliada: " + str(cant_unicas_ampliada))
        #filtro = ~selTuplasRepetidasSobrantes(dict_df1, lista_ampliada).isin(selTuplasRepetidasSobrantes(dict_df1, lcols))
        filtro = selTuplasRepetidasSobrantes(dict_df1, lista_ampliada).isin(selTuplasRepetidasSobrantes(dict_df1, lcols))
        return dict_df1['df'][filtro]
    if cant_unicas_ampliada > cant_unicas:
        return False
    elif cant_unicas_ampliada == cant_unicas:
        return True
    else:
        print("ERROR")


def obtenerDFCandidata(dict_df, listacols):
    for i in list(dict_df['df'].columns):
        if i not in listacols:
            if agregarColumnaMantieneCantidadDeTuplasUnicas(dict_df, listacols, i):
                print("Según los datos existentes en la tabla, la columna",
                      dict_df['name'] +"['" + str(i) + "']",
                      "depende funcionalmente de", dict_df['name'] + str(listacols) + "\n")
    print("----------------------")


# Ejemplo de uso:
#
# obtenerDFCandidata(operadores, ['razon_social', 'establecimiento']) 
# obtenerDFCandidata(operadores, ['rubro'])



# =============================================================================
# FUNCIONES PARA LA VISUALIZACIÓN
# =============================================================================


def cant_trozos_texto_separados_por_comas_e_yes(texto): 
    res = 1
    for i in range(1,len(texto)-1): 
        if texto[i] == ',':  
            res = res + 1
        if texto[i] == 'Y' and texto[i-1] == ' ' and texto[i+1] == ' ' :
            res = res + 1
        if texto[i] == 'y' and texto[i-1] == ' ' and texto[i+1] == ' ' :
            res = res + 1 
    return res 




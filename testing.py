#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
  Archivo: testing.py
  Proyecto para Laboratorio de datos, Facultad de Ciencias Exactas y Naturales, UBA
  Integrantes del grupo: Ariel Dembling, Antony Suárez Caina, Camila Guibaudo 
  Fecha: 27/06/23
  Descripción: aquí están todos los tests que se utilizaron para verificar la 
  correcta funcionalidad de las funciones de funciones.py
"""

# =============================================================================
# LIBRERÍAS
# =============================================================================
import pandas as pd
import numpy as np
from inline_sql import sql, sql_val
import sys
import os

# Establecer rootdir al directorio actual desde donde se ejecuta el script
rootdir = os.getcwd()

# Agregamos el rootdir del directorio donde está ubicado nuestro módulo a importar
sys.path.append(rootdir)
import funciones as fn


# =============================================================================
# FUNCIÓN PARA TESTING
# =============================================================================

# Función genérica para correr un test.
# Recibe un diccionario con metadata describiendo el test, donde evuelve True o
# False según el test concuerde con el resultado esperado o no.
def testear(test):
    if test['test_cant_args'] == 1:
        res = test['test_func'](test['test_arg1'])
    elif test['test_cant_args'] == 2:
        res = test['test_func'](test['test_arg1'], test['test_arg2'])
    elif test['test_cant_args'] == 3:
        res = test['test_func'](test['test_arg1'], test['test_arg2'], test['test_arg3'])
    elif test['test_cant_args'] == 4:
        res = test['test_func'](test['test_arg1'], test['test_arg2'], test['test_arg3'], test['test_arg4'])
    else:
        print("ERROR: la función testear() solo soporta funciones de hasta 4 argumentos.")
        return False

    if res is None and isinstance(test['test_arg1'], dict) and 'df' in test['test_arg1']:
        res = test['test_arg1']['df'][test['name'].replace('test ', '')].tolist()

    res_correcto = np.array(test['res_correcto'])
    res_obtenido = np.array(res)

    if np.array_equal(res_correcto, res_obtenido):
        print("OK: el test "+ test['name'] +" dio el resultado esperado.")
        return True
    else:
        print(f"ERROR: el test {test['name']} falló.")
        print(f"Resultado esperado para {test['name']}:", res_correcto)
        print(f"Resultado obtenido para {test['name']}:", res_obtenido)
        return False


##################################################
### Test: porcentaje de tuplas repetidas sobrantes
##################################################
# Nota: en lugar de crear un nuevo diccionario de metadata
#       del dataframe en "test_arg1", reutilizamos este
#       mismo diccionario y lo hacemos compatible con la
#       función a testear al agregarle una entrada "df".
#       Para esto, es necesario primero crear el diccionario y luego
#       agregarle el valor a "test_arg1" una vez que el diccionario ya
#       exista.

# Test 1
dicc_test = {'name': 'test'}
d = {
    'A': ['a', 'b', 'c', 'a', 'a'],
    'B': [1, 2, 3, 1, 1],
    'C': ['alfa', 'beta', 'gamma', 'alfa', 'alfa']
}
df_test = pd.DataFrame(data=d)
dicc_test['df'] = df_test
resultado = 40
test = {'name': 'test porcentajeTuplasRepetidas',
        'test_func' : fn.porcentajeTuplasRepetidasSobrantes,
        'test_cant_args' : 1,
        'test_arg1' : dicc_test,
        'res_correcto' : resultado
        }

testear(test)

# Test 2
d = pd.DataFrame({
    'A': ['a', 'b', 'c', 'a', 'a'],
    'B': [1, 2, 3, 1, 1],
    'C': ['alfa', 'beta', 'gamma', 'alfa', 'alfa']
})
test_a1={"name": "test_a1",
             "df": d,
             "test_func": fn.porcentajeTuplasRepetidasSobrantes,
             "test_cant_args": 2,
             "test_arg2": list(d.columns),
             "res_correcto": 40.0}
test_a1["test_arg1"] = test_a1
testear(test_a1)

# Test 3
df_test_r = pd.DataFrame({
  "columna1": ["x", np.nan, np.nan, "", "", "0", "a", "b", "c", "a", "b", "c", "a", "b", "c", "c"],
  "columna2": ["0", np.nan, np.nan, "", "", "x", "1", "2", "3", "1", "2", "3", "1", "2", np.nan, np.nan]
    })

test_gqm={"name": "test_gqm1",
           "df": df_test_r, 
           "test_func": fn.porcentajeTuplasRepetidasSobrantes,
           "test_cant_args": 2,
           "test_arg2": list(df_test_r.columns),
           "res_correcto": 50.0}
test_gqm["test_arg1"] = test_gqm
testear(test_gqm)

##################################################
### Test: porcentaje de tuplas conteniendo NULL (all)
##################################################

# Test 1
test_gqm2_2={"name": "test_gqm2_2",
             "df": df_test_r,
             "test_func": fn.porcentajeDeTuplasConValoresNullAll,
             "test_cant_args": 2,
             "test_arg2": list(df_test_r.columns),
             "res_correcto": 12.5}
test_gqm2_2["test_arg1"] = test_gqm2_2
testear(test_gqm2_2)

# Test 2
d = {
    'A': [np.nan, np.nan, np.nan, np.nan, np.nan],
    'B': [1.2, 3, 1, 7, 1],
    'C': ['alfa', 'beta', 'gamma', 'theta', 'sigma'],
    'D': [2, np.nan, 5, np.nan, 9]
}
df_test = pd.DataFrame(data=d)
dicc_test['df'] = df_test
test = {'name': 'test porcentajeDeTuplasConValoresNullAll',
        'test_func' : fn.porcentajeDeTuplasConValoresNullAll,
        'test_cant_args' : 1,
        'test_arg1' : dicc_test,
        'res_correcto' : 0
        }

testear(test)

resultados = {'A':100, 'B':0, 'C':0, 'D': 40}
test['test_cant_args'] = 2
for k in resultados:
    test['test_arg2'] = k
    test['res_correcto'] = resultados[k]
    testear(test)

##################################################
### Test: porcentaje de tuplas conteniendo NULL (any)
##################################################

# Test 1
test_gqm2_1={"name": "test_gqm2_1",
             "df": df_test_r,
             "test_func": fn.porcentajeDeTuplasConValoresNullAny,
             "test_cant_args": 2,
             "test_arg2": list(df_test_r.columns),
             "res_correcto": 25.0}
test_gqm2_1["test_arg1"] = test_gqm2_1
testear(test_gqm2_1)

# Test 2
test['test_cant_args'] = 1
test['name'] = 'test porcentajeDeTuplasConValoresNullAny'
test['test_func'] = fn.porcentajeDeTuplasConValoresNullAny
test['res_correcto'] = 100
testear(test)

test['test_cant_args'] = 2
for k in resultados:
    test['test_arg2'] = k
    test['res_correcto'] = resultados[k]
    testear(test)
    

##################################################
### Test: porcentaje de valores indefinidos
##################################################
test_gqm3={"name": "test_gqm3",
             "df": df_test_r,
             "test_func": fn.porcentajeDeValoresIndefinidos,
             "test_cant_args": 3,
             "test_arg2": "columna1",
             "test_arg3": ["", "0", "a"],
             "res_correcto": 37.5}
test_gqm3["test_arg1"] = test_gqm3
testear(test_gqm3)

d = {
    'A': [np.nan, np.nan, np.nan, 'SIN DEFINIR', np.nan],
    'B': [1.2, 3, 1, 7, 1],
    'C': ['alfa', 'beta', 'NC', 'theta', 'INDEFINIDO'],
    'D': [2, np.nan, 5, np.nan, 9],
    'E': ['a', 'INDEFINIDA', 'c', 'NC', 'e']
    }
df_test = pd.DataFrame(data=d)
dicc_test['df'] = df_test
resultados = {'A':20,'B':0,'C':40,'D':0,'E':40}
valores_indefinidos = ["INDEFINIDO", "INDEFINIDA", "SIN DEFINIR", "NC"]
test = {'name': 'test porcentajeDeValoresIndefinidos',
        'test_func' : fn.porcentajeDeValoresIndefinidos,
        'test_cant_args' : 3,
        'test_arg1' : dicc_test,
        'test_arg2' : 'A',
        'test_arg3' : valores_indefinidos,
        'res_correcto' : 20
        }

for k in resultados:
    test['test_arg2'] = k
    test['res_correcto'] = resultados[k]
    testear(test)


####################################################
### Test: diferencia de cantidades de valores únicos
####################################################

# Test 1
test_gqm4={"name": "test_gqm4",
             "df": df_test_r[df_test_r["columna1"] == "c"],
             "test_func": fn.diferenciaDeCantidadesDeValoresUnicosUnsigned,
             "test_cant_args": 3,
             "test_arg2": "columna1",
             "test_arg3": "columna2",
             "res_correcto": 1}
test_gqm4["test_arg1"] = test_gqm4
testear(test_gqm4)

# test 2
d = {
    'A': [1, 2, 3, 1, 4, 4],
    'B': ['a', 'b', 'c', 'a', 'd', 'd'],
    'C': ['X', 'Y', 'Z', 'X', 'V', 'V'],
    'D': ['alfa', 'beta', 'gamma', 'alfa', 'delta', 'delta'],
    'E': [11, 11, 11, 11, 44, 44]
}
df_test = pd.DataFrame(data=d)
dicc_test = {'name': 't1', 'df':df_test}

test = {'name': 'test diferenciaDeCantidadesDeValoresUnicosUnsigned',
        'test_func' : fn.diferenciaDeCantidadesDeValoresUnicosUnsigned,
        'test_cant_args' : 3,
        'test_arg1' : dicc_test,
        'test_arg2' : 'A',
        'test_arg3' : 'E',
        'res_correcto' : 2
        }
testear(test)

test['test_arg3'] = 'B'
test['res_correcto'] = 0
testear(test)

test['test_arg3'] = 'C'
test['res_correcto'] = 0
testear(test)


####################################################
### Test: diferencia de tipos
####################################################
d = {
    'A': [1, 2, 4, 3, 5],
    'B': [1.2, 5.4, 5.6, 7, 8],
    'C': [1, 'e', 'y', 6, 1.2],
    'D': ['a', 'b', 'c', 'd', 'e'],
    'E': ['x', 'r', 't', np.nan, 'y']}
df_test = pd.DataFrame(data=d)
dicc_test = {'name': 'test de tipos de datos', 'df':df_test}
resultado = {'A':(100,int),
             'B':(100,float),
             'C':(40,int),
             'D': (100,str),
             'E':(80,str)}
test = {'name': 'test evaluacionDeTiposPorColumna',
        'test_func' : fn.evaluacionDeTiposPorColumna,
        'test_cant_args' : 2,
        'test_arg1' : dicc_test,
        'test_arg2' : 'A',
        'res_correcto' : (100,int)
        }

for k in resultado:
    test['test_arg2'] = k
    test['res_correcto'] = resultado[k]
    testear(test)

####################################################
### Test: diferencia de consistencia de valores
####################################################
d = {
    'A': [1, 2, 3, 1, 4, 5],
    'B': ['a', 'b', 'c', 'a', 'd', 'e'],
    'C': ['X', 'Y', 'Z', 'X', 'V', 'W'],
    'D': ['alfa', 'beta', 'gamma', 'alfa', 'delta', 'epsilon'],
    'E': [11, 22, 33, 11, 44, 55]
}
df_test = pd.DataFrame(data=d)
dicc_test1 = {'name': 't1', 'df':df_test}

d = {
    'A': [1, 2, 4, 1, 5, 4],
    'B': ['a', 'b', 'd', 'a', 'e', 'd'],
    'C': ['X', 'Y', 'V', 'X', 'W', 'V'],
    'D': ['alfa', 'beta', 'delta', 'alfa', 'epsilon', 'delta'],
    'E': [11, 22, 44, 11, 55, 44]
}
df_test = pd.DataFrame(data=d)
dicc_test2 = {'name': 't2', 'df':df_test}

d = {
    'A': [1, 2, 4, 1, 5, 4],
    'B': ['a', np.nan, 'c', 'f', 'd', 'e'],
    'C': ['X', 'Y', 'Z', 'T', 'V', 'W'],
    'D': ['alfa', 'theta', 'gamma', 'alfa', 'delta', 'epsilon'],
    'E': [11, 22, 33, 11, 44, 55]
}
df_test = pd.DataFrame(data=d)
dicc_test3 = {'name': 't3', 'df':df_test}

d = {
    'A': [1, 2, 4, 1, 5, 4],
    'B': ['a', np.nan, np.nan, 'f', 'd', 'e'],
    'C': ['X', 'Y', 'Z', 'T', 'V', 'W'],
    'D': ['alfa', 'theta', 'gamma', 'alfa', 'delta', 'epsilon'],
    'E': [11, 22, 33, 11, 44, 55]
}
df_test = pd.DataFrame(data=d)
dicc_test4 = {'name': 't4', 'df':df_test}

columnas_a_evaluar = {
        'caso 1': {'col_id': ('A','A'),
                  'col_dep':(['B', 'C', 'D', 'E'], ['B', 'C', 'D', 'E'])},  # dicc_test1,dicc_test2
        'caso 2': {'col_id': ('B','B'),
                  'col_dep': (['D', 'C'], ['D', 'C'])},                     # dicc_test1,dicc_test2
        'caso 3': {'col_id': ('A','A'),
                  'col_dep': (['B', 'C'], ['B', 'C'])},                     # dicc_test1,dicc_test3
        'caso 4': {'col_id': ('D','D'),
                  'col_dep': (['E', 'C'], ['E', 'C'])},                     # dicc_test1, dicc_test4
        'caso 5': {'col_id': ('C','C'),
                  'col_dep': (['B'],['B'])}                                 # dicc_test3, dicc_test4
        }
resultados = {'caso 1':[0, 0, 0, 0],
              'caso 2':[0 ,0],
              'caso 3':[5, 4],
              'caso 4':[0, 1],
              'caso 5':[1]}
test = {'name': 'test consistenciaExtendida',
        'test_func' : fn.consistenciaExtendida,
        'test_cant_args' : 3,
        'test_arg1' : dicc_test1,
        'test_arg2' : dicc_test2,
        'test_arg3' : columnas_a_evaluar['caso 1'],
        'res_correcto' : resultados['caso 1']
        }

testear(test)

test['test_arg3'] = columnas_a_evaluar['caso 2']
test['res_correcto'] = resultados['caso 2']
testear(test)

test['test_arg2'] = dicc_test3
test['test_arg3'] = columnas_a_evaluar['caso 3']
test['res_correcto'] = resultados['caso 3']
testear(test)

test['test_arg2'] = dicc_test4
test['test_arg3'] = columnas_a_evaluar['caso 4']
test['res_correcto'] = resultados['caso 4']
testear(test)

test['test_arg1'] = dicc_test3
test['test_arg3'] = columnas_a_evaluar['caso 5']
test['res_correcto'] = resultados['caso 5']
testear(test)


####################################################
### Test: diferencia de porcentaje de valores inexistentes
####################################################
d = {
    'A': [1, 2, 3, 1, 4, 5],
    'B': ['a', 'b', 'c', 'a', 'd', 'e'],
    'C': ['X', 'Y', 'Z', 'X', 'V', 'W'],
    'D': ['alfa', 'beta', 'gamma', 'alfa', 'delta', 'epsilon'],
    'E': ['rosa', 'agapanto', 'lilium', 'rosa', 'azucena', 'jazmin']
    }
df_test = pd.DataFrame(data=d)
dicc_test1 = {'name': 't1', 'df':df_test}

d = {
    'A': [1, 2, 4, 1, 5, 4],
    'B': ['a', 'b', 'd', 'a', 'e', 'd'],
    'C': ['X', 'Y', 'V', 'X', 'W', 'V'],
    'D': ['alfa', 'beta', 'delta', 'alfa', 'epsilon', 'delta'],
    'E': ['rosa', 'agapanto', 'azucena', 'rosa', 'jazmin', 'azucena']
    }
df_test = pd.DataFrame(data=d)
dicc_test2 = {'name': 't2', 'df':df_test}

d = {
    'A': [1, 2, 3, 1, 4, 5],
    'B': ['INDEFINIDO', 'Y', 'NC', 'X', 'd', 'SIN DEFINIR'],
    'C': ['X', 'INDEFINIDA', 'Z', np.nan, 'V', 'W'],
    'D': ['alfa', 'beta', 'gamma', 'alfa', 'delta', 'epsilon'],
    'E': ['rosa', 'agapanto', 'lilium', 'rosa', 'azucena', 'jazmin'],
    'F': [22,11,66,33,55,44]      # B, C
    }
df_test = pd.DataFrame(data=d)
dicc_test3 = {'name': 't3', 'df':df_test}

d = {
    'A': [1, 2, 3, 1, 4, 5],
    'B': ['NC', np.nan, 'Z', 'f', 'di', 'W'],
    'C': ['alfa', 'Y', 'NC', 'INDEFINIDO', 'Vi', 'wu'],
    'D': ['X', 'alfa', 'gamma', 'T', 'V', 'epsilon'], 
    'E': [11, 22, 33, 11, 44, 55]                                           # B,C,D
    }
df_test = pd.DataFrame(data=d)
dicc_test4 = {'name': 't4', 'df':df_test}

columnas_a_evaluar = {
        'caso 1': {('B','C'):('B', 'C', 'D'),
                   'A': ('A')},                 # dicc_test3,dicc_test4
        'caso 2': {'E': ('E'),
                   'D': ('D'),
                   'B': ('B'),
                   'C': ('C'),
                   'A':('A')},                   # dicc_test1,dicc_test2
        'caso 3': {'A':('A'), 'E':('F')}        # dicc_test4, dicc_test3
        }
resultados = {'caso 1': 0, 'caso 2':20, 'caso 3': 100}

test = {'name': 'test porcentajeValoresInexistentes',
        'test_func' : fn.porcentajeDeValoresInexistentes,
        'test_cant_args' : 4,
        'test_arg1' : dicc_test3,
        'test_arg2' : dicc_test4,
        'test_arg3' : columnas_a_evaluar['caso 1'],
        'test_arg4' : valores_indefinidos,
        'res_correcto' : resultados['caso 1']
        }

testear(test)

test['test_arg1'] = dicc_test1
test['test_arg2'] = dicc_test2
test['test_arg3'] = columnas_a_evaluar['caso 2']
test['res_correcto'] = resultados['caso 2']
testear(test)

test['test_arg1'] = dicc_test4
test['test_arg2'] = dicc_test3
test['test_arg3'] = columnas_a_evaluar['caso 3']
test['res_correcto'] = resultados['caso 3']
testear(test)



####################################################
### Test: añadir IDs
####################################################
# Datos iniciales para la prueba
d = {
    'A': [1, 2, 3, 1, 4, 5, 6, 7],
    'B': ['NC', 'b', 'c', 'NC', 'd', 'e', 'INDEFINIDA', 'SIN DEFINIR'],
    'C': ['X', 'Y', 'Z', 'X', 'V', 'W', np.nan, 'U'],
    'D': ['alfa', 'beta', 'gamma', 'alfa', 'delta', 'epsilon', 'iota', 'sigma'],
    'E': [11, 22, 33, 11, 44, 55, 66, 77],
    'F': [np.nan, np.nan, 'cala', 'INDEFINIDO', 'rosa', 'lilium', 'NC', np.nan]
}
df_test = pd.DataFrame(data=d)
dicc_test = {'name': 'test añadir IDs', 'df': df_test}

d_resultados = {
    'id_A_E': [0, 1, 2, 0, 3, 4, 5, 6], # A_E
    'id_B_D': [90000.0,0.0,1.0,90001.0,2.0, 3.0, 90002.0,90003.0], # B_D
    'id_F': [80000.0,80001.0,0.0,90000.0,1.0,2.0,90001.0,80002.0], # F
    'id_B_F': [100000.0,80000.0,0.0,90000.0,1.0,2.0,90001.0,100001.0], #B_F
    'id_A_B_C_D_E_F': [100000.0,80000.0,0.0,90000.0,1.0,2.0,100001.0,100002.0] #d.columns
}

resultados = pd.DataFrame(data=d_resultados)

columnas_a_evaluar = {
    'id_A_E': ['A', 'E'],
    'id_B_D': ['B', 'D'],
    'id_F': ['F'],
    'id_B_F': ['B', 'F'],
    'id_A_B_C_D_E_F': []
}

test_aprobado = True
for case, columns in columnas_a_evaluar.items():
    # Copia el diccionario y DataFrame para no modificar el original
    temp = dicc_test.copy()
    temp['df'] = df_test.copy()

    test = {
        'name': f'test {case}',
        'test_func': fn.añadirIDs,
        'test_cant_args': 2,
        'test_arg1': temp,
        'test_arg2': columns,
        'res_correcto': d_resultados[case]
    }

    if not testear(test):
        test_aprobado = False
        print(f"El test para {case} falló.")

####################################################
### Test: reasignar IDs
####################################################
d = {
    'A': [1, 2, 3, np.nan, 4, 5, 6, 7, 8, np.nan],
    'B': ['NC', 'b', 'c', 'NC', 'd', 'e','INDEFINIDA', 'SIN DEFINIR','T', 'g'],
    'C': ['X', 'Y', 'Z', 'X', 'V', 'W', np.nan, 'U', 'A', 'B'],
    'D': ['alfa', 'beta', 'gamma', 'alfa', 'delta', 'epsilon', 'iota', 'NC', np.nan, 'sigma'],
    'E': [11, 22, 33, 11, 44, 55, 66, 77, 88, 99],
    'F': [np.nan, np.nan, 'cala','INDEFINIDO', 'rosa', 'lilium', 'NC', np.nan, np.nan, 'hibiscus']
}
df_test = pd.DataFrame(data=d)
dicc_test = {'name': 'test añadir IDs', 'df' : df_test}
fn.reasignarIDs(dicc_test,('A','D'),lista_de_indefinidos=["INDEFINIDO", "INDEFINIDA", "SIN DEFINIR", "NC"])


####################################################
### Test: valores fuera de rango
####################################################

d = {
    'A': [1, 2, 3, np.nan, 4, 5, 6, -7, 8, np.nan],
    'B': [11, 22, 33, 11, 44, 55, 66, 77, 88, 99]
}
dicc_test = {'name':'test valores fuera de rango'}
df_test = pd.DataFrame(data=d)
dicc_test['df'] = df_test

test = {
        'name':'valores fuera de rango',
        'test_func': fn.porcentajeDeValoresFueraDeRango,
        'test_cant_args': 4,
        'test_arg1': dicc_test,
        'test_arg2': 'A',
        'test_arg3': 0,
        'test_arg4': 1000,    
        'res_correcto': (1/8)*100
        }

testear(test)

test = {
        'name':'valores fuera de rango',
        'test_func': fn.porcentajeDeValoresFueraDeRango,
        'test_cant_args': 4,
        'test_arg1': dicc_test,
        'test_arg2': 'B',
        'test_arg3': 0,
        'test_arg4': -1,    
        'res_correcto': 0
        }

testear(test)











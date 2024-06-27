import mysql.connector
import os
import threading
from queue import Queue

# Configuración de la conexión a la base de datos
db_config = {
    'host': '',
    'user': '',
    'password': '',
    'database': '',
}

# Directorio donde se guardarán los archivos
output_dir = 'archivos_descargados'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def descargar_archivo(id, archivo):
    """Función para descargar un archivo."""
    file_path = os.path.join(output_dir, f'documento_{id}.png')
    with open(file_path, 'wb') as file:
        file.write(archivo)
    print(f'Archivo {file_path} guardado.')

def worker(queue):
    """Worker thread function."""
    while True:
        item = queue.get()
        if item is None:
            break
        descargar_archivo(*item)
        queue.task_done()

# Conexión a la base de datos
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Consulta para obtener los archivos BLOB
query = "SELECT  id,  imagen FROM example_table " #ejemplo de tabla
cursor.execute(query)

# Cola de tareas
queue = Queue()

# Crear hilos de trabajo
num_threads = 10  # Ajustar según los recursos disponibles
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue,))
    t.start()
    threads.append(t)

# Encolar tareas
for (id, archivo) in cursor:
    queue.put((id, archivo))

# Esperar a que todas las tareas se completen
queue.join()

# Detener hilos de trabajo
for i in range(num_threads):
    queue.put(None)
for t in threads:
    t.join()

# Cierre de la conexión
cursor.close()
conn.close()

print('Todos los archivos png han sido descargados.')

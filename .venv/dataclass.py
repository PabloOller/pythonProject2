from pyspark.sql import SparkSession
from dataclasses import dataclass
from typing import List
from pyspark.sql.functions import avg, col, sum

# Inicializamos la sesión de Spark
spark = SparkSession.builder.appName("TiendaInformaticaExample").getOrCreate()

# Definimos la dataclass para los productos
@dataclass
class Producto:
    nombre: str
    categoria: str
    precio: float
    stock: int
    marca: str

# Creamos una lista de instancias de la dataclass
productos: List[Producto] = [
    Producto(nombre="Laptop A", categoria="Laptop", precio=1200.0, stock=10, marca="MarcaX"),
    Producto(nombre="Laptop B", categoria="Laptop", precio=1500.0, stock=5, marca="MarcaY"),
    Producto(nombre="Monitor A", categoria="Monitor", precio=300.0, stock=20, marca="MarcaX"),
    Producto(nombre="Teclado A", categoria="Teclado", precio=50.0, stock=100, marca="MarcaZ"),
    Producto(nombre="Teclado B", categoria="Teclado", precio=70.0, stock=60, marca="MarcaY"),
    Producto(nombre="Monitor B", categoria="Monitor", precio=450.0, stock=15, marca="MarcaY"),
    Producto(nombre="Laptop C", categoria="Laptop", precio=1800.0, stock=3, marca="MarcaZ"),
]

# Convertimos la lista de productos en un DataFrame
df = spark.createDataFrame(productos)

# 1. Calcular el precio promedio por categoría
precio_promedio_por_categoria = df.groupBy("categoria").agg(avg("precio").alias("precio_promedio"))
print("Precio promedio por categoría:")
precio_promedio_por_categoria.show()

# 2. Calcular el total de stock por marca
stock_total_por_marca = df.groupBy("marca").agg(sum("stock").alias("stock_total"))
print("Stock total por marca:")
stock_total_por_marca.show()

# 3. Filtrar productos con precio superior a 1000 y recolectar los resultados
productos_caros = df.filter(col("precio") > 1000 & col("precio")>1800).collect()

print("Productos con precio superior a 1000:")
for producto in productos_caros:
    print(f"Nombre: {producto['nombre']}, Categoría: {producto['categoria']}, Precio: {producto['precio']}, Marca: {producto['marca']}")

# 4. Crear una lista de nombres de productos de la marca "MarcaY" usando collect()
nombres_marca_y = df.filter(col("marca") == "MarcaY").select("nombre").collect()

# Convertimos los resultados en una lista de nombres de Python
nombres_marca_y = [row['nombre'] for row in nombres_marca_y]
print("\nNombres de productos de la marca 'MarcaY':", nombres_marca_y)

# Cerramos la sesión de Spark
spark.stop()

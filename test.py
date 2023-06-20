import re 
import pandas as pd
import mysql.connector

# Conexión a la base de datos MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="cdugarte1",
    password="9uwkkcgkCD",
    database="farmu"
)

# Creación de la tabla
create_table_query = """
CREATE TABLE IF NOT EXISTS test3 (
    order_number VARCHAR(255),
    order_status VARCHAR(255),
    customer_email VARCHAR(255),
    preferred_delivery_date DATE,
    preferred_delivery_hours VARCHAR(255),
    sales_person VARCHAR(255),
    notes TEXT,
    address VARCHAR(255),
    neighbourhood VARCHAR(255),
    city VARCHAR(255),
    creation_date DATETIME,
    source VARCHAR(255),
    warehouse VARCHAR(255),
    shopify_id VARCHAR(255),
    sales_person_role VARCHAR(255),
    order_type VARCHAR(255),
    is_pitayas VARCHAR(255),
    discount_applications VARCHAR(255),
    payment_method VARCHAR(255)
)
"""
cursor = connection.cursor()
cursor.execute(create_table_query)
connection.commit()
print("Tabla 'test3' creada con éxito.")


# Leer datos del archivo CSV en un DataFrame
df = pd.read_csv('bquxjob_3b3102f3_18728d1eb06.csv')

# Realizar el procesamiento de los datos en el DataFrame
df['preferred_delivery_date'] = pd.to_datetime(df['preferred_delivery_date'], errors='coerce').dt.date

# Iterar sobre las filas del DataFrame y realizar la inserción en la base de datos
for _, row in df.iterrows():
    # Verificar si el valor de preferred_delivery_date es nulo
    if pd.isnull(row['preferred_delivery_date']):
        preferred_delivery_date = None  # Asignar None si el valor es nulo en el DataFrame
    else:
        preferred_delivery_date = row['preferred_delivery_date']

    # Utilizar expresiones regulares para extraer "AM" o "PM" y eliminar el texto sobrante
    pattern = re.compile(r'(AM|PM)')
    df['preferred_delivery_hours'] = df['preferred_delivery_hours'].apply(lambda x: pattern.search(x).group() if pattern.search(x) else x)


    # Realizar la inserción en la base de datos
    insert_query = """
    INSERT INTO test3 (order_number, order_status, customer_email, preferred_delivery_date, preferred_delivery_hours, sales_person, notes, address, neighbourhood, city, creation_date, source, warehouse, shopify_id, sales_person_role, order_type, is_pitayas, discount_applications, payment_method)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        row['order_number'],
        row['order_status'],
        row['customer_email'],
        preferred_delivery_date,
        row['preferred_delivery_hours'],
        row['sales_person'],
        row['notes'],
        row['address'],
        row['neighbourhood'],
        row['city'],
        row['creation_date'],
        row['source'],
        row['warehouse'],
        row['shopify_id'],
        row['sales_person_role'],
        row['order_type'],
        row['is_pitayas'],
        row['discount_applications'],
        row['payment_method']
    ))

# Confirmar los cambios en la base de datos
connection.commit()

# Cerrar el cursor y la conexión
cursor.close()
connection.close()

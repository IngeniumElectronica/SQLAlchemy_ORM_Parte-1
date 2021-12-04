from datetime import datetime
from sqlalchemy import (MetaData, Table, Column, Integer, Numeric, String, DateTime, ForeignKey, create_engine, insert)
metadata=MetaData()

#Definicion de tablas
cookies = Table ("cookies", metadata,
        Column("cookie_id", Integer(), primary_key=True),
        Column("cookie_name", String(50), index=True),
        Column("cookie_recipe_url", String(255)),
        Column("cookie_sku", String(55)),
        Column("quantity", Integer()),
        Column("unit_cost", Integer())
)

users = Table ("users", metadata,
        Column("user_id", Integer(), primary_key=True),
        Column("customer_number", Integer(), autoincrement=True),
        Column("username", String(15), nullable=False, unique=True),
        Column("email_address", String(255), nullable=False),
        Column("phone", String(20), nullable=False),
        Column("password", String(25), nullable=False),
        Column("created_on", DateTime(), default=datetime.now),
        Column("update_on", DateTime(), default=datetime.now, onupdate=datetime.now)
)

#Relacion entre tablas
orders= Table("orders", metadata,
        Column("order_id", Integer(), primary_key=True),
        Column("user_id", ForeignKey("users.user_id"))
        )

line_items = Table('line_items', metadata,    
        Column('line_items_id', Integer(), primary_key=True),    
        Column('order_id', ForeignKey('orders.order_id')),    
        Column('cookie_id', ForeignKey('cookies.cookie_id')),    
        Column('quantity', Integer()),    
        Column('extended_cost', String(15))
        )

#Creacion de motor
engine=create_engine("sqlite:///C:\\Users\\Baez\\Desktop\\datos\\virtual\\coredata.db") 
connection=engine.connect()
metadata.drop_all(engine)
metadata.create_all(engine)

#Insercion de datos
#Insercion sencilla
"""ins= cookies.insert().values(
    cookie_name="chocolate chip", 
    cookie_recipe_url="chocolatechip.com",
    cookie_sku="CC01",
    quantity="20", 
    unit_cost="5"
)

#Confirmacion del registro
print(str(ins))

ins.compile().params
result=connection.execute(ins)
result.inserted_primary_key"""

inventory_list= [
                {
                'cookie_name': 'chocolate chip',        
                'cookie_recipe_url': 'chocolatechip.com',        
                'cookie_sku': 'CC01',        
                'quantity': '24',        
                'unit_cost': '0.25'     
                },
                {        
                'cookie_name': 'dark chocolate chip',        
                'cookie_recipe_url': 'dark.com',        
                'cookie_sku': 'DCC01',        
                'quantity': '24',        
                'unit_cost': '0.25'    
                },
                {        
                'cookie_name': 'peanut butter',        
                'cookie_recipe_url': 'peanut.com',        
                'cookie_sku': 'PB01',        
                'quantity': '24',        
                'unit_cost': '0.25'    
                },    
                {        
                'cookie_name': 'oatmeal raisin',        
                'cookie_recipe_url': 'raisin.com',        
                'cookie_sku': 'EWW01',        
                'quantity': '100',        
                'unit_cost': '1.00'    
                } 
]
ins= cookies.insert()
result=connection.execute(ins, inventory_list)

#Consulta de datos
print("\nConsulta de datos")
from sqlalchemy.sql import select
s=select([cookies])
rp=connection.execute(s)
results=rp.fetchall()
for result in results:
    print(result)

print("\nManipulacion de los resultados de una delcaracion")
first_row=results[0]
print(first_row)
print(first_row[2])#Acceder a la columna por indice
print(first_row.cookie_name)#Acceder a la columna nombre
rp=connection.execute(s)#Iterar todos los nombres de la columna cookies
for cookie in rp:
    print(cookie.cookie_name)

print("\nLimitar los campos devueltos en una consulta")
s=select([cookies.c.cookie_name, cookies.c.quantity])
rp=connection.execute(s)
print(rp.keys())
for cookie in rp:
    print(cookie)

print("\nOrdenando datos")
from sqlalchemy import desc
s=select([cookies.c.cookie_name, cookies.c.quantity])
s=s.order_by(cookies.c.quantity)#Ordenar forma descendente
rp=connection.execute(s)
for cookie in rp:
    print("{} - {}".format(cookie.quantity, cookie.cookie_name))

print("\nLimitar los campos devueltos en una consulta")
s=select([cookies.c.cookie_name, cookies.c.quantity])
s=s.order_by(cookies.c.quantity)#Ordenar forma descendente
s=s.limit(3)
rp=connection.execute(s)
print([cookie.cookie_name for cookie in rp])
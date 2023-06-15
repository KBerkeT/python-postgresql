from psycopg2 import connect
import pandas as pd

class DB:

	def __init__(self,db_name, user, password):
		self.db_name = db_name
		self.user = user
		self.password = password
		self.host = "localhost"
		self.port = "5432"


	def connect_db(self):
		self.conn = connect(database=self.db_name, user=self.user, password=self.password, host=self.host, port=self.port)
		self.cursor = self.conn.cursor()

		print(f"Bağlantı kuruldu : {self.db_name}")
	

	def close_conn(self):
		self.conn.close()

		print("Bağlantı kesildi.....")


	def create_db(self, new_db):
		self.conn.autocommit = True
		drop_db = """DROP DATABASE IF EXISTS """ + new_db
		self.cursor.execute(drop_db)

		sql = f"""CREATE DATABASE {new_db}"""
		self.cursor.execute(sql)
		self.conn.autocommit = False

		print("Veritabanı başarıyla oluşturuldu.....")


	def create_table(self, new_table, col_name):
		self.conn.autocommit = True
		self.cursor.execute("DROP TABLE IF EXISTS " + new_table)

		collumns = ",".join([f"{i} {j}" for i, j in col_name])
		sql = f"""CREATE TABLE {new_table} ({collumns})"""

		self.cursor.execute(sql)
		self.conn.autocommit = False

		print("Tablo başarı ile oluşturuldu.....")


	def add_data2table(self, table, col_name, s_list, data):
		
		collumn = "INSERT INTO " + table + " " + col_name + " VALUES " + s_list
		self.cursor.executemany(collumn, data)
		self.conn.commit()

		print("Kayıtlar eklendi.....")
		
	

db_name = "deneme_py"
user = ""
password = ""
coll = [("First_name", "VARCHAR(255)"),("Last_Name", "VARCHAR(255)"),("Age","INT"),
    ("Place_Of_Birth" ,"VARCHAR(255)"),
    ("Country", "VARCHAR(255)")]
col_name = "(First_name, Last_name, Age, Country)"
s_list = "(%s, %s, %s, %s)"
val = [("Berke", "TÜRKEN", 23, "Bafra"),("Alperen", "YÜCEL", 24, "Elbistan"),
       ("Melih", "SADİÇ", 24, "Lüleburgaz"), ("İbrahim", "ÇINAR", 23, "Yenişehir")]


db = DB(db_name, user, password)
db.connect_db()

#db.create_db("deneme_py")
db.create_table("kisiler", coll)
db.add_data2table("kisiler", col_name, s_list, val)

db.close_conn()


import pypyodbc as odbc

class SQLServer:
	DRIVER_NAME = 'SQL Server'
	def __init__(self, server_name, database_name):
		self.server_name = server_name
		self.database_name = database_name
	
	def connect_to_sql_server(self):
		try:
			self.conn = odbc.connect(self._connection_string())
			print('Connected')
			return self.conn
		except Exception as e:
			print(e)
			return None
	
	def _connection_string(self):
		conn_string = f"""
			DRIVER={{{self.DRIVER_NAME}}};
			SERVER={self.server_name};
			DATABASE={self.database_name};
			Trust_Connection=yes;        
		"""
		return conn_string

	def query(self, sql_statement):
		if self.conn is None:
			print('Connection is absent')
			return
		try:
			cursor = self.conn.cursor()
			cursor.execute(sql_statement)
			data = cursor.fetchall()
			columns = [col[0] for col in cursor.description]
			return columns, data
		except Exception as e:
			print(e)

	def cursor(self):
		if not self.check_connection():
			print('Connection is absent')
			return
		cursor = self.conn.cursor()
		return cursor
		
	def check_connection(self):
		if self.conn is None:
			return False
		else:
			return True
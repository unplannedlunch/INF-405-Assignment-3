# Jonah Wolmark
# INF 405 Assignment 3

import sqlite3
from sqlite3 import Error

class DBHandler:
	def __init__(self, dbFilePath):
		self.dbFilePath = dbFilePath
	
	def __enter__(self):
		self.connection = self.startDBConnection(self.dbFilePath)
		self.connection.row_factory = lambda cursor, row: row[0] # Why does fetchall default to returning half empty tuples? This changes it to a list of singular values.
		

	def __exit__(self, exception_type, exception_value, traceback):
		self.connection.close()

	def startDBConnection(self, dbFilePath):
		connection = None
		try:
			connection = sqlite3.connect(dbFilePath)
		except Error as err:
			print(err)

		return connection
	
	def createTables(self):
		createStatesTableQuery = '''
									CREATE TABLE IF NOT EXISTS states(
									stateid integer PRIMARY KEY AUTOINCREMENT,
									name text NOT NULL
									);
								'''
		createSchoolsTableQuery = '''
									CREATE TABLE IF NOT EXISTS schools(
									schoolid integer PRIMARY KEY,
									type text,
									immrate real NOT NULL,
									state integer NOT NULL,
									FOREIGN KEY(state) REFERENCES states(stateid)
									);
								'''

		c = self.connection.cursor()
		c.execute(createStatesTableQuery)
		c.execute(createSchoolsTableQuery)
		c.close()
	
	def getStateID(self, statename):
		getStateIDQuery = f'''
								SELECT stateid FROM states WHERE name = "{statename}";
							'''
		c = self.connection.cursor()
		c.execute(getStateIDQuery)
		output = c.fetchall()[0]
		c.close()
		return output

	def addSchool(self, schoolData):

		# fstrings are pretty neat for this.  Probably bad since input isn't really sanitized but whatever, this will never be used in a context where that matters
		insertStateQuery = f'''
								INSERT INTO states(name)
									SELECT ("{schoolData.get('statename')}") WHERE NOT EXISTS (
										SELECT * FROM states WHERE name = "{schoolData.get('statename')}"
									);
							'''

		# This query needs to be two strings, because the state ID will not be known until after it is added to the DB.
		insertSchoolQueryPart1 = f'''
									INSERT OR IGNORE INTO schools(schoolid, type, immrate, state) VALUES ({schoolData.get('id')}, {schoolData.get('type')}, {schoolData.get('immrate')}, 
								'''
		insertSchoolQueryPart2 = f'''
									);
								'''
		c = self.connection.cursor()
		c.execute(insertStateQuery)
		stateID = self.getStateID(schoolData.get('statename'))

		# This should probably be one line, but it's mildly more readable this way.  The [10:-9] on each query part strips out the whitespace introduced in formatting.  There's definitely a better way to do it.
		compositequerystring = insertSchoolQueryPart1[10:-9] + str(stateID) + insertSchoolQueryPart2[10:-9]
		c.execute(compositequerystring)
		c.close()
	
	# There is lots of code duplication in these four methods.  It can be done in a much more concise way, but this is fine.
	def getOverallImmunizationRate(self):
		ratequerystring = 'SELECT immrate FROM schools;'
		countquerystring = 'SELECT COUNT(immrate) FROM schools;'
		c = self.connection.cursor()
		c.execute(ratequerystring)
		rates = c.fetchall()
		c.execute(countquerystring)
		count = c.fetchall()
		c.close()
		if count[0] == 0:
			return None
		return sum(rates) / count[0]

	def getOverallImmunizationRatePerSchoolType(self, schooltype):
		ratequerystring = f'SELECT immrate FROM schools WHERE type IS "{schooltype}";'
		countquerystring = f'SELECT COUNT(immrate) FROM schools WHERE type IS "{schooltype}";'
		c = self.connection.cursor()
		c.execute(ratequerystring)
		rates = c.fetchall()
		c.execute(countquerystring)
		count = c.fetchall()
		c.close()
		if count[0] == 0:
			return None
		return sum(rates) / count[0]
	
	def getStateImmunizationRate(self, statename):
		ratequerystring = f'SELECT immrate FROM schools WHERE state IS "{self.getStateID(statename)}";'
		countquerystring = f'SELECT COUNT(immrate) FROM schools WHERE state IS "{self.getStateID(statename)}";'
		c = self.connection.cursor()
		c.execute(ratequerystring)
		rates = c.fetchall()
		c.execute(countquerystring)
		count = c.fetchall()
		c.close()
		if count[0] == 0:
			return None
		return sum(rates) / count[0]

	def getStateImmunizationRatePerSchoolType(self, statename, schooltype):
		ratequerystring = f'SELECT immrate FROM schools WHERE state IS "{self.getStateID(statename)}" AND type IS "{schooltype}";'
		countquerystring = f'SELECT COUNT(immrate) FROM schools WHERE state IS "{self.getStateID(statename)}" AND type IS "{schooltype}";'
		c = self.connection.cursor()
		c.execute(ratequerystring)
		rates = c.fetchall()
		c.execute(countquerystring)
		count = c.fetchall()
		c.close()
		if count[0] == 0:
			return None
		return sum(rates) / count[0]

	def getAllStates(self):
		c = self.connection.cursor()
		c.execute('SELECT DISTINCT name FROM states')
		output = c.fetchall()
		c.close()
		return output
			
	def getAllSchoolTypes(self):
		c = self.connection.cursor()
		c.execute('SELECT DISTINCT type FROM schools')
		output = c.fetchall()
		c.close()
		return output

	# DEBUG FUNCTIONS --------------------------------------------------------------------------------------------------------------

	# This is pretty self explanatory.
	def resetDB(self):
		c = self.connection.cursor()
		c.execute('DROP TABLE states;')
		c.execute('DROP TABLE schools;')
		c.close()

	# This function tests if the table creation function worked by printing out all columns from each table
	def testTables(self):
		c = self.connection.cursor()
		c.execute('PRAGMA table_info(states);')
		print(c.fetchall())
		c.execute('PRAGMA table_info(schools);')
		print(c.fetchall())
		c.close()

	# This function displays all the contents of each table, in order to determine if the addSchool function is working.
	# Becomes borderline useless when working with the full dataset.
	def testAddSchool(self):
		c = self.connection.cursor()
		c.execute('SELECT * FROM states;')
		print(c.fetchall())
		c.execute('SELECT * FROM schools;')
		print(c.fetchall())
		c.close()
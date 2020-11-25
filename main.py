# Jonah Wolmark
# INF 405 Assignment 3

import csv
import re
import sys
from pathlib import Path
from dbHandler import DBHandler

def populateDBFromFile(dbhandler, filePath):
	with open(filePath, newline = '') as file:
		csvReader = csv.reader(file)
		row_count = len(list(csvReader))
		file.seek(0)
		rowDisplayCount = 0
		idCounter = 0
		for row in csvReader:
			if rowDisplayCount%100 == 0:
				sys.stdout.write('\rConstructing database. Percentage complete: %f' % (100*float(rowDisplayCount)/float(row_count)))
				sys.stdout.flush()
			rowDisplayCount += 1
			# Discards header row
			if(re.search('index', row[0])):
				continue
			
			# Discards rows without usable MMR data
			if(float(row[9]) < 0 or float(row[9]) > 100):
				continue

			# Any school type that is not Public, Charter, or Private will be recorded into the database without a type.
			schoolType = 'NULL'
			if(row[4] == 'Public' or row[4] == 'Charter' or row[4] == 'Private'):
				schoolType = '"' + row[4] + '"'
			
			dbhandler.addSchool({
				'id': idCounter,
				'statename': row[1],
				'type': schoolType,
				'immrate': row[9]
			})

			idCounter += 1
		sys.stdout.write('\rConstructing database. Percentage complete: 100.00000\n')
		sys.stdout.flush()

def main():
	inputFilePath = 'all-measles-rates.csv'
	dbPath = 'measlesData.db'
	dbfile = Path(dbPath)
	dbfile.touch(exist_ok=True)
	dbh = DBHandler(dbPath)

	print('Welcome to my MMR data analysis program.  Please wait while the database is constructed.')

	with dbh:
		#dbh.resetDB()
		dbh.createTables()
		populateDBFromFile(dbh, inputFilePath)

		running = True
		while running:
			userin = input('Please enter "US", "State", "SchoolType", or "Exit": ').lower()
			if userin == 'us':
				print('National overall rate: ' + str(dbh.getOverallImmunizationRate()))
				for schooltype in dbh.getAllSchoolTypes():
					if schooltype == None:
						continue
					print('National ' + schooltype + ' school rate: ' + str(dbh.getOverallImmunizationRatePerSchoolType(schooltype)))
			elif userin == 'state':
				userin = input('Please choose a state: ').lower().title()
				if userin not in dbh.getAllStates():
					print('No data exists for state ' + userin)
				else:
					print(userin + ' overall rate: ' + str(dbh.getStateImmunizationRate(userin)))
					for schooltype in dbh.getAllSchoolTypes():
						if schooltype == None:
							continue
						print(userin + ' ' + schooltype + ' school rate: ' + str(dbh.getStateImmunizationRatePerSchoolType(userin, schooltype)))
			elif userin == 'schooltype':
				userin = input('Please choose a school type: ').lower().capitalize()
				if userin not in dbh.getAllSchoolTypes():
					print('No data exists for school type ' + userin)
				else:
					print('National ' + userin + ' school rate: ' + str(dbh.getOverallImmunizationRatePerSchoolType(userin)))
			elif userin == 'exit':
				running = False
			else:
				print('Unrecognized input.')

		print('Goodbye!')


# I like this aspect of your code formatting style, it feels much better than leaving a large amount of code outside of any function.
if __name__ == '__main__':
	main()
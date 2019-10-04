'''main file running server'''
import sys
from processor import Query_Executer,readfile
import sqlparse

METADATA_PATH = './files/metadata.txt'


def main_process():
	'''takes query and executes it '''
	length = len(sys.argv)
	table_names,table_attr = (readfile(METADATA_PATH))
	try:
		if length >= 2:
			queries = str(sys.argv[1])
			queries = sqlparse.split(queries)
			executer = Query_Executer(table_names,table_attr)
			for each_query in queries:
				if each_query != '':
					executer.execute_query(each_query)
		else:
			sys.exit('Error: No query found')
	except IOError:
		sys.exit('Error: Wrong input')


if __name__ == '__main__':
	main_process()
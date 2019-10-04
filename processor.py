'''code for class Query_Executer and its functions'''

import re
import sys
import csv
FUNCTIONS = ['distinct', 'max', 'sum', 'avg', 'min']


class Query_Executer:
	'''executes a query'''
	def __init__(self,table_names,table_attr):
		'''
		:param metadata : info about tables and its attributes
		'''
		self.table_names = table_names
		self.table_attr= table_attr

	def execute_query(self,query):
		'''
		checks the query and then processes it'''

		# removed trailing space in the query 
		query =  (re.sub(' +', ' ', str(query))).strip()
		if query[len(query)-1]==';':
			query = query[:-1]

        #check table,select errors
		if 'from' not in query.split():
			sys.exit('Syntax Error:No table selected')
		lis = query.split('from')
		if len(lis) > 2 or len(lis) <= 1:
			sys.exit('Syntax Error near from')
		if len(lis)==2 and lis[1]==' ':
			sys.exit('Syntax Error: No table selected')
		if 'select' not in lis[0].lower().split():
			sys.exit('Syntax Error: select not given')
		elif query.lower().count('select')>1:
			sys.exit('Syntax Error: More than one \'select\' given')

		project = (re.sub(' +', ' ', str(lis[0]))).strip()
		remaining = (re.sub(' +', ' ', str(lis[1]))).strip()
		clause = remaining.split('where')
		tables_req = (re.sub(' +', ' ', str(clause[0]))).strip()
		tables_req = tables_req.split(',')
		tables_req_data = {}
		for i in range(0, len(tables_req)):
			if tables_req[i] not in self.table_attr.keys():
				sys.exit('No Such Table \'' + tables_req[i] + '\' Exists')
			tables_req_data[tables_req[i]] = read_table_data('./files/'+tables_req[i])

		required = project[len('select'):]
		required = (re.sub(' +', ' ', str(required))).strip()
		required = required.split(',')

		columns,funct_process,distinct = get_req_from_query(required)

		# errors in selecting columns
		# both distinct and function process cannot be non zero at a time
		if len(funct_process)!=0 and len(distinct)!=0:
			sys.exit('ERROR : distinct and aggregate functions cannot be given at a time')
		if len(columns) + len(funct_process) + len(distinct) < 1:
			sys.exit('ERROR : Nothing given to select')
		if len(clause) > 1 and (len(funct_process) != 0 or len(distinct) != 0):
			sys.exit('ERROR : Where Condition can only be given to project columns')

		self.divide_query_type(columns,funct_process,distinct,tables_req,tables_req_data,clause)

	def divide_query_type(self,columns,funct_process,distinct,tables_req,tables_req_data,clause):
		if len(clause) > 1 and len(tables_req) == 1:
			self.execute_where(clause[1], columns,tables_req[0], tables_req_data[tables_req[0]])
		elif len(clause) > 1 and len(tables_req) > 1:
			self.execute_where_join(clause[1],columns,tables_req,tables_req_data)
		elif len(funct_process)!=0:
			self.execute_aggr(funct_process,tables_req,tables_req_data)
		elif len(distinct)!=0:
			self.execute_dist(distinct,tables_req,tables_req_data)
		elif len(tables_req) > 1:
			self.execute_join(columns, tables_req, tables_req_data)
		else:
			self.execute_project(columns,tables_req[0],tables_req_data)
	def execute_where_join(self,condition,columns,tables,tables_data):
		""" Join type queries with where """
		condition = (re.sub(' +', ' ', str(condition))).strip()
		init_stote = condition
		operations = ['>=','<=','<', '>', '=']
		operator = ''
		if 'and' in condition:
			condition = condition.split('and')
			operator = 'and'
		elif 'or' in condition:
			condition = condition.split('or')
			operator = 'or'
		else:
			condition = [condition]
		#print(condition)
		if len(condition) > 2:
			sys.exit('Maximum one AND or one OR clause can be given')
		condition1 = condition[0]
		for i in operations:
			if i in condition1:
				condition1 = condition1.split(i)
		if len(condition1) == 2 and '.' in condition1[1]:
			self.execute_where_join1([condition, operator], columns,tables, tables_data)
		else:
			self.execute_where_join2(init_stote, columns, tables,tables_data)
	def execute_where_join1(self, clauses, columns, tables,tables_data):
		""" executes where with join condition"""
		need_data = {}
		remove_data = {}
		operations = ['<=','>=','<', '>', '=']
		for condition in clauses[0]:
		    needed = []
		    operator = ''
		    condition = (re.sub(' +', ' ', str(condition))).strip()
		    for i in operations:
		        if i in condition:
		            needed = condition.split(i)
		            #print(needed,i)
		            operator = i
		            if operator == '=':
	                	operator = '=='
		            break
		    if len(needed) > 2:
	        	sys.exit('Error in where clause')
		    columns_condition, tables_condition = self.get_tables_columns(needed, tables)
		    table1 = tables[0]
		    table2 = tables[1]

		    checking_errors_in_c_attr(columns_condition[table1][0],self.table_attr[table1], table1)
		    checking_errors_in_c_attr(columns_condition[table2][0],self.table_attr[table2], table2)
		    column1 = self.table_attr[table1].index(columns_condition[table1][0])
		    column2 = self.table_attr[table2].index(columns_condition[table2][0])

		    remove_data[condition] = []
		    need_data[condition] = []
		    for data in tables_data[table1]:
		        for row in tables_data[table2]:
		            evaluator = data[column1] + operator + row[column2]
		            if eval(evaluator):
	                	need_data[condition].append(data + row)
		            else:
	                	remove_data[condition].append(data + row)
		if clauses[1] != '':
			join_data = join_needed_data(clauses[1],clauses[0], need_data, remove_data)
		else:
			join_data = []
			for key in need_data.keys():
				for data in need_data[key]:
					join_data.append(data)
		columns, tables = self.get_tables_columns(columns, tables)
		self.display(tables, columns, self.table_attr, join_data, True)
	def execute_where_join2(self, sentence, columns, tables,tables_data):
		"""Process the special case of where"""
		operator = ''
		if 'and' in sentence.lower().split():
		    operator = 'and'
		    condition = sentence.split('and')
		elif 'or' in sentence.lower():
		    operator = 'or'
		    condition = sentence.split('or')
		else:
			condition = [sentence]
		#	print(condition)
		need_data = self.get_needed_data(condition, tables, tables_data)
		columns_in_table, tables_need = self.get_tables_columns(columns,tables)
		join_data = join_needed_data(operator, tables_need, need_data,tables_data)
		self.display(tables_need, columns_in_table, self.table_attr,join_data,True)
	def get_needed_data(self, condition, tables, tables_data):
		""" Gets needed data for where clause"""
		operators = ['<', '>', '=','>=','<=']
		needed_data = {}
		for query in condition:
			needed = []
			for operator in operators:
				if operator in query:
					needed = query.split(operator)
					break

			#check error in where clause
			if len(needed) != 2:
				sys.exit('Syntax error in where clause')

			table, column = self.search_for_column(re.sub(' +', ' ', str(needed[0])).strip(), tables)
			needed_data[table] = []
			query = query.replace(needed[0], ' ' + column + ' ')
			for data in tables_data[table]:
				evaluator = self.make_evaluator(query, table, data)
				if eval(evaluator):
					#print((evaluator))
					needed_data[table].append(data)
		#print(needed_data)
		return needed_data
	def get_tables_columns(self, columns, tables):
		""" Selects required tables and columns in it"""
		columns_in_table = {}
		tables_needed = []
		if len(columns) == 1 and columns[0] == '*':
			for table in tables:
				columns_in_table[table] = []
				for column in self.table_attr[table]:
					columns_in_table[table].append(column)
			return columns_in_table, tables

		for column in columns:
			table, column = self.search_for_column(column, tables)
			if table not in columns_in_table.keys():
				columns_in_table[table] = []
				tables_needed.append(table)
			columns_in_table[table].append(column)
		return columns_in_table, tables_needed

	def execute_where(self,clause,columns,table,table_req_data):
		""" Process where clause on a single table"""
		condition = (re.sub(' +', ' ', str(clause))).strip()
		if len(columns) == 1 and columns[0] == '*':
			columns = self.table_attr[table]
		print(self.make_heading(table, columns))
		for row in table_req_data:
			evaluator = self.make_evaluator(condition, table,row)
			ans = ''
			if eval(evaluator):
				for column in columns:
					if '.' in column:
						column = column.split('.')[1]
					if column not in self.table_attr[table]:
						sys.exit('No Such column \'' + column + '\' found')
					ans += row[self.table_attr[table].index(column)] + ','
				print(ans.strip(','))
	def make_evaluator(self,condition,table,data):
		"""Generates the evaluator string for single table where"""
		condition = condition.split(' ')
		evaluator = ''
		for i in condition:
			i = (re.sub(' +', ' ', str(i))).strip()
			if i == '=':
				evaluator += '=='
			elif i.lower() == 'and' or i.lower() == 'or':
				evaluator += ' ' + i.lower() + ' '
			elif '.' in i:
				table_here, column = self.search_for_column(i, [table])
				#check_errors_in_condition
				if table_here != table:
					sys.exit('Unknown table \'' + table_here + '\' given')
				elif column not in self.table_attr[table]:
					sys.exit('No Such column \'' + column + '\' found in \'' + table_here + '\' given')
				evaluator += data[self.table_attr[table_here].index(column)]
			elif i in self.table_attr[table]:
				evaluator += data[self.table_attr[table].index(i)]
			else:
				evaluator += i
		return evaluator

	def execute_project(self, columns, tables, tables_data):
		""" Deals with project operation without in a single table"""
		if len(columns) == 1 and columns[0] == '*':
			columns=self.table_attr[tables]	
		for column in columns:
			if '.' in column:
					column = column.split('.')[1]
			if column not in self.table_attr[tables]:
				sys.exit('No Such column \'' + column + '\' found in the given table \'' + tables + '\' ')
		print(self.make_heading(tables, columns))
		for data in tables_data[tables]:
			ans = ''
			for column in columns:
				if '.' in column:
					column = column.split('.')[1]
				ans += data[self.table_attr[tables].index(column)] + ','
			print(ans.strip(','))
	def make_heading(self,table_name,columns):
		"""Prints the header of the columns needed"""
		string = ''
		c=''
		for column in columns:
			if '.' in column:
				c = column.split('.')
				column=c[1]
				if c[0]==table_name:
					if string != '':
						string += ','
					string += table_name + '.' + str(column)
				else:
					sys.exit('attribute not present in table given')
			else:
				if string != '':
					string += ','
				string += table_name + '.' + str(column)

		return string 
	def execute_dist(self,distinct,tables,tables_data):
		column_data = {}
		max_len = 0
		heading = ''
		for column in distinct:
			table, column = self.search_for_column(column, tables)
			heading += table + '.' + column + ','
			data = []
			for row in tables_data[table]:
				if '.' in column:
					column = column.split('.')[1]
				value = row[self.table_attr[table].index(column)]
				if value not in data:
					data.append(value)
			column_data[column] = data
			max_len = max(max_len, len(tables_data[table]))
		print(heading.strip(','))
		for i in range(max_len):
			ans = ''
			for column in column_data:
				if '.' in column:
					column = column.split('.')[1]
				if i < len(column_data[column]):
					ans += column_data[column][i] + ','
				else:
					ans += ','
			print(ans.strip(','))
	def execute_aggr(self,funct_process,tables,tables_data):
		heading, result = '', ''
		for query in funct_process:
			function_req = query[0]
			column_name = query[1]
			table_name, column = self.search_for_column(column_name,tables)
			data = []
			heading += table_name + '.' + column + ','
			for row in tables_data[table_name]:
				data.append(int(row[self.table_attr[table_name].index(column)]))

			if function_req.lower() == 'max':
				result += str(max(data))
			elif function_req.lower() == 'min':
				result += str(min(data))
			elif function_req.lower() == 'sum':
				result += str(sum(data))
			elif function_req.lower() == 'avg':
				result += str(float(sum(data)) / len(data))
			result += ','
			heading.strip(',')
		print(heading)
		print(result)
	def search_for_column(self, column, tables):
		"""Searches for column in list of tables"""
		if '.' in column:
			table, column = column.split('.')
			table = (re.sub(' +', ' ', str(table))).strip()
			column = (re.sub(' +', ' ', str(column))).strip()
			if table not in tables:
				sys.exit('No Such table \'' + table + '\' exists')
			return table, column
		cnt = 0
		table_needed = ''
		for table in tables:
			if column in self.table_attr[table]:
				cnt += 1
				table_needed = table
		if cnt > 1:
			sys.exit('Ambigous column name \'' + column + '\' given')
		elif cnt == 0:
			sys.exit('No Such Column \'' + column + '\' found')
		return table_needed, column
	def display(self,tables_needed, columns_in_table, table_info, tables_data, join):
		""" Displays the output for a join operation without `where` clause"""
		if join:
			table1 = tables_needed[0]
			table2 = tables_needed[1]
			header1 = self.make_heading(table1, columns_in_table[table1])
			header2 = self.make_heading(table2, columns_in_table[table2])
			print(header1 + ',' + header2)
			ans =''
			for item in tables_data:
				for column in columns_in_table[table1]:
					ans += item[table_info[table1].index(column)] + ','
				for column in columns_in_table[table2]:
					ans += item[table_info[table2].index(column) +len(table_info[table1])] + ','
				ans += ';'
			sol=ans.split(',;')[::-1][1:]
			sol = list(dict.fromkeys(sol))
			for i in sol:
				print(i,end='\n')
		else:
			ans =''
			for table in tables_needed:
				print(make_heading(table, columns_in_table[table]))
				for data in tables_data[table]:
					for column in columns_in_table[table]:
						ans += data[table_info[table].index(column)] + ','
					ans += ';'
					sol=ans.split(',;')[::-1][1:]
					sol = list(dict.fromkeys(sol))
					for i in sol:
						print(i,end='\n')
				print('\n')
	def execute_join(self, columns, tables, tables_data):
		"""Deals with Join type queries"""
		columns_in_table, tables_needed = self.get_tables_columns(columns, tables)
		join_data = []
		if len(tables_needed) == 2:
			table1 = tables_needed[0]
			table2 = tables_needed[1]
			for item1 in tables_data[table1]:
				for item2 in tables_data[table2]:
					join_data.append(item1 + item2)
			self.display(tables_needed, columns_in_table, self.table_attr,join_data, join=True)
		else:
			self.display(tables_needed, columns_in_table, self.table_attr,tables_data, join=False)


def get_req_from_query(required):
	columns = []
	funct_process = []
	distinct = []
	for i in required:
		included = False
		i = (re.sub(' +', ' ', str(i))).strip()
		for func in FUNCTIONS:
			if func + '(' in i.lower():
				if ')' not in i:
					sys.exit('Syntax Error: near \'(\'')
				included = True
				column_name = i.strip(')').split(func+'(')[1]
				if func == 'distinct':
					distinct.append(column_name)
				else:
					funct_process.append([func,column_name])
				break
		if not included:
			if i != ' ':
				columns.append(i.strip('()'))
	return columns,funct_process,distinct
def join_and(tables, needed_data):
	""" Joins the data if AND operator in condition"""
	final_data = []
	table1 = (re.sub(' +', ' ', str(tables[0]))).strip()
	table2 = (re.sub(' +', ' ', str(tables[1]))).strip()
	for item1 in needed_data[table1]:
	    for item2 in needed_data[table2]:
        	final_data.append(item1 + item2)
	return final_data


def join_or(tables, needed_data, tables_data):
	""" Joins the data if OR operator in condition"""
	final_data = []
	table1 = (re.sub(' +', ' ', str(tables[0]))).strip()
	table2 = (re.sub(' +', ' ', str(tables[1]))).strip()
	for item1 in needed_data[table1]:
		for item2 in tables_data[table2]:
			if item2 not in needed_data[table2]:
				final_data.append(item1 + item2)
	for item1 in needed_data[table2]:
		for item2 in tables_data[table1]:
			if item2 not in needed_data[table1]:
				final_data.append(item2 + item1)
	return final_data

def join_needed_data(oper, tables, needed_data, tables_data):
	""" Joins the data needed for where clause"""
	if oper == 'and':
		return join_and(tables, needed_data)
	elif oper == 'or':
		return join_or(tables, needed_data, tables_data)
	else:
		final_data = []
		table1 = next(iter(needed_data))
		flag = False
		table2 = tables[1]
		if table1 == tables[1]:
		    table2 = tables[0]
		    flag = True
		for item1 in needed_data[table1]:
		    for item2 in tables_data[table2]:
		        if not flag:
		            final_data.append(item2 + item1)
		            continue
		        final_data.append(item1 + item2)
		return final_data

def checking_errors_in_c_attr(column, column_list, table_name):
	""" Check for columns in the table_name """
	if column not in column_list:
		sys.exit('No Such column \'' + column + '\' in table \'' + table_name + '\'')
def readfile(filename):
	'''reads table name and attributes'''
	try:
		file = open(filename,'r')
		table_names = []
		table_attr = {}
		do = False
		name = ' '
		for each_line in file:
			each_line = each_line.strip()
			if each_line == '<begin_table>':
				do = True
			elif do == True:
				name = each_line
				table_names.append(name)
				table_attr[name] = []
				do = False
			elif do == False and each_line != '<end_table>':
				table_attr[name].append(each_line)
		return table_names,table_attr

	except IOError:
		sys.exit('Error: NO metadata file can be accessed: Please check')
    

def read_table_data(table_name):
    """ Reads the csv file data and returns it as a list"""
    data = []
    file_name = table_name + '.csv'
    try:
        data_file = open(file_name, 'r')
        reader = csv.reader(data_file)
        for row in reader:
            data.append(row)
        data_file.close()
    except IOError:
        sys.exit('No file for given table: \'' + table_name + '\' found')
    return data
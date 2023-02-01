import sqlite3
import pandas as pd
import jaydebeapi
import openpyxl
import numpy as np
import os 
import shutil




conn = jaydebeapi.connect(
 'oracle.jdbc.driver.OracleDriver',
 'jdbc:oracle:thin:de1h/bilbobaggins@de-oracle.chronosavant.ru:1521/deoracle',
 ['de1h','bilbobaggins'],
 'ojdbc7.jar')

curs = conn.cursor()


def drop_table_tmp(table_name):

	'''функция удаления таблицы'''

	try:
		curs.execute(f'DROP TABLE {table_name}')
		print(f'++++++++\nТаблица {table_name} удалена \n+++++++++')
	except jaydebeapi.DatabaseError:
		print(f'========\nТаблица {table_name} уже! удалена \n========')


def show_table(table_name):

	'''функция отображения таблицы в терминале.'''

	print('_-'*20) 
	print(table_name)
	print('_-'*20)
	curs.execute(f'select * from {table_name}')
	for row in curs.fetchmany(10):
		print(row)
 	
	print('_-'*20+'\n')


def csv2sql(path_to_file):

	'''функция преобразования данных из txt, scv файла '''

	df = pd.read_csv(path_to_file, delimiter=';')
	print(df.head())
	df_list = df.values.tolist()
	return df_list


def xlsx2sql(path_to_file):

	'''функция преобразования данных из excele файла passport '''

	df = pd.read_excel(path_to_file, index_col=0)
	df.to_csv('passfile.csv')
	df = pd.read_csv('passfile.csv')
	print(df.head())
	df_list = df.values.tolist()
	return df_list
	


def xlsxterm2sql(path_to_file):

	'''функция преобразования данных из excele файла terminals '''

	df = pd.read_excel(path_to_file)

	print(df.head())
	df_list = df.values.tolist()
	return df_list


def create_table_STG_TRMNLS():

	'''функция создания таблицы de1h.S_07_STG_TRMNLS'''

	try:
		curs.execute('''
			CREATE TABLE de1h.S_07_STG_TRMNLS (
				
					TERMINAL_ID varchar(128),
					TERMINAL_TYPE varchar(128),
					TERMINAL_CITY varchar(128),
					TERMINAL_ADDRESS varchar(128))''')
		print('[+] Таблица de1h.S_07_STG_TRMNLS создана.')
	except jaydebeapi.DatabaseError:
		print('[=] Такая таблица существует.')


	
def add_table_STG_TRMNLS(list_row):

	'''функция обновления записей в таблицу de1h.S_07_STG_TRMNLS '''
	# curs.execute('''UPDATE de1h.S_07_STG_TRNSCTN_TMP
	# 		SET UPDATE_DT = SYSDATE
	# 		WHERE UPDATE_DT = TO_TIMESTAMP('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
	# 		'''),
	curs.executemany('''INSERT INTO de1h.S_07_STG_TRMNLS
          (TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS)
          VALUES(?, ?, ?, ?)''', list_row)


def add_STG_in_DWH_DIM_TRMNLS():

	'''функция добавления строк из временной таблицы в таблицу de1h.S_07_DWH_TRNSCTN'''
	
	curs.execute('''INSERT INTO de1h.S_07_DWH_DIM_TRMNLS ( 
		TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY, TERMINAL_ADDRESS) 
		SELECT * FROM de1h.S_07_DWH_DIM_TRMNLS
		UNION 
		SELECT * FROM de1h.S_07_STG_TRMNLS
		''')



def create_table_DWH_DIM_TRMNLS():

	'''функция создания таблицы de1h.S_07_DWH_DIM_TRMNLS_HIST'''

	try:
		curs.execute('''
			CREATE TABLE de1h.S_07_DWH_DIM_TRMNLS (
					TERMINAL_ID varchar(128),
					TERMINAL_TYPE varchar(128),
					TERMINAL_CITY varchar(128),
					TERMINAL_ADDRESS varchar(128))
					-- DELETED_FLG integer check(deleted_flg in (0, 1)),
					-- CREATE_DT TIMESTAMP default sysdate,
					-- UPDATE_DT TIMESTAMP default TO_TIMESTAMP('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
					-- ) 
				''')
		print('[+] Таблица создана.')
	except jaydebeapi.DatabaseError:
		print('[=] Такая таблица существует.')


########################################################################################################

def create_table_report():

	'''функция создания таблицы отчета'''

	try:
		curs.execute('''
			CREATE TABLE de1h.S_07_REP_FRAUD (
					EVENT_DT TIMESTAMP,
					PASSPORT varchar(128),
					FIO varchar(128),
					PHONE varchar(128),
					EVENT_TYPE varchar(128)
					)
				''')
		print('[+] Таблица REP_FRAUD создана.')
	except jaydebeapi.DatabaseError:
		print('[=] Такая таблица REP_FRAUD  существует.')


def add_to_table_report():

	'''функция поиска мошенников и составления отчета'''

	curs.execute('''INSERT INTO de1h.S_07_REP_FRAUD (
			EVENT_DT,
			PASSPORT,
			FIO,
			PHONE,
			EVENT_TYPE)
			SELECT t4.TRANSACTION_DATE,
 					t1.passport_num,
 					t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic as FIO,
 					t1.phone,
 					'Совершение операции при просроченном или заблокированном паспорте.' as EVENT_TYPE 
 			FROM BANK.clients t1
 			inner join  BANK.accounts t2
 			on t1.client_id = t2.client
 			inner join BANK.cards t3
 			on t2.account = t3.account
 			inner join de1h.S_07_DWH_FACT_TRNSCTN t4
 			on TRIM(t3.card_num) = t4.CARD_NUM
 			WHERE t4.TRANSACTION_DATE > t1.passport_valid_to
 			or t1.passport_num in (SELECT passport_num FROM de1h.S_07_DWH_FACT_PSSPRT_BLCKLST)
 			ORDER BY FIO

		''')
# curs.execute('''
# 				SELECT t4.TRANSACTION_DATE,
# 									t1.passport_num,
# 									t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic as FIO,
# 									t1.phone 
# 							FROM BANK.clients t1
# 							inner join  BANK.accounts t2
# 							on t1.client_id = t2.client
# 							inner join BANK.cards t3
# 							on t2.account = t3.account
# 							inner join de1h.S_07_DWH_FACT_TRNSCTN t4
# 							on TRIM(t3.card_num) = t4.CARD_NUM
# 							WHERE t4.TRANSACTION_DATE > t1.passport_valid_to
# 							or t1.passport_num in (SELECT passport_num FROM de1h.S_07_DWH_FACT_PSSPRT_BLCKLST)
# 							ORDER BY FIO
			
# 	''')
# for row in curs.fetchmany(10):
# 	print(row)
#######################################################################################################


def create_table_dwh_FACT_TRNSCTN():

	"""функция создания таблицы  и представления  de1h.S_07_DWH_TRNSCTN """

	try:
		curs.execute('''
			CREATE TABLE de1h.S_07_DWH_FACT_TRNSCTN (
					TRANSACTION_ID integer,
					TRANSACTION_DATE TIMESTAMP, 
					AMOUNT number (30, 2),
					CARD_NUM varchar(128),
					OPER_TYPE varchar(128),
					OPER_RESULT varchar(128),
					TERMINAL varchar(128),
					CREATE_DT TIMESTAMP default sysdate,
					UPDATE_DT TIMESTAMP default TO_TIMESTAMP('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
					) 
				''')
		print(f'[+] Таблица  создана.')
	except jaydebeapi.DatabaseError:
		print('[+] Такая таблица существует.')


	# try:
	# 	curs.execute('''
	# 		CREATE VIEW V_de1h.S_07_DWH_TRNSCTN AS
	# 		        SELECT 
	# 				TRANSACTION_ID,
	# 				TRANSACTION_DATE, 
	# 				AMOUNT,
	# 				CARD_NUM,
	# 	            OPER_TYPE,
	# 				OPER_RESULT,
	# 				TERMINAL,
	# 				CREATE_DT,
	# 				UPDATE_DT 
	# 				FROM de1h.S_07_DWH_TRNSCTN
	# 				WHERE current_timestamp between CREATE_DT and UPDATE_DT
	# 			''')
	# 	print(f'[view] Таблица представлений  создана.')
	# except jaydebeapi.DatabaseError:
	# 	print('[view] Такая таблица  представлений  существует.')


def create_table_STG_TRNSCTN():

	'''функция создания таблицы de1h.S_07_STG_TRNSCTN_TMP'''

	try:
		curs.execute('''
			CREATE TABLE de1h.S_07_STG_TRNSCTN_TMP (
					TRANSACTION_ID integer,
					TRANSACTION_DATE TIMESTAMP, 
					AMOUNT number (30, 2),
					CARD_NUM varchar(128),
					OPER_TYPE varchar(128),
					OPER_RESULT varchar(128),
					TERMINAL varchar(128),
					CREATE_DT TIMESTAMP default sysdate,
					UPDATE_DT TIMESTAMP default TO_TIMESTAMP('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
					) 
				''')
		print('[+] Таблица TMP создана.')
	except jaydebeapi.DatabaseError:
		print('[=] Такая таблица TMP существует.')


def create_table_new_row():
	curs.execute('''
		CREATE TABLE de1h.S_07_STG_NEW_ROW_TRNSCTN_TMP AS
			SELECT
			    T1.TRANSACTION_ID,
				T1.TRANSACTION_DATE,
				T1.AMOUNT,
				T1.CARD_NUM,
				T1.OPER_TYPE,
				T1.OPER_RESULT,
				T1.TERMINAL,
				T1.CREATE_DT,
				T1.UPDATE_DT
			FROM de1h.S_07_STG_TRNSCTN_TMP  T1
			LEFT JOIN de1h.S_07_DWH_TRNSCTN  T2
			ON T1.TRANSACTION_ID = T2.TRANSACTION_ID
			WHERE T2.TRANSACTION_ID IS NULl
			''')
	

def add_table_STG_TRNSCTN_TMP(list_row):

	'''функция добавления  записей в таблицу UPDATE de1h.S_07_STG_TRNSCTN_TMP '''

	curs.execute('''UPDATE de1h.S_07_STG_TRNSCTN_TMP
		SET UPDATE_DT = SYSDATE
		WHERE UPDATE_DT = TO_TIMESTAMP('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
		'''),

	curs.executemany('''INSERT INTO de1h.S_07_STG_TRNSCTN_TMP (
		TRANSACTION_ID, 
		TRANSACTION_DATE, 
		AMOUNT, 
		CARD_NUM, 
		OPER_TYPE, 
		OPER_RESULT, 
		TERMINAL 
		-- CREATE_DT, 
		-- UPDATE_DT
		)
		VALUES(?, TO_DATE(?, 'yyyy/mm/dd hh24:mi:ss'), ?, ?, ?, ?, ?)''', list_row)



def add_tmp_in_FACT_TRNSCTN():

	'''функция добавления строк из временной таблицы в таблицу de1h.S_07_DWH_TRNSCTN'''
	
	curs.execute('''INSERT INTO de1h.S_07_DWH_FACT_TRNSCTN ( 
		TRANSACTION_ID, 
		TRANSACTION_DATE, 
		AMOUNT, 
		CARD_NUM, 
		OPER_TYPE, 
		OPER_RESULT, 
		TERMINAL, 
		CREATE_DT, 
		UPDATE_DT
		) SELECT * FROM de1h.S_07_DWH_FACT_TRNSCTN
		UNION 
		SELECT * FROM de1h.S_07_STG_TRNSCTN_TMP
		''')

###########################################################################################################
	
def passport_blacklist_tmp():

	'''функция создания таблицы de1h.S_07_STG_PSSPRT_BLCKLST_TMP'''

	try:
		curs.execute('''
			CREATE TABLE de1h.S_07_STG_PSSPRT_BLCKLST_TMP (
				
					ENTRY_DT TIMESTAMP, 
					PASSPORT_NUM varchar(128)) 
				''')
		print('[+] Таблица BLCKLST_TMP создана.')
	except jaydebeapi.DatabaseError:
		print('[=] Такая таблица BLCKLST_TMP существует.')


def addTableBlacklistTmp(list_row):

	'''функция добавления записей в таблицу de1h.S_07_STG_PSSPRT_BLCKLST_TMP '''

	curs.executemany('''INSERT INTO de1h.S_07_STG_PSSPRT_BLCKLST_TMP (
					 
					ENTRY_DT,
					PASSPORT_NUM)
					VALUES(TO_DATE(?, 'yyyy/mm/dd'), ?)''', list_row)


def passport_blacklist_fact():

	'''функция создания таблицы de1h.S_07_DWH_FACT_PSSPRT_BLCKLST'''

	try:
		curs.execute('''
			CREATE TABLE de1h.S_07_DWH_FACT_PSSPRT_BLCKLST (
				
					ENTRY_DT TIMESTAMP, 
					PASSPORT_NUM varchar(128)) 
				''')
		print('[+] Таблица BLCKLST_DWH создана.')
	except jaydebeapi.DatabaseError:
		print('[=] Такая таблица BLCKLST_DWH существует.')


def add_tmp_in_blacklist_hist():

	'''функция добавления строк из временной таблицы в таблицу de1h.S_07_DWH_FACT_PSSPRT_BLCKLST'''
	
	curs.execute('''INSERT INTO de1h.S_07_DWH_FACT_PSSPRT_BLCKLST ( 
		ENTRY_DT,
		PASSPORT_NUM
		) SELECT * FROM de1h.S_07_DWH_FACT_PSSPRT_BLCKLST
		UNION 
		SELECT * FROM de1h.S_07_STG_PSSPRT_BLCKLST_TMP
		''')

###########################################################################################

def mkarchive():

	'''функия создающая папку для архивации файлов(источников данных)'''

	if not os.path.exists('archive'):
		os.mkdir('archive')

	sours = os.path.abspath('store_1')
	archive = os.path.abspath('archive')

	for file in os.listdir(sours):
		shutil.move(os.path.join(sours, file), archive)

	for file in os.listdir(archive):
		os.rename(os.path.join(archive, file),  os.path.join(archive, file.replace('txt', 'txt.backup')))

	for file in os.listdir(archive):
		os.rename(os.path.join(archive, file),  os.path.join(archive, file.replace('xlsx', 'xlsx.backup')))
		
	for file in os.listdir(archive):
		os.rename(os.path.join(archive, file),  os.path.join(archive, file.replace('csv', 'scv.backup')))


###########################################################################################




# drop_table_tmp('de1h.S_07_REP_FRAUD')
# drop_table_tmp('de1h.S_07_DWH_TRNSCTN')
# drop_table_tmp('de1h.S_07_DWH_DIM_TRMNLS')

drop_table_tmp('de1h.S_07_STG_TRNSCTN_TMP')
drop_table_tmp('de1h.S_07_STG_PSSPRT_BLCKLST_TMP')
drop_table_tmp('de1h.S_07_STG_TRMNLS')

# curs.execute('''DELETE de1h.S_07_STG_TRMNLS''')

xlsx_term1 = xlsxterm2sql('store_1/terminals_01032021.xlsx')
xlsx_term2 = xlsxterm2sql('store_1/terminals_02032021.xlsx')
xlsx_term3 = xlsxterm2sql('store_1/terminals_03032021.xlsx')

# print(xlsx_term1)




create_table_STG_TRMNLS()
# create_table_DWH_DIM_TRMNLS()
add_table_STG_TRMNLS(xlsx_term1)
add_STG_in_DWH_DIM_TRMNLS()
# show_table('de1h.S_07_STG_TRMNLS')
# show_table('de1h.S_07_DWH_DIM_TRMNLS')


list1 = csv2sql('store_1/transactions_01032021.txt')
list2 = csv2sql('C:/de_lesson/S_07/store_1/transactions_02032021.txt')
list3 = csv2sql('C:/de_lesson/S_07/store_1/transactions_03032021.txt')

# print(list)

create_table_STG_TRNSCTN()
# create_table_dwh_FACT_TRNSCTN()
add_table_STG_TRNSCTN_TMP(list3)
add_tmp_in_FACT_TRNSCTN()
# show_table('de1h.S_07_STG_TRNSCTN_TMP')
# show_table('de1h.S_07_DWH_FACT_TRNSCTN')


xlsx_pass = xlsx2sql('C:/de_lesson/S_07/store_1/passport_blacklist_01032021.xlsx')
xlsx_pass2 = xlsx2sql('C:/de_lesson/S_07/store_1/passport_blacklist_02032021.xlsx')
xlsx_pass3 = xlsx2sql('C:/de_lesson/S_07/store_1/passport_blacklist_03032021.xlsx')

# print(xlsx_pass2)

passport_blacklist_tmp()
addTableBlacklistTmp(xlsx_pass)
# show_table('de1h.S_07_STG_PSSPRT_BLCKLST_TMP')
# passport_blacklist_fact()
add_tmp_in_blacklist_hist()
# show_table('de1h.S_07_DWH_FACT_PSSPRT_BLCKLST')


# create_table_report()
add_to_table_report()
# show_table('de1h.S_07_REP_FRAUD')


mkarchive()







# curs.execute('''select * from de1h.S_07_STG_TRNSCTN_TMP
# 	WHERE update_dt <> TO_TIMESTAMP('2999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
# 	''')
# for row in curs.fetchmany(10):
# 	print(row)

# def queri_terminals():
# 	curs.execute(''' SELECT * FROM bank.accounts ''')
# 	for row in curs.fetchmany(10):
# 		print(row)
		
# # queri_terminals()

# def query_bank_clients():
	
# 	curs.execute(''' SELECT * FROM bank.clients ''')
# 	for row in curs.fetchmany(10):
# 		print(row)
# # query_bank_clients()



# curs.execute(''' SELECT last_name ||' '|| first_name ||' ' ||patronymic, phone 
# 				FROM bank.clients ''')
# for row in curs.fetchmany(5):
# 	print(row)



# curs.execute('''select * 
#                    from bank.accounts''')
# print([t[0] for t in curs.description])
# for row in curs.fetchmany(5):
#     print(row)


# curs.execute('''select * 
#                    from bank.clients''')
# print([t[0] for t in curs.description])
# for row in curs.fetchmany(5):
#     print(row)


# curs.execute('''select * 
#                    from bank.cards''')
# print([t[0] for t in curs.description])
# for row in curs.fetchmany(5):
#     print(row)


# curs.execute('''select * from de1h.S_07_STG_TRMNLS ''')
# print([t[0] for t in curs.description])
# for row in curs.fetchmany(5):
# 	print(row)
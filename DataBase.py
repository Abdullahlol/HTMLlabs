import logging
import sqlite3
import datetime
import datetime
from urllib.request import urlopen
from urllib.parse import quote_plus
#$sqlite3 MyDB.db

sqlite3.connect('MyDB.db')
def get_interval(name, date_start, date_end): #запрос данных в заданном интервале

    try:
        connection=sqlite3.connect('C:\Downloads\MyBD.db') #создаем БД, если такой не имелось ранее
        print ("Created successfully")
        cursor=connection.cursor()  #создаем курсор для БД
        cursor.execute( 'CREATE TABLE IF NOT EXISTS "DATA"' \
                            ' (stock TEXT, date DATE UNIQUE, open REAL, close REAL, high REAL, low REAL, volume INTEGER)') #создаем таблицу с перечисленными полями

        cursor.execute('SELECT * FROM DATA WHERE  stock="{}"  AND  date<=? AND date>=? order by date'.format(name), (date_end, date_start)) #выборка данных из табл, удовлетворяющих введенным условиям + сортируем по дате

        res = cursor.fetchall()  #извлекаем все данные из курсора
        return(res)
        connection.commit() #фиксируем транзакцию
        connection.close() #прерываем соединение
    except Exception as e:
        print(e)
        raise e

def get_all():#запрос всех данных, содержащихся в БД

    try:
        connection=sqlite3.connect(r'MyBD.db') #создаем БД, если такой не имелось ранее
        print("Created successfully")
        cursor=connection.cursor() #создаем курсор
        cursor.execute( 'CREATE TABLE IF NOT EXISTS "DATA"' \
                            ' (stock TEXT, date DATE UNIQUE, open REAL, close REAL, high REAL, low REAL, volume INTEGER)') #создаем таблицу
        cursor.execute('SELECT * FROM DATA order by date') #выбираем все значения + сортируем по дате

        res = cursor.fetchall() #извлекаем все данные из курсора
        return(res)
        connection.commit() #фиксируем транзакцию
        connection.close() #прерываем соединение
    except Exception as e:
        print(e)
        raise e

def delete_DB(name, date_start, date_end):#удаляем инфо из БД за выделенный интервал

    try:
            connection=sqlite3.connect('MyDB.db') #создаем БД, если ранее такой не имелось
            cursor=connection.cursor() #создаем курсор
            cursor.execute( 'CREATE TABLE IF NOT EXISTS "DATA"' \
                                ' (stock TEXT, date DATE UNIQUE, open REAL, close REAL, high REAL, low REAL, volume INTEGER)') #создаем таблицу
            cursor.execute('DELETE  FROM DATA WHERE  stock="{}"  AND  date<=? AND date>=?'.format(name), (date_end, date_start)) #удаляем данные из таблицы, удовлетворяющие условиям

            res = cursor.fetchall() #извлекаем данные из курсора
            connection.commit() #фиксируем транзакцию
            connection.close() #прерываем соединение
    except Exception as e:
            print(e)
            raise e

def insert_in_DB(data_list): # вставка данных в БД, аргумент - данные из интернета
    try:
        connection=sqlite3.connect('MyDB.db') #создаем БД, если ранее такой не имелось
        cursor=connection.cursor() #создаем курсор
        cursor.execute( 'CREATE TABLE IF NOT EXISTS "DATA"' \
                            ' (stock TEXT, date DATE UNIQUE, open REAL, close REAL, high REAL, low REAL, volume INTEGER)')  #создаем таблицу

        res = cursor.fetchall()
        for item in data_list:
            try: #вставка данных в БД
                cursor.execute(' INSERT INTO DATA  (stock, date, open, close, high, low, volume ) VALUES (?, ?, ?, ?, ?, ?, ? )', (item['stock'], item['date'].date(),  item['open'], item['close'], item['high'], item['low'], item['volume']))
            except:
                pass

        connection.commit() #фиксируем транзакцию
        connection.close()  #прерываем соединение
    except Exception as e:
        print(e)
        raise e

def read_symbol(symbol, start, end): #считываем название компании на торгах
    url = "http://www.google.com/finance/historical?q={0}&startdate={1}&enddate={2}&output=csv"
    url = url.format(symbol.upper(), quote_plus(start.strftime('%b %d, %Y')), quote_plus(end.strftime('%b %d, %Y'))) #подстановка вместо {0},{1},{2}
    logging.debug(url)
    data = urlopen(url).readlines() #открываем получившийся URL в виде таблицы с указанными датами и названием компании
    result=[]
    dict={}
    for line in data[1:]:
        values =  line.decode().strip().split(',') #удаляем лишние пробелы, знак разделения ,
        dict={ 'stock': symbol,  'date': datetime.strptime(values[0], '%d-%b-%y'), 'open':values[1],  'close': values[2],  'high': values[3],  'low': values[4], 'volume': values[5] }#сохраняем данные в виде словаря
        result.append(dict) #дополняем словарь с каждой итерацией
    return(result)




def get_data(symbol, start, end):
    logging.basicConfig(filename='app.log', level=logging.INFO)
    fromDB=get_interval(symbol, start, end)# выгружаем что хранится в БД на данный промежуток времени
    min=None
    max=None
    need_google=True
    if fromDB:                  #если fromDB  не пустой, то смотрим какое макс-е и миним-е значение есть в  БД из нужного интервала
        max=datetime.strptime(fromDB[-1][1], "%Y-%m-%d").date()
        min=datetime.strptime(fromDB[-0][1], "%Y-%m-%d").date()

        if min and max :                          #проверяем есть ли в БД данные на весь нужный промежуток
            if min==start and max==end:  #случай, когда в БД есть данные на весь промежуток
                #for row in fromDB:
                    #print(row)
                need_google=False #скачка не нужна
            else:                                       #случай когда в БД есть только часть данных за  нужный помежуток времени
                delete_DB(symbol, start, end) #всё удаляем, скачиваем нужный интервал, вставляем в БД
                need_google=True

    if need_google: #последовательность скачивания
        from_google=read_symbol(symbol, start, end) #считываем символ, даты, организуем всё в словарь
        insert_in_DB(from_google)  #вставляем в БД
        fromDB=get_interval(symbol, start, end) #запрашиваем данные из БД в интервале
    return (fromDB)

#1) запрашиваем данные на нужный период в БД
#2) если нужной инфы нет - докачиваем с сети
#3) из скаченного вставляем в БД
#get_data('AAPL','01 01 2015','01 01 2016')
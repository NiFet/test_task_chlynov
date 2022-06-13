# -*- coding: utf-8 -*-

import sys 
import shutil 
import xml.etree.ElementTree as ET
import pandas as pd
import logging
import os

file_path = sys.argv[1]            ### полный путь к файлу через параметр
file_path = file_path.replace('\\', '/')
file_name = file_path.split('/')[-1]         ### имя файла


logging.basicConfig(
    level=logging.DEBUG,
    filename = file_name[:-3] + 'log',
    format = "%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S',
    )


if file_name.split('.')[-1] != 'xml':  ### LOG перемещение в подкаталог bad из-за неверного формата
    logging.info('перемещение в подкаталог bad из-за неверного формата файла')
    shutil.move(file_path, file_path[:len(file_name)]+'bad')

tree = ET.parse(file_path)
root = tree.getroot()

keys = []          ### уникальные комбинации ЛицСч+Период
trash = []           ### индексы дубликатов
rows = []        

for k, payers in enumerate(root[1].findall('Плательщик')):          ### формируем таблицу
    try:
        id = payers.find('ЛицСч').text
    except TypeError:
        id = str(None)  ### log строка 19 + k*7 не имеет одного из ключевых реквизитов 
        logging.info('строка ' + str(19+k*7) + ' не имеет одного из ключевых реквизитов, не возможно обработать данные')
        continue

    try:
        name = payers.find('ФИО').text
    except TypeError:
        name = str(None)
    
    try:
        adres = payers.find('Адрес').text
    except  TypeError:
        adres = str(None)

    try:    
        sum = payers.find('Сумма').text
        try:
            sum = float(sum)
            if (sum  <= 0)  or (len(str(sum).split('.')[1]) > 2):            ### log неверный формат суммы
                logging.info('неверный формат суммы, строка ' + str(19+k*7))
                sum = str(None)
            else:
                sum = str(sum)
        except ValueError:
            sum = str(None)                            ### log  неверный формат суммы
            logging.info('неверный формат суммы, строка ' + str(19+k*7))
    except TypeError:
        sum = str(None)

    try:
        period = payers.find('Период').text
    except TypeError:
        period = str(None)
        logging.info('строка ' + str(19+k*7) + ' не имеет одного из ключевых реквизитов, не возможно обработать данные')            
        continue                                ### log строка 19 + k*7 не имеет одного из ключевых реквизитов 
        

    if len(period) != 6:
        period = str(None)               ###  log  неверный формат периода
        logging.info('неверный формат периода, строка ' + str(19+k*7))
    else:
        for i in (period):
            try:
                i = str(i)              #### log  неверный формат периода
            except ValueError:
                logging.info('неверный формат периода, строка ' + str(19+k*7))
                period = str(None)
                break


    if (id + period) in keys:
        trash.append(id)             ##### log  невозможно обработать запись, присутствуют дубликаты
        logging.info('невозможно обработать запись, присутствуют дубликаты, строка ' + str(19+k*7))
        keys.append(id + period)
        continue
    
    keys.append(id + period)

    rows.append({"ЛицСч": id,
             "ФИО": name,
             "Адрес": adres,
             "Период": period,
             "Сумма": sum})

doc_date = root[0][0][0][1].text

df = pd.DataFrame(rows)
df.insert(0, 'Дата акт. данных', doc_date)
df.insert(0, 'Имя файла реестра', file_name)

df = df[df.ЛицСч.apply(lambda s:  bool(s not in trash))]        ###срез таблицы без дубликатов
df['Период'].where(~(df.Период == 'None'), other=None, inplace=True)
df['Сумма'].where(~(df.Сумма == 'None'), other='0', inplace=True)
df.Сумма = df.Сумма.apply(lambda s:  float(s))
df['Сумма'].where(~(df.Сумма == 0), other=None, inplace=True)

df.to_csv(file_name[:-3] + 'csv', sep = ';', header=False, encoding='windows-1251')          ### сохранение таблицы

logging.shutdown()

if not os.path.exists(file_path[:-len(file_name)]+'arh/'):
   os.mkdir(file_path[:-len(file_name)]+'arh')

shutil.move(file_path, file_path[:-len(file_name)]+'arh/' + file_name)         ### перемещение исходника в архив

if not os.path.exists(file_path[:-len(file_name)]+'log/'):
   os.mkdir(file_path[:-len(file_name)]+'log')
   
shutil.move(file_name[:-3] + 'log', file_path[:-len(file_name)]+'log/')        
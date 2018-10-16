# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 15:27:45 2018

@author: Anand
"""
import sqlite3 as sql
import pandas as pd

class DataBase:
    
    def __init__(self):
        self.main()
        
    def convert(self, db, table, col_names_list):
        
        connection = sql.connect(db)
        cursor = connection.cursor()
        row_values = []
        col_data_copy = []
        for col in col_names_list:
            col_data_obj = cursor.execute("select * from {}".format( table))

            data = [list(i) for i in col_data_obj]
            
        only_col_names = []
        for col in col_names_list:
            only_col_names.append(col[1])
        
        
        df = pd.DataFrame(data, columns = only_col_names)
        print(df)
        df.to_csv('{}.csv'.format(table))
        connection.close()
        self.main()
                
    
    def getData(self, db, table):
        
        connection = sql.connect(db)
        cursor = connection.cursor()
        
        col_names = cursor.execute("PRAGMA TABLE_INFO({})".format(table))
        
        col_names_list = col_names.fetchall()
        for col in col_names_list:
            col_data_obj = cursor.execute("select {} from {}".format(col[1], table))
            col_data = col_data_obj.fetchall()
            print(col[1], " : ", col_data)
        connection.close()
        self.convert(db, table, col_names_list)
            
        
    
    def connect(self, db):
        
        connection = sql.connect(db)
        cursor = connection.cursor()
        
        tables = cursor.execute("select name from sqlite_master where type='table'")
        table_list = tables.fetchall()
        if len(table_list) == 0:
            print("This database is empty")
            self.main()
        else:
            print("Table in the database")
            print(table_list)
            table = input("Enter the name of the table you want to retrieve data from")
            self.getData(db, table)
        connection.close()
    
    def main(self):
        
        choice = int(input("1. Retrive data\n2. Exit"))
        if choice == 1:
            db = input("Enter the name of the db without the extension")
            db = db + ".sqlite3"
        
            self.connect(db)
        else:
            exit()
        
        
DataBase()
        
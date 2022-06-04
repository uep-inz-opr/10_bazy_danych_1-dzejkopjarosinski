
import sqlite3
import csv


#Establishing connection and creating a cursor to execute SQL Queries
sqlite_con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
cur = sqlite_con.cursor()

cur.execute('''CREATE TABLE polaczenia (from_subscriber data_type INTEGER, 
                  to_subscriber data_type INTEGER, 
                  datetime data_type timestamp, 
                  duration data_type INTEGER , 
                  celltower data_type INTEGER);''')


# Using with open, so you don't have to remember about closing the file at the end
with open('polaczenia_duze.csv', 'r') as fin:
    reader = csv.reader(fin, delimiter = ";")
    next(reader, None)
    rows= [x for x in reader]
    cur.executemany("INSERT INTO polaczenia (from_subscriber, to_subscriber, datetime, duration , celltower) VALUES (?, ?, ?, ?, ?);", rows)
    sqlite_con.commit()

class ReportGenerator:
  def __init__(self,connection):
    self.connection = connection
    self.report_text = None
    # Escape string is not needed fot this task


  def dur_sum(self):
     cursor = self.connection.cursor()
     sql_query = f"Select sum(duration) from polaczenia"
     cursor.execute(sql_query) #Args are not needed 
     result = cursor.fetchone()[0]
     return result


# Adding a clause not to run the methods unintenitonally 
if __name__ == "__main__":

    RepGen = ReportGenerator(sqlite_con)
    print(RepGen.dur_sum())
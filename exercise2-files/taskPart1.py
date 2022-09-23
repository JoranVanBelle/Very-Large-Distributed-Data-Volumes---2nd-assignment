from DbConnector import DbConnector
from tabulate import tabulate
import os
import time

from tqdm import tqdm

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.base_path = os.path.join("dataset", "Data")

    # def create_table(self, table_name):
    #     query = """CREATE TABLE IF NOT EXISTS %s (
    #                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
    #                name VARCHAR(30))
    #             """
    #     # This adds table_name to the %s variable and executes the query
    #     self.cursor.execute(query % table_name)
    #     self.db_connection.commit()

    # def insert_data(self, table_name):
    #     names = ['Bobby', 'Mc', 'McSmack', 'Board']
    #     for name in names:
    #         # Take note that the name is wrapped in '' --> '%s' because it is a string,
    #         # while an int would be %s etc
    #         query = "INSERT INTO %s (name) VALUES ('%s')"
    #         self.cursor.execute(query % (table_name, name))
    #     self.db_connection.commit()
        
    def create_users(self, table_name):
      query = """CREATE TABLE IF NOT EXISTS %s (
                  id VARCHAR(30) NOT NULL PRIMARY KEY,
                  has_labels boolean)
              """
      # This adds table_name to the %s variable and executes the query
      self.cursor.execute(query % (table_name))
      self.db_connection.commit()
    
    def create_activity(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id VARCHAR(30),
                   transportation_mode VARCHAR(30),
                   start_date_time datetime,
                   end_date_time datetime,
                   FOREIGN KEY (user_id) REFERENCES User(id))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_trackPoint(self, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   activity_id INT DEFAULT 888888,
                   lat DOUBLE NOT NULL,
                   lon DOUBLE NOT NULL,
                   altitude INT NOT NULL,
                   date_days double,
                   date_time datetime)
                """
                   #FOREIGN KEY (activity_id) REFERENCES Activity(id)
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_userdata(self, table_name):

      with open(os.path.join("dataset", 'labeled_ids.txt')) as file:
        users_with_labels = file.readlines()
        users_with_labels = [int(s.rstrip()) for s in users_with_labels]

      for id in os.listdir(self.base_path):
          # Take note that the name is wrapped in '' --> '%s' because it is a string,
          # while an int would be %s etc
          query = "INSERT INTO %s (id, has_labels) VALUES ('%s', %s)"
          self.cursor.execute(query % (table_name, id, str(int(id) in users_with_labels).upper())) 
      self.db_connection.commit()


    def insert_trackPointdata(self, table_name):

      for folder in tqdm(os.listdir(self.base_path)):
        data = {'lat': [], 'lon': [], 'altitude': [], 'date_days': [], 'date_time': []}
        for filename in os.listdir(os.path.join(self.base_path, folder, 'Trajectory')):
            with open(os.path.join(self.base_path, folder, 'Trajectory', filename)) as file:
                
                for line in file.readlines()[6:]:
                    line_data = (line.rstrip().split(','))
                    data['lat'].append(line_data[0])
                    data['lon'].append(line_data[1])
                    data['altitude'].append(line_data[3])
                    data['date_days'].append(line_data[4])
                    data['date_time'].append(line_data[5] + ' ' + line_data[6])
        
        activity_id = 888888
        #Insert data from one user
        for i in range(len(data['lat'])):
          # Take note that the name is wrapped in '' --> '%s' because it is a string,
          # while an int would be %s etc
          query = "INSERT INTO %s (lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, '%s')"
          #print(activity_id, data['lat'][i], data['lon'][i], data['altitude'][i], data['date_days'][i], data['date_time'][i])
          self.cursor.execute(query % (table_name, data['lat'][i], data['lon'][i], data['altitude'][i], data['date_days'][i], data['date_time'][i])) 
          self.db_connection.commit()

        #df = pd.DataFrame(data)

      
        

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = ExampleProgram()
        # time.sleep(3000)
        
        # program.drop_table(table_name="User")
        # program.drop_table(table_name="Activity")
        # program.drop_table(table_name="TrackPoint")
        program.create_users(table_name="User")
        program.create_activity(table_name="Activity")
        program.create_trackPoint(table_name="TrackPoint")
        program.insert_userdata(table_name="User")
        program.insert_trackPointdata(table_name="TrackPoint")
        _ = program.fetch_data(table_name="User")
        program.show_tables()
        # program.drop_table(table_name="User)
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        program.drop_table(table_name="TrackPoint")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="User")
        
        # TODO
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()

from DbConnector import DbConnector
from tabulate import tabulate
import os
import sys
from datetime import datetime

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
                   user_id VARCHAR(30) NOT NULL,
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
                   activity_id INT NOT NULL,
                   lat DOUBLE NOT NULL,
                   lon DOUBLE NOT NULL,
                   altitude INT NOT NULL,
                   date_days DOUBLE,
                   date_time DATETIME,
                   FOREIGN KEY (activity_id) REFERENCES Activity(id))
                """
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

    def insert_activitydata(self, table_name=""):
        
        data = {'user_id' : [], 'transportation_mode': [], 'start_date_time': [], 'end_date_time': []}

        with open(os.path.join("dataset", 'labeled_ids.txt')) as file:
            users_with_labels = file.readlines()
            users_with_labels = [s.rstrip() for s in users_with_labels]
        
        print("----------------------------------")
        print("Parsing ActivityData")       
        print("----------------------------------")
        for user in tqdm(users_with_labels):
            labels_path = os.path.join("dataset", "Data", user, "labels.txt")

            with open(labels_path) as labels_file:
                for line in labels_file.readlines()[1:]:
                    line_data = (line.rstrip().split('\t'))
                    
                    data["user_id"].append(user)
                    data["transportation_mode"].append(line_data[2])
                    data["start_date_time"].append(line_data[0])
                    data["end_date_time"].append(line_data[1])

        print("----------------------------------")
        print("Adding ActivityData to the Database")       
        print("----------------------------------")
        for i in tqdm(range(len(data['user_id']))):
            query = "INSERT INTO %s (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
            self.cursor.execute(query % (table_name, data['user_id'][i], data['transportation_mode'][i], data['start_date_time'][i], data['end_date_time'][i])) 
            self.db_connection.commit()

            #TODO: Delete this break   
            # break



    def insert_trackPointdata(self, table_name):

      print("----------------------------------")
      print("Parsing and adding Trackpoints to the Database")       
      print("----------------------------------")
      for folder in tqdm(os.listdir(self.base_path)):

        #TODO: Get activities of that User
        get_activities = "SELECT id, start_date_time, end_date_time FROM Activity where user_id = '%s' ORDER BY start_date_time, end_date_time"
        self.cursor.execute(get_activities % (folder))
        activities_sql = self.cursor.fetchall()

        #TODO: Check if that makes sense
        if len(activities_sql) == 0:
            continue

        for filename in os.listdir(os.path.join(self.base_path, folder, 'Trajectory')):
            with open(os.path.join(self.base_path, folder, 'Trajectory', filename)) as file:
                lines = file.readlines()[6:]

                if len(lines) <= 2500:
                    data = {'lat': [], 'lon': [], 'altitude': [], 'date_days': [], 'date_time': [], 'activity_ids': []}

                    for line in lines:
                        line_data = (line.rstrip().split(','))
                        data['lat'].append(line_data[0])
                        data['lon'].append(line_data[1])
                        data['altitude'].append(line_data[3])
                        data['date_days'].append(line_data[4])
                        data['date_time'].append(line_data[5] + ' ' + line_data[6])

        
        
        ## Loop through every trackpoint
            ## Try to find the correct activity id

        j = 0
        for i in range(len(data['lat'])):

            while (j < len(activities_sql) and activities_sql[j][1] < datetime.strptime(data['date_time'][i], "%Y-%m-%d %H:%M:%S")): 
                if(activities_sql[j][2] >= datetime.strptime(data['date_time'][i], "%Y-%m-%d %H:%M:%S")):
                    data['activity_ids'].append(activities_sql[j][0])
                    break
                else:
                    j += 1

            if j >= len(activities_sql):
                j = 0
                data['activity_ids'].append(-1) # Maybe this causes the error


        #Insert data from one user
        j = 0
        while True:
            if data['activity_ids'][j] >= 0:
                query = f"INSERT INTO {table_name} (activity_id, lat, lon, altitude, date_days, date_time) VALUES ({data['activity_ids'][0]}, {data['lat'][0]}, {data['lon'][0]}, {data['altitude'][0]}, {data['date_days'][0]}, '{data['date_time'][0]}')"
                break

            if j < len(data['lat']):
                j += 1
            

        for i in range(j + 1, len(data['lat'])):

            #TODO: Find the corresponding activity and get id of it

            if data['activity_ids'][i] >= 0:
                
                query += f", ({data['activity_ids'][i]}, {data['lat'][i]}, {data['lon'][i]}, {data['altitude'][i]}, {data['date_days'][i]}, '{data['date_time'][i]}')"
        
        self.cursor.execute(query) 
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
    program = ExampleProgram()

    try:
        
        # time.sleep(3000)
        print("Hello")

        # program.create_users(table_name="User")
        # program.create_activity(table_name="Activity")
        # program.create_trackPoint(table_name="TrackPoint")
        # program.insert_userdata(table_name="User")
        # program.insert_activitydata(table_name="Activity")
        program.insert_trackPointdata(table_name="TrackPoint")
        _ = program.fetch_data(table_name="User")
        program.show_tables()
        # program.drop_table(table_name="User)
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        program.drop_table(table_name="TrackPoint")
        # program.drop_table(table_name="Activity")
        # program.drop_table(table_name="User")
        
        # TODO
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()


#ERROR 1055 (42000): Expression #2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'sec_assignment.Activity.start_date_time' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by

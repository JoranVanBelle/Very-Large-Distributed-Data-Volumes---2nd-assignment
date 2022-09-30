from DbConnector import DbConnector
from tabulate import tabulate
import os
import sys
from datetime import datetime

from tqdm import tqdm

class Task1Program:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.base_path = os.path.join("dataset", "Data")
        
    def create_users(self, table_name: str):
        """Creates the user table in the database.

        Args:
            table_name (string): The name we want to use for the table in the database.
        """        

        query = """CREATE TABLE IF NOT EXISTS %s (
                  id VARCHAR(30) NOT NULL PRIMARY KEY,
                  has_labels boolean)
              """

        self.cursor.execute(query % (table_name))
        self.db_connection.commit()
    
    def create_activity(self, table_name: str):
        """Creates the activity table in the database.

        Args:
            table_name (string): The name we want to use for the table in the database.
        """        

        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   user_id VARCHAR(30) NOT NULL,
                   transportation_mode VARCHAR(30),
                   start_date_time datetime,
                   end_date_time datetime,
                   FOREIGN KEY (user_id) REFERENCES User(id))
                """
                
        
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def create_trackPoint(self, table_name: str):
        """Creates the trackpoints table in the database.

        Args:
            table_name (string): The name we want to use for the table in the database.
        """     

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
        
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_userdata(self, table_name: str):
        """ This function inserts the user data into the user table of our database.
            The data for the "has_labels" column is extracted from the labeld_ids.txt file.
            The user_ids themselves are just the names of the folders in the Data folder of the dataset.

        Args:
            table_name (string): The name of the table we want to insert the data into.
        """        

        with open(os.path.join("dataset", 'labeled_ids.txt')) as file:
            users_with_labels = file.readlines()
            users_with_labels = [int(s.rstrip()) for s in users_with_labels]

        for id in os.listdir(self.base_path):
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (id, has_labels) VALUES ('%s', %s)"
            self.cursor.execute(query % (table_name, id, str(int(id) in users_with_labels).upper())) 
        self.db_connection.commit()

    def insert_activitydata(self, table_name: str):
        """ This function inserts the activities into the activity table of our database.
            
            Since only the folders of the users with "has_labels = 1" contain activity data, we only need to process these users.
            After parsing all the activities, we add the activites to our activity table in the database.


        Args:
            table_name (string): The name of the table we want to insert the data into.
        """   


        
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


    def find_matching_activities(self, activities, data):
        """This is a helper function which allows us to efficiently find the matching activities for a given trackpoint.

        Args:
            activities (list): A list with all the rows of the activities for a given user.
            data (map): A map with all the necassary information about the trackpoints. This function then adds the activities_ids to the data['activity_ids'] array.
        """        

        print("----------------------------------")
        print(f"Finding matching activities for {len(data['lat'])} Trackpoints and {len(activities)} activities")       
        print("----------------------------------")
        
        #Binary search for a corresponding activity:

        for i in tqdm(range(len(data['lat']))):
            
            lower_bound = 0
            upper_bound = len(activities) - 1

            success = False

            while lower_bound < upper_bound:

                j = int(lower_bound + (upper_bound - lower_bound)/2)

                #Starttime of activity after the timestamp 
                if activities[j][1] > datetime.strptime(data['date_time'][i], "%Y-%m-%d %H:%M:%S"):
                    upper_bound = j - 1

                #Endtime of activity before the timestamp
                elif activities[j][2] < datetime.strptime(data['date_time'][i], "%Y-%m-%d %H:%M:%S"):
                    lower_bound = j + 1

                #Found a corresponding activity
                else:
                    data['activity_ids'].append(activities[j][0])
                    success = True
                    break

            if not success:
                data['activity_ids'].append(-1)



    def insert_trackPointdata(self, table_name):
        """ This function inserts the trackpoints into the trackpoint table of our database.
            For this the function loops through all the users, and then performs the following steps:
                1. Get all the activities corresponding to this user from the activities table.
                   If the user has no activities, we continue with the next user.
                2. We parse the trackpoints from the .plt files for this user. 
                   We skip those .plt files that contain more than 2,500 trackpoints.
                3. We use the find_matching_activities function find the corresponding activity for each trackpoint.
                   If we don't find a activity at the timestamp of the trackpoint, we don't add the trackpoint to the database.
                4. We create a batch query for all the trackpoints we want to add for this one user and then commit the query to the database.



        Args:
            table_name (_type_): _description_
        """        

        print("----------------------------------")
        print("Parsing and adding Trackpoints to the Database")       
        print("----------------------------------")
        for folder in tqdm(os.listdir(self.base_path)):

            #Step 1:
            get_activities = "SELECT id, start_date_time, end_date_time FROM Activity where user_id = '%s' ORDER BY start_date_time, end_date_time"
            self.cursor.execute(get_activities % (folder))
            activities_sql = self.cursor.fetchall()


            if len(activities_sql) == 0:
                continue

            #Step 2:

            data = {'lat': [], 'lon': [], 'altitude': [], 'date_days': [], 'date_time': [], 'activity_ids': []}
            
            for filename in os.listdir(os.path.join(self.base_path, folder, 'Trajectory')):
                with open(os.path.join(self.base_path, folder, 'Trajectory', filename)) as file:
                    lines = file.readlines()[6:]

                    if len(lines) <= 2500:

                        for line in lines:
                            line_data = (line.rstrip().split(','))
                            data['lat'].append(line_data[0])
                            data['lon'].append(line_data[1])
                            data['altitude'].append(line_data[3])
                            data['date_days'].append(line_data[4])
                            data['date_time'].append(line_data[5] + ' ' + line_data[6])
   
            #Step 3:

            self.find_matching_activities(activities_sql, data)
    

            j = 0
            while True:
                if data['activity_ids'][j] >= 0:
                    query = f"INSERT INTO {table_name} (activity_id, lat, lon, altitude, date_days, date_time) VALUES ({data['activity_ids'][j]}, {data['lat'][j]}, {data['lon'][j]}, {data['altitude'][j]}, {data['date_days'][j]}, '{data['date_time'][j]}')"
                    break

                if j < len(data['lat']):
                    j += 1
                    
                if len(data["activity_ids"]) == j:
                    break
                    
            #Step 4:

            for i in range(j + 1, len(data['lat'])):

                if data['activity_ids'][i] >= 0:
                        
                    query += f", ({data['activity_ids'][i]}, {data['lat'][i]}, {data['lon'][i]}, {data['altitude'][i]}, {data['date_days'][i]}, '{data['date_time'][i]}')"
                
            self.cursor.execute(query) 
            self.db_connection.commit()


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
    program = Task1Program()

    try:
        
        program.create_users(table_name="User")
        program.create_activity(table_name="Activity")
        program.create_trackPoint(table_name="TrackPoint")
        program.insert_userdata(table_name="User")
        program.insert_activitydata(table_name="Activity")
        program.insert_trackPointdata(table_name="TrackPoint")
        _ = program.fetch_data(table_name="User")
        program.show_tables()
        

    except Exception as e:
        print("ERROR: Failed to use database:", e)
        #program.drop_table(table_name="TrackPoint")
        #program.drop_table(table_name="Activity")
        #program.drop_table(table_name="User")
        
        
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()


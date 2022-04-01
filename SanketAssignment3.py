"""Author: Sanket Parmar
Python Version: 3.9.5
Course: CST8333 - Assignment 3
Professor: Leanne Seaward"""

''' python library '''
from pandas import read_csv, DataFrame
from mysql import connector
from multiprocessing import Process

''' inserting data function '''
def db_datainsertion(pipeline_data):
    pipeline_database = connector.connect(
        host='127.0.0.1',
        user="root",
        password="",
        database="pipeline_database"
    )

    '''  create a cursor to pass a query'''
    pipeline_cursordb = pipeline_database.cursor()

    '''  execute query'''
    data_query = '''Insert into pipeline_data (Incident_Number,Incident_Types,ReportedDate, Nearest_Populated_Centre,Province,Company,Substance,Significant,What_happened_category)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        '''
    pipeline_cursordb.execute(data_query, (pipeline_data.Incident_Number, pipeline_data.Incident_Types,            pipeline_data.ReportedDate, pipeline_data.Nearest_Populated_Centre, pipeline_data.Province, pipeline_data.Company, pipeline_data.Substance, pipeline_data.Significant, pipeline_data.What_happened_category))

    pipeline_database.commit()

''' execution:'''
if __name__ == '__main__':

    n=10
    print("Sanket Assignment 3")
    for i in range(0, n):
        print("Select any of the given options")
        print("A. Build a Database")
        print("B. Create a Table in the Database")
        print("C. Enter Pipeline Data into the Database")
        print("D. Show Pipeline Data from the Database")
        print("E. Update Pipeline Data of the Database")
        print("F. Insert Pipeline Data into Database")
        print("G. Delete Pipeline Data from the Database")
        print("H. Stop the program")
        print("**********************")
        sel_op = input("Enter the operation to perform:")
        print("**********************")

    #     perform operation
        if (sel_op == "A" or sel_op == "a"):
            print("Sanket Assignment 3: Build a Database")
            
    #       building a database
    #       with try and except (unit test)
    #       connect to mysql server
            try:
                pipeline_database = connector.connect(
                    host='127.0.0.1',
                    user="root",
                    password=""
                )

        #       create a cursor to pass a query
                pipeline_cursordb = pipeline_database.cursor()

        #       execute query
                db_create = "Create database pipeline_database"
                pipeline_cursordb.execute(db_create)
                print("Pipeline database is builded in Mysql Server.")

            except:
               
                print("Check if your Mysql Server is on or Look if the database is already created in the server.")
                

            print("**********************\n")

        if (sel_op == "B" or sel_op == "b"):
            print("Sanket Assignment 3: Create a Table in the Database")
            # creating a table
            try:
                pipeline_database = connector.connect(
                    host='127.0.0.1',
                    user="root",
                    password="",
                    database="pipeline_database"
                )

                #       create a cursor to pass a query
                pipeline_cursordb = pipeline_database.cursor()

                #       execute query
                table_create = '''Create table pipeline_data (Incident_Number nvarchar(100), Incident_Types nvarchar(100), ReportedDate nvarchar(100), Nearest_Populated_Centre nvarchar(100), Province nvarchar(100), Company nvarchar(100), Substance nvarchar(100), Significant nvarchar(100), What_happened_category nvarchar(100))'''

                pipeline_cursordb.execute(table_create)
                print("Pipeline table is created in Mysql Server Pipeline Database.")

            except:
                
                print(
                    "Check if your Mysql Server is on or Look if the table is already created in the server.")
               
            print("**********************\n")

        if (sel_op == "C" or sel_op == "c"):
            print("Sanket Assignment 3: Insert Data into Table of the Database")
            """# Insert Data
            # Read CSV file"""
            try:
                data_pipeline = read_csv(r'pipeline_dataset.csv')
            except:
                print("File not found or corrupted")
                continue

            """# Remove empty rows using dropna inbuilt function
            # To see if there are empty values and it ll return true"""
            print(data_pipeline.isna())
            data_pipeline = data_pipeline.dropna()
            print(data_pipeline.isna())

            # using multiprocessing to execute the db_datainsertion function
            for index, data in data_pipeline.iterrows():
                insert_data_process1 = Process(target=db_datainsertion, args=(data,))
                # start the process
                insert_data_process1.start()
                # Complete the process then forward ahead
                insert_data_process1.join()

           
            print("**********************\n")

        if (sel_op == "D" or sel_op == "d"):
            print("Sanket Assignment 3: Data from the Database")
            '''# connect database'''
            pipeline_database = connector.connect(
                host='127.0.0.1',
                user="root",
                password="",
                database="pipeline_database"
            )

            '''#       create a cursor to pass a query'''
            pipeline_cursordb = pipeline_database.cursor()
            '''#       execute query'''
            show_data_query = "Select * from pipeline_data"
            pipeline_cursordb.execute(show_data_query)

            pipeline_dbdata = pipeline_cursordb.fetchall()

            '''#convert to dataframe'''
            pipeline_dbdf = DataFrame(pipeline_dbdata)

            '''# sort dataframe data using pruid'''
            pipeline_dbdf = pipeline_dbdf.sort_values(pipeline_dbdf.columns[1])

            print("Data:\n", pipeline_dbdf)
            print("**********************")


        if (sel_op == "E" or sel_op == "e"):
            print("Sanket Assignment 3: Update Data in the Database")

            # user inputs
            incidentNumber_val = input("Enter Incident Number to update:")
            newIncidentNumber_val = input("Enter New Incident Number to change with:")
            # connect database
            pipeline_database = connector.connect(
                host='127.0.0.1',
                user="root",
                password="",
                database="pipeline_database"
            )

            #       create a cursor to pass a query
            pipeline_cursordb = pipeline_database.cursor()
            #       execute query
            up_query_attr = (str(newIncidentNumber_val), str(incidentNumber_val))
            pipeline_cursordb.execute("Update pipeline_data set Incident_Number = %s where Incident_Number = %s", up_query_attr)
            pipeline_database.commit()
            print("Data Updated perfectly by changing Incident Number value ", incidentNumber_val, " to ", newIncidentNumber_val)
            print("**********************")

        if (sel_op == "F" or sel_op == "f"):
            print("Sanket Assignment 3: Insert Data into the Database")

            # user inputs
            incidentNumber_val = input("Enter incident number Value to insert:")
            company_val = input("Enter Company value to insert:")
            province_val = input("Enter province value to insert:")
            # connect database
            pipeline_database = connector.connect(
                host='127.0.0.1',
                user="root",
                password="",
                database="pipeline_database"
            )

            #       create a cursor to pass a query
            pipeline_cursordb = pipeline_database.cursor()
            #       execute query
            insert_query_attr = (incidentNumber_val, company_val, province_val)
            pipeline_cursordb.execute("Insert into pipeline_data (Incident_Number,Company,Province ) values (%s, %s, %s)", insert_query_attr)
            pipeline_database.commit()
            print("Data inserted perfectly with Incidnet Number, Company, Province values with ", insert_query_attr)
            print("**********************")

        if (sel_op == "G" or sel_op == "g"):
            print("Sanket Assignment 3: Delete Data from the Database")

            # user inputs
            province_val = input("Enter Province value to delete values:")
            # connect database
            pipeline_database = connector.connect(
                host='127.0.0.1',
                user="root",
                password="",
                database="pipeline_database"
            )

            #       create a cursor to pass a query
            pipeline_cursordb = pipeline_database.cursor()
            #       execute query
            del_query_attr = (province_val,)
            pipeline_cursordb.execute("Delete from pipeline_data where Province = %s", del_query_attr)
            pipeline_database.commit()
            print("Data delete perfectly with Province value ", province_val)
            print("**********************")

        if (sel_op == "H" or sel_op == "h"):
            exit()
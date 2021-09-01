import psycopg2
import sys
import threading
import time
from array import *
from random import randint, getrandbits, randrange
import os
l = threading.Lock()

#read the main function first to make sense of the program
with open('password.txt') as f:
    lines = [line.rstrip() for line in f]

username = lines[0]
pg_password = lines[1]

#variables
book_refArray = [] #these arrays are just to check for duplicate values while generating random booking references, nothing to do with database
ticket_noArray =[]
threads_array = []
passengers = []
countLines = 0

#global variables
successfulTrans = 0
unsuccessfulTrans = 0
failedTrans = 0

#possibly put them in threads, as every thread will need to use the cursor at the same time if you remove locks
conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password )
cursor = conn.cursor()

filename = str(sys.argv[-1])
args = filename.split(";")
inputFile = args[0].split("input=",1)[1]
transaction = args[1].split("transaction=",1)[1]
threads = int(args[2].split("threads=",1)[1])

f = open("transaction-bookings.sql", "w+")

class myThread (threading.Thread):
    def __init__(self, passengers, num_of_times, location): #initializer
        threading.Thread.__init__(self)
        self.passengers = passengers
        self.num_of_times = num_of_times
        self.location = location
    def run(self):
        cursor = conn.cursor() #every thread will have their own cursor
        #print("Thread", (self.location//self.num_of_times)+1) #I'm just making sure I have the correct number of threads, but this is irrelevant to the homework
        mylocation = self.location
        for i in range(self.num_of_times):
            book_ref = str(randint(100000, 999999))
            ticket_no = str(randrange(1000000000000,9999999999999))
            while book_ref in book_refArray:
                #print(book_ref, "book_ref already exists, doing mixing again") #I AM NOT PRINTING THESE VALUES ANYMORE AS THE TA'S ONLY WANTED TO SEE THE RESULTS
                book_ref = str(randint(100000, 999999))
                #print("this is the new value:", book_ref)
            while ticket_no in ticket_noArray:
                #print(ticket_no, "ticket already exists, doing mixing again")
                ticket_no = str(randrange(1000000000000,9999999999999))
                #print("this is the new value: ", ticket_no)
            ticket_noArray.append(ticket_no)
            book_refArray.append(book_ref)
            l.acquire()
            if transaction == "y":
                #If we have to do it with transactions, I am creating an isolation level to make sure that other threads will not interfere with the data I'm updating
                cursor.execute("BEGIN TRANSACTION isolation level repeatable read;")
                updateTables(self.passengers[mylocation][0],self.passengers[mylocation][1],book_ref, ticket_no)
                conn.commit() #this function call 
            else: #no
                conn.autocommit = True
                updateTables(self.passengers[mylocation][0],self.passengers[mylocation][1],book_ref, ticket_no)
            mylocation += 1
            l.release()
            #print("Book ref: ", book_ref, "and ticket no: ", ticket_no, "is successfully inserted to thread no: ", mylocation, self.passengers[mylocation][0]) #Irrelevant to the homework, just for our testing

def updateTables(passenger_id, flight_id, book_ref, ticket_no):
    global unsuccessfulTrans, successfulTrans, failedTrans
    sql = "SELECT COUNT(*) FROM flights WHERE flight_id = %s" %flight_id #at first, we check if such a flight exists at all from the flights table
    cursor.execute(sql)
    f.write(sql)
    f.write(";\n")
    flightExists = int(cursor.fetchall()[0][0])
    if flightExists == 0 or passenger_id is "": #if the flight does not exist or passenger name does not exist, then it's a failed transaction
        failedTrans += 1
        return
    sql = "SELECT seats_available FROM flights WHERE flight_id = %s" %flight_id #if the flight exists, then we can check the availability of the seats
    cursor.execute(sql)
    f.write(sql)
    f.write(";\n")
    checkAvailability = cursor.fetchone()[0]
    if checkAvailability > 0:
        sql_available = "UPDATE flights SET seats_available = seats_available - 1 WHERE seats_available != 0 AND flight_id = %s;" %flight_id
        f.write(sql_available)
        f.write("\n")
        sql_booked = "UPDATE flights SET seats_booked = seats_booked + 1 WHERE seats_booked != 50 AND flight_id = %s;" %flight_id
        f.write(sql_booked)
        f.write("\n")
        sql_bookings = "INSERT INTO bookings VALUES(%s ,CURRENT_TIMESTAMP, 13000);" %book_ref
        f.write(sql_bookings)
        f.write("\n")
        sql_ticket = "INSERT INTO ticket VALUES(%s, %s,%s, 'Renee', NULL, NULL);" % (ticket_no, book_ref, passenger_id)
        f.write(sql_ticket)
        f.write("\n")
        sql_ticketflight = "INSERT INTO ticket_flights VALUES (%s,%s,'Economy', 13000);" % (ticket_no, flight_id)
        f.write(sql_ticketflight)
        f.write("\n")
        sql = f"\n{sql_bookings}\n{sql_ticket}\n{sql_ticketflight}\n{sql_available}\n{sql_booked}\n\n\n"
        cursor.execute(sql)
        successfulTrans += 1
    else:
        sql = "INSERT INTO bookings VALUES(%s ,CURRENT_TIMESTAMP, 13000)" %book_ref
        cursor.execute(sql)
        f.write(sql)
        f.write(";\n")
        unsuccessfulTrans += 1

threads_array = []
passengers = []
countLines = 0

if __name__ == '__main__':
    try:
        with open (inputFile, 'r') as file:
            line = file.readline()
            for line in file:
                if line == "":
                    continue
                passenger_id = line.split(",")[0] 
                flight_id = line.split(",")[1]
                countLines = countLines + 1
                passengers.append([passenger_id, flight_id])
            #for now, I am just putting passenger and flight id to an array, because I will need their whole count to figure out how many threads to create
        start_time = time.time() #irrelevant to the homework
        #here I calculated the number of threads needs to be created
        thread_divide = countLines // threads
        thread_remainder = countLines % threads
        for i in range(threads):
            threads_array.append(thread_divide)
        for i in range(thread_remainder): #if we will have a remainder, this guy will add it to the threads in equal number
            threads_array[i] += 1 #threads_array is merely an array tells which thread will have how many element. it's sth like [11, 11, 11, 10, 10, etc.]
        array_location = 0
        threads_storage_names = []
        for i in threads_array:
            x = myThread(passengers, i, array_location) #I've created a class for thread rather than prof's example, so that we can use multiple functions in thread.run()
            x.start()
            array_location += i
            threads_storage_names.append(x) #I am storing threads here, because technically all of their names is x, so at least we can access their locations maybe? but it makes it possible for us to join threads don't worry
        
        for t in threads_storage_names:
            t.join()
    except (KeyboardInterrupt, SystemExit):
        print("The program has been stopped by the TA with keyboard interrupt! I am pulling the database back to initial state!")
        cursor.execute("SELECT COUNT(*) FROM ticket;")
        tickets = cursor.fetchall()[0][0]
        print("# records successfully updated so far: ", tickets)
        os._exit(1)
    except:
        print("The process has been killed for an unknown reason! Exiting the program!")
        os._exit(2)
    
    print("Successful Transactions: ", successfulTrans, "\nUnsuccessful Transactions: ", unsuccessfulTrans, "\nFailed Transactions: ", failedTrans)
    cursor.execute("SELECT COUNT(*) FROM ticket;")
    f.write("SELECT COUNT(*) FROM ticket;")
    f.write("\n")
    tickets = cursor.fetchall()[0][0]
    if tickets:
        print("# records updated in ticket table: ", tickets)
    else:
        print("# records updated in tickets table: 0")
    cursor.execute("SELECT COUNT(*) FROM bookings;")
    f.write("SELECT COUNT(*) FROM bookings;")
    f.write("\n")
    bookings = cursor.fetchall()[0][0]
    if bookings:
        print("# records updated in bookings table: ", tickets)
    else:
        print("# records updated in bookings table: 0")
    cursor.execute("SELECT COUNT(*) FROM ticket_flights;")
    f.write("SELECT COUNT(*) FROM ticket_flights;")
    f.write("\n")
    ticket_flights = cursor.fetchall()[0][0]
    if ticket_flights:
        print("# records updated in ticket_flights table: ", tickets)
    else:
        print("# records updated in ticket_flights table: 0")
    cursor.execute("SELECT SUM(seats_booked) FROM flights;")
    f.write("SELECT SUM(seats_booked) FROM flights;")
    f.write("\n")
    flights = cursor.fetchall()[0][0]
    if flights:
        print("# records updated in flights table: ", flights)
    else:
        print("# records updated in flights table: 0")
    #print("Time taken: ", time.time() - start_time) #Just for our testing, irrelevant to the homework
    cursor.close()
    conn.close()
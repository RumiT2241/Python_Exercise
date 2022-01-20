import sqlite3
import json
import sys
table_names = {}

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Exception as e:
        pass
    return conn

def createTables(conn):
    createEmployee(conn)
    createCompany(conn)
    createJob(conn)
    createPosition(conn)

def createEmployee(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS EMPLOYEE
             (GUID TEXT PRIMARY KEY     NOT NULL,
             STATUS           TEXT    NOT NULL,
             STATE            TEXT     NOT NULL);''')
    table_names["Employee"] = 1

def createCompany(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS COMPANY
             (GUID TEXT PRIMARY KEY     NOT NULL,
             NAME           TEXT    NOT NULL);''')
    table_names["Company"] = 1 

def createPosition(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS POSITION
             (GUID TEXT PRIMARY KEY     NOT NULL,
             NAME           TEXT    NOT NULL,
             STATUS           TEXT    NOT NULL);''')
    table_names["Position"] = 1 


def createJob(conn):
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS JOB
                (GUID TEXT PRIMARY KEY     NOT NULL,
                COMPANY_GUID           TEXT    NOT NULL,
                POSITION_GUID            TEXT     NOT NULL,
                EMPLOYEE_GUID            TEXT     NOT NULL);''')
        table_names["Job"] = 1 
    except Exception as e:
        pass

def insertEmployee(conn, employee):
    try:
        sql = ''' INSERT INTO EMPLOYEE(GUID,STATUS,STATE)
                VALUES(?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, employee)
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        pass

def insertCompany(conn, company):
    try:
        sql = ''' INSERT INTO COMPANY(GUID,NAME)
                VALUES(?,?) '''
        cur = conn.cursor()
        cur.execute(sql, company)
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        pass

def insertPosition(conn, position):
    try:
        sql = ''' INSERT INTO POSITION(GUID,NAME,STATUS)
                VALUES(?,?,?) '''
        cur = conn.cursor()
        cur.execute(sql, position)
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        pass

def insertJob(conn, job):
    try:
        cur = conn.cursor()
        cur.execute(''' SELECT EXISTS(SELECT 1 FROM EMPLOYEE WHERE GUID=?) ''',[job[2]])
        result = cur.fetchone()
        if not result:
            return
        cur.execute(''' SELECT EXISTS(SELECT 1 FROM COMPANY WHERE GUID=?) ''',[job[1]])
        
        result = cur.fetchone()
        if not result:
            return
        if job[3] is not "":
            cur.execute(''' SELECT EXISTS(SELECT 1 FROM POSITION WHERE GUID=?) ''',[job[3]])
            result = cur.fetchone()
            if not result:
                return

        sql = ''' INSERT INTO JOB(GUID,COMPANY_GUID,EMPLOYEE_GUID,POSITION_GUID)
                VALUES(?,?,?,?) '''
        
        cur.execute(sql, job)
        conn.commit()
        return cur.lastrowid
    except Exception as e:
        pass

def updateEmployee(conn, employee):
    sql = ''' UPDATE EMPLOYEE
              SET STATUS = ? ,
                  STATE = ? 
              WHERE GUID = ?'''
    cur = conn.cursor()
    cur.execute(sql, employee)
    conn.commit()

def updateCompany(conn, company):
    sql = ''' UPDATE COMPANY
              SET NAME = ? 
              WHERE GUID = ?'''
    cur = conn.cursor()
    cur.execute(sql, company)
    conn.commit()

def updatePosition(conn, position):
    sql = ''' UPDATE COMPANY
              SET NAME = ? ,
                 STATUS = ?
              WHERE GUID = ?'''
    cur = conn.cursor()
    cur.execute(sql, position)
    conn.commit()

def updateJob(conn, job):
    sql = ''' UPDATE JOB
              SET COMPANY_GUID = ? ,
                 EMPLOYEE_GUID = ?,
                 POSITION_GUID = ?
              WHERE GUID = ?'''
    cur = conn.cursor()
    cur.execute(sql, job)
    conn.commit()

def update(conn, input):
    try:
        if input["source_table"] == "Employee":
                updateEmployee(conn,(input["status"],input["state"],input["guid"]))
        elif input["source_table"] == "Company":
                updateCompany(conn,(input["name"],input["guid"]))
        elif input["source_table"] == "Position":
                updatePosition(conn,(input["name"],input["status"],input["guid"]))
        elif input["source_table"] == "Job":
                if "position_guid" in input.keys():
                    pos = input["position_guid"]
                else:
                    pos = ""
                updateJob(conn,(input["company_guid"],input["employee_guid"],pos,input["guid"]))
    except Exception as e:
        pass


def insert(conn, input):
    try:
        if input["source_table"] == "Employee":
                insertEmployee(conn,(input["guid"],input["status"],input["state"]))
        elif input["source_table"] == "Company":
                insertCompany(conn,(input["guid"],input["name"]))
        elif input["source_table"] == "Position":
                insertPosition(conn,(input["guid"],input["name"],input["status"]))
        elif input["source_table"] == "Job":
                if "position_guid" in input.keys():
                    pos = input["position_guid"]
                else:
                    pos = ""
                insertJob(conn,(input["guid"],input["company_guid"],input["employee_guid"],pos))
    except Exception as e:
        pass  


def deleteEmployee(conn, id):
    sql = 'DELETE FROM EMPLOYEE WHERE GUID=?'
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()

def deleteJob(conn, id):
    sql = 'DELETE FROM JOB WHERE GUID=?'
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()

def deletePosition(conn, id):
    sql = 'DELETE FROM POSITION WHERE GUID=?'
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()

def deleteCompany(conn, id):
    sql = 'DELETE FROM COMPANY WHERE GUID=?'
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()

def delete(conn, input):
    try:
        if input["source_table"] == "Employee":
                deleteEmployee(conn,input["guid"])
        elif input["source_table"] == "Company":
                deleteCompany(conn,input["guid"])
        elif input["source_table"] == "Position":
                deletePosition(conn,input["guid"])
        elif input["source_table"] == "Job":
                insertJob(conn,input["guid"])
    except Exception as e:
        pass  
def processData(conn, input):
    file = open(input, "r")
    lines = file.readlines()
    for i in lines:
        payload = json.loads(i)
        if "source_table" in payload.keys():
            if "action" in payload.keys():
                if payload["action"] == "INSERT":
                        insert(conn, payload)
                elif payload["action"] == "UPDATE":
                        update(conn, payload)
                elif payload["action"] == "DELETE":
                        delete(conn, payload)

def printTable(conn,table):
    print(table)
    print("======")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM "+ table)
    rows = cur.fetchall()
    print(json.dumps( [dict(ix) for ix in rows]  ))


def printTables(conn):
    printTable(conn,"COMPANY")
    printTable(conn,"EMPLOYEE")
    printTable(conn,"JOB")
    printTable(conn,"POSITION")

def main():

    n = len(sys.argv)
    if n == 2:
        input = sys.argv[1]
    else:
        input = "payload.txt"
    conn = create_connection("test.db")
    with conn:
        createTables(conn)
        processData(conn,input)
        printTables(conn)
if __name__ == '__main__':
    main()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/

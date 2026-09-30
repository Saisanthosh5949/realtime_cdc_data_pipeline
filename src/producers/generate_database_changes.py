import random,time
import psycopg2
from src.common.config import *

STATES=["TX","CO","CA","NY","FL","WA"]
SEGMENTS=["Consumer","Corporate","Small Business"]

def main():
    conn=psycopg2.connect(host=POSTGRES_HOST,port=POSTGRES_PORT,dbname=POSTGRES_DB,user=POSTGRES_USER,password=POSTGRES_PASSWORD)
    conn.autocommit=True
    cur=conn.cursor()
    seq=1
    print("Generating INSERT/UPDATE/DELETE activity. Ctrl+C to stop.")
    try:
        while True:
            roll=random.random()
            if roll < .55:
                email=f"customer{int(time.time()*1000)}{seq}@example.com"
                cur.execute("""INSERT INTO customers(first_name,last_name,email,state,customer_segment)
                               VALUES(%s,%s,%s,%s,%s)""",
                            (f"First{seq}",f"Last{seq}",email,random.choice(STATES),random.choice(SEGMENTS)))
                print("INSERT",email)
            elif roll < .9:
                cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
                row=cur.fetchone()
                if row:
                    cur.execute("""UPDATE customers SET state=%s, customer_segment=%s, updated_at=CURRENT_TIMESTAMP
                                   WHERE customer_id=%s""",(random.choice(STATES),random.choice(SEGMENTS),row[0]))
                    print("UPDATE",row[0])
            else:
                cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
                row=cur.fetchone()
                if row:
                    cur.execute("DELETE FROM customers WHERE customer_id=%s",(row[0],))
                    print("DELETE",row[0])
            seq+=1
            time.sleep(.5)
    except KeyboardInterrupt:
        pass
    finally:
        cur.close(); conn.close()
if __name__=="__main__": main()

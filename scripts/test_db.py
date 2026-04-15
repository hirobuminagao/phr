from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST"),
    port=int(os.getenv("MYSQL_PORT")),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE"),
)

cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM exam_item_master")
print("exam_item_master count:", cur.fetchone()[0])

cur.execute("SELECT * FROM exam_item_master LIMIT 5")
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()
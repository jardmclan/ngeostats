import db_connect
import math
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, exc
import time

Base = declarative_base()



class GNTrans(Base):
    __tablename__="gene_name_translation"
    gene_name = Column(String, primary_key=True)
    gene_id = Column(String, primary_key=True)
    tax_id = Column(String)

row = 0
def insert_test(n):
    global row
    fields_list = []
    for i in range(n):
        row = row + 1
        test_fields = {
            "gene_name": "test_%d" % row,
            "gene_id":  "test_%d" % row,
            "tax_id":  "test_%d" % row 
        }
        fields_list.append(test_fields)
    start = time.time()
    with engine.begin() as con:
        con.execute(GNTrans.__table__.insert(), fields_list)
    print(time.time() - start)


engine = db_connect.get_db_engine()
try:
    
    insert_test(10000)
    insert_test(100000)
    
finally:
    db_connect.cleanup_db_engine()

# def split_arr(arr, n):
#     split = []
#     length = len(arr)
#     avg = length / n
#     lower = math.floor(avg)
#     upper = lower + 1

#     #baseline all lower (n chunks of size lower), how many remain? each of the remaining elements adds one to a group (upper group)
#     n_upper = length - (n * lower)
#     #rest are lower
#     n_lower = n - n_upper
#     p = 0
#     #generate groups
#     for i in range(n_upper):
#         split.append(arr[p:p + upper])
#         p += upper

#     for i in range(n_lower):
#         split.append(arr[p:p + lower])
#         p += lower


#     return split


# arr = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# parts = 1
# print(split_arr(arr, parts))


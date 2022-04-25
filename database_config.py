import pymysql as sql
import pymongo as mongo


class ConfigDatabase():


    def __init__(self, database, table, host = "localhost", user = "root", password = "vishal"):
        
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        self.connSql = sql.connect(
            host = self.host,
            user = self.user,
            passwd = self.password,
            database = self.database
        )
        self.connMongo = mongo.MongoClient(f"mongodb://{host}:27017/")
        self.dbmongo = self.connMongo[self.db]
        self.crsrSql = self.connSql.cursor(sql.cursors.DictCursor)


    def fetchResultsfromSql(self, fields = [], conditions = {} ,start=0, end=1):

        fieldtofetch = ",".join(fields) if fields else "*"
        cond = [f"{key}={value}" for key,value in conditions if conditions.items()]
        cond = f'where {",".join(cond)} ' if cond else ""
        self.crsrSql.execute(f"select {fieldtofetch} from {self.table} {cond} limit {start},{end}")
        results = self.crsrSql.fetchall()
        return results


    def insertItemToSql(self, item):

        try:
            field_list = []
            value_list = []
            for field in item:
                field_list.append(str(field))
                value_list.append(str(item[field]).replace("'", "’"))
            fields = ','.join(field_list)
            values = "','".join(value_list)
            insert_db = f"insert into {self.table}" + "( " + fields + " ) values ( '" + values + "' )"
            print(insert_db)
            self.crsr.execute(insert_db)
            self.con.commit()
            print(f"Item Successfully Inserted...")
        except Exception as e:
            print(str(e))

    
    def insertItemToMongo(self, item):

        try:
            self.dbmongo[self.table].insert_one(item)
        except Exception as e:
            print(e)


    def updateStatusSql(self, item):

        try:
            update = f"update {self.table} set status='done' where cat = '{item['StoreId']}'"
            self.crsr.execute(update)
            self.con.commit()
        except Exception as e:
            print(e)
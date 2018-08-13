import psycopg2
from psycopg2.sql import SQL, Identifier


class Cluster:
    
    def __init__(self, dbname, user, password, host='localhost', port=5439):
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
        except:
            raise
    
    def get_tables(self, schema):
        cur = self.conn.cursor()
        cur.execute(
            "select distinct(tablename) from pg_table_def where schemaname = %(schemaname)s;",
            {'schemaname': schema},
        )
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]
    
    def get_table_columns(self, schema, table):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT c.column_name, c.udt_name, c.data_type
                from pg_catalog.pg_statio_all_tables as st
                inner join pg_catalog.pg_description pgd on (pgd.objoid=st.relid)
                right outer join information_schema.columns c
                    on (pgd.objsubid=c.ordinal_position
                    and  c.table_schema=st.schemaname
                    and c.table_name=st.relname)
                where table_schema =  %(table_schema)s and table_name = %(table_name)s;""",
            {'table_schema': schema, 'table_name': table},
        )
        rows = cur.fetchall()
        cur.close()
        return [{'name':r[0],'udt_name':r[1],'data_type':r[2]} for r in rows]
    
    def get_table_data(self, schema, table):
        cur = self.conn.cursor()
        cur.execute(SQL("SELECT * FROM {}.{};").format(Identifier(schema), Identifier(table)))
        rows = cur.fetchall()
        cur.close()
        return rows
    
    def disconnect(self):
        try:
            self.conn.close()
        except:
            pass

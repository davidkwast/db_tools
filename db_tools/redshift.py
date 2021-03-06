import unicodedata
from datetime import datetime, date

import psycopg2
from psycopg2.sql import SQL, Identifier


REDSHIFT_TO_POSTGRE = {
    'character varying':           'text',
    'character':                   'text',
}


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
            self.pk = 1
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
    
    def get_table_row_count(self, schema, table, where_clause=None):
        cur = self.conn.cursor()
        sql = "SELECT COUNT(*) FROM {}.{}"
        if where_clause:
            sql += ' WHERE ' + where_clause
        sql += ';'
        cur.execute(SQL(sql).format(Identifier(schema), Identifier(table)))
        result = cur.fetchone()
        return result[0]
    
    def get_table_data__generator(self, schema, table, offset=None, limit=None, select_columns=None, where_clause=None):
        cur = self.conn.cursor()
        sql = "SELECT "
        if select_columns:
            sql += ','.join(select_columns)
        else:
            sql += "*"
        sql += " FROM {}.{}"
        if where_clause:
            sql += ' WHERE ' + where_clause
        if offset is not None and limit is not None:
            sql += ' OFFSET %s LIMIT %s'
        sql = SQL(sql).format(Identifier(schema), Identifier(table))
        if offset is not None and limit is not None:
            cur.execute(sql, (offset, limit))
        else:
            cur.execute(sql)
        for row in cur:
            yield row, cur.rowcount
        cur.close()
    
    def get_table_schema__dict(self, schema, table, select_columns=None):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT c.column_name, c.udt_name, c.data_type,
				c.is_nullable, c.character_maximum_length,
				c.numeric_precision, c.numeric_scale
                from pg_catalog.pg_statio_all_tables as st
                inner join pg_catalog.pg_description pgd on (pgd.objoid=st.relid)
                right outer join information_schema.columns c
                    on (pgd.objsubid=c.ordinal_position
                    and  c.table_schema=st.schemaname
                    and c.table_name=st.relname)
                where table_schema =  %(table_schema)s and table_name = %(table_name)s
                order by ordinal_position;""",
            {'table_schema': schema, 'table_name': table},
        )
        rows = cur.fetchall()
        cur.close()
        data = [{
                'name': unicodedata.normalize('NFKD', r[0].replace(' ','_').replace('%','percent')).encode('ASCII', 'ignore').decode(),
                'udt_name': r[1],
                'data_type': r[2],
                'is_nullable': r[3],
                'character_maximum_length': r[4],
                'numeric_precision': r[5],
                'numeric_scale': r[6],
            } for r in rows]
        columns = {x['name']:x for x in data}
        # filter columns from schema
        if select_columns:
            data = [columns[x['name']] for x in data if x['name'] in select_columns]
        return data
    
    
    def get_table__sql_create(self, schema, table, pg_schema=None, select_columns=None):
        
        self.pk = 1
        
        if pg_schema is None:
            pg_schema = schema
        
        data = self.get_table_schema__dict(schema, table, select_columns)
        if not data:
            raise ValueError('database "{}" not found'.format(schema + '.' + table))
        
        schema_table = pg_schema + '.' + table
        sql = "-- TABLE {}\n".format(schema_table)
        
        schema_table = schema_table.replace('-','_')
        sql += "DROP TABLE IF EXISTS {};\n".format(schema_table)
        sql += "CREATE TABLE {} (\n".format(schema_table)
        
        # injects primary key
        sql += '    id serial PRIMARY KEY,\n'
        
        for count, column in enumerate(data):
            sql += ' '*4
            
            data_type = column['data_type']
            if data_type in REDSHIFT_TO_POSTGRE:
                pg_data_type = REDSHIFT_TO_POSTGRE[data_type]
            else:
                pg_data_type = data_type
            
            if pg_data_type == 'numeric':
                pg_data_type = '{}({},{})'.format(pg_data_type, column['numeric_precision'], column['numeric_scale'])
            sql += '{} {}'.format(column['name'], pg_data_type)
            
            # if column['is_nullable'] == 'NO':
            #     sql += ' NOT NULL'
            
            if count < len(data) - 1:
                sql += ','
            
            sql += '\n'
        
        sql += ");\n\n"
        return sql
    
    
    def get_table__sql_dump_data__generator(self, schema, table, offset=None, limit=None, pg_schema=None, select_columns=None, where_clause=None, generate_pk=False):
        
        if pg_schema is None:
            pg_schema = schema
        
        columns_schema = self.get_table_schema__dict(schema, table, select_columns)
        
        first = True
        for row_index, (columns_data, row_count) in enumerate(self.get_table_data__generator(schema, table, offset, limit, select_columns, where_clause)):
        
            sql = ""
            
            if first:
                first = False
                
                sql += "INSERT INTO {} (".format(pg_schema + '.' + table)
                
                if generate_pk:
                    sql += "id, "
                
                for count, column_schema in enumerate(columns_schema):
                    sql += column_schema['name']
                    if count < len(columns_schema) - 1:
                        sql += ', '
                
                sql += ") VALUES \n\n"
            
            else:
                pass
            
            values = []
            if generate_pk:
                values.append(str(self.pk))
            
            for count, (column_schema, row_data) in enumerate(zip(columns_schema,columns_data)):
                
                data_type = column_schema['data_type']
                if data_type in REDSHIFT_TO_POSTGRE:
                    pg_data_type = REDSHIFT_TO_POSTGRE[data_type]
                else:
                    pg_data_type = data_type
                
                value = row_data
                
                if value is not None:
                    if pg_data_type == 'text':
                        
                        if value:
                            value = str(psycopg2.extensions.QuotedString(value.encode('utf-8')))
                        else:
                            value = 'NULL'
                        
                    elif pg_data_type in 'date':
                        value = "'{}'".format(value.isoformat())
                    
                    elif pg_data_type in ['timestamp without time zone','timestamp with time zone']:
                        value = "'{}'".format(value.isoformat())
                    
                    elif pg_data_type in ['integer','real','numeric','double precision','smallint','bigint']:
                        value = str(value)
                    
                    else:
                        print('-'*80)
                        print(column_schema)
                        print(repr(row_data))
                        print(type(row_data))
                        raise ValueError('ERROR: CHECK VALUE CONVERSION')
                
                else:
                    value = 'NULL'
                
                values.append(value)
            
            sql += '(' + ','.join(values) + ')'
            if row_index < row_count - 1:
                sql += ','
            
            yield sql
            
            self.pk += 1
        
        yield '\n;'
    

    def disconnect(self):
        try:
            self.conn.close()
        except:
            pass

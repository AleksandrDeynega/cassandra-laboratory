import logging

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)



from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "crimesdb"
BASIC_TABLE = "mytable"

    
def create_mat_view(session, col_to_group_by=None):
    session.execute(f'''CREATE MATERIALIZED VIEW  IF NOT EXISTS crimes_{col_to_group_by}_event_number AS
                            SELECT event_number, date_time, address_rounded_to_block_number_or_intersection, 
                                patrol_beat, incident_type, incident_type_description, 
                                priority, time, hour, priority_hour
                                FROM {BASIC_TABLE}
                                WHERE {col_to_group_by} IS NOT NULL AND event_number IS NOT NULL
                            PRIMARY KEY ({col_to_group_by}, event_number)''')


def select_count(session, col_to_group_by=None):
    return session.execute(f'''SELECT {col_to_group_by}, count(event_number) 
                    FROM crimes_{col_to_group_by}_event_number GROUP BY {col_to_group_by};''')

def select_count_where(session, col_to_group_by=None, value=None):
    return session.execute(f'''SELECT {col_to_group_by}, count(event_number) 
                    FROM crimes_{col_to_group_by}_event_number WHERE {col_to_group_by} = '{value}';''')


def main():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    create_mat_view(session, col_to_group_by='priority')
    create_mat_view(session, col_to_group_by='hour')
    create_mat_view(session, col_to_group_by='incident_type')

    print(select_count_where(session, col_to_group_by='hour', value='14:00')[0])
    # for row in select_count(session, col_to_group_by='priority'):
    #     log.info(list(row))

    
    # for row in select_count(session, col_to_group_by='hour'):
    #     log.info(list(row))

    
    # for row in select_count(session, col_to_group_by='incident_type'):
    #     log.info(list(row))
if __name__ == "__main__":
    main()
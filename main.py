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


def main():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    log.info("creating keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    session.execute("""DROP TABLE  mytable""")

    log.info("creating table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS mytable (
            event_number                                       text,
            date_time                                          text,
            address_rounded_to_block_number_or_intersection    text,
            patrol_beat                                        text,
            incident_type                                      text,
            incident_type_description                          text,
            priority                                            int,
            time                                               time,
            hour                                               text,
            priority_hour                                      text,
            PRIMARY KEY (event_number)
        )
        """)

    query = SimpleStatement("""
        INSERT INTO mytable (event_number, date_time, address_rounded_to_block_number_or_intersection, 
                            patrol_beat, incident_type, incident_type_description, 
                            priority, time, hour, priority_hour)

        VALUES (%(event_number)s, %(date_time)s, %(address_rounded_to_block_number_or_intersection)s,
                %(patrol_beat)s, %(incident_type)s, %(incident_type_description)s,
                %(priority)s, %(time)s, %(hour)s, %(priority_hour)s)
        """, consistency_level=ConsistencyLevel.ONE)

    prepared = session.prepare("""
        INSERT INTO mytable (event_number, date_time, address_rounded_to_block_number_or_intersection, 
                            patrol_beat, incident_type, incident_type_description, 
                            priority, time, hour, priority_hour)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

    import pandas as pd
    data = pd.read_csv('oak-crimes-for-cassandra.csv').dropna().sample(10000, random_state=42)


    from tqdm import tqdm
    
    for i, row in tqdm(data.iterrows()):
        # log.info("inserting row %d" % i)
        # log.info(tuple(row))
        # session.execute(query, dict(key="key%d" % i, a='a', b='b'))
        session.execute(prepared, tuple(row))

    future = session.execute_async("SELECT * FROM mytable")
    # log.info("key\tcol1\tcol2")
    # log.info("---\t----\t----")

    try:
        rows = future.result()
    except Exception:
        log.exception("Error reading rows:")
        return

    for row in rows:
        log.info(row)

    # session.execute("DROP KEYSPACE " + KEYSPACE)

if __name__ == "__main__":
    main()

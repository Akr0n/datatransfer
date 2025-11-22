import psycopg2
import logging
import sys

def setup_logger(logfile="migrator.log", level=logging.INFO):
    logging.basicConfig(
        filename=logfile,
        format="%(asctime)s %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s",
        level=level
    )

def get_connection(conf):
    return psycopg2.connect(
        host=conf['host'],
        port=conf['port'],
        dbname=conf['database'],
        user=conf['user'],
        password=conf['password']
    )

def migrate_table(source_conf, target_conf, src_schema, src_table, tgt_schema, tgt_table, chunk_size=500):
    setup_logger()
    logging.info(f"Avvio migrazione da {src_schema}.{src_table} a {tgt_schema}.{tgt_table} con chunk={chunk_size}")

    try:
        src_conn = get_connection(source_conf)
        tgt_conn = get_connection(target_conf)
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        src_cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position""", (src_schema, src_table))
        columns = [row[0] for row in src_cur.fetchall()]
        cols_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))

        src_cur.execute(f'SELECT {cols_str} FROM "{src_schema}"."{src_table}"')
        total = 0
        chunk_num = 0
        while True:
            rows = src_cur.fetchmany(chunk_size)
            if not rows:
                break
            chunk_num += 1
            logging.info(f"Chunk {chunk_num}: preparo trasferimento di {len(rows)} record")
            insert_sql = f'INSERT INTO "{tgt_schema}"."{tgt_table}" ({cols_str}) VALUES ({placeholders})'
            try:
                for idx, row in enumerate(rows, start=1):
                    tgt_cur.execute(insert_sql, row)
                tgt_conn.commit()
                total += len(rows)
                logging.info(f"Chunk {chunk_num}: commit completato, totale record trasferiti finora: {total}")
            except Exception as e:
                tgt_conn.rollback()
                logging.error(f"Chunk {chunk_num}: errore durante l’inserimento (rollback eseguito). Dettaglio: {e}")
                # Decidi se vuoi continuare o interrompere:
                #   continue    -> prosegue con chunk successivo
                #   sys.exit(1) -> abortisce tutto
                # Qui si continua, cambia come preferisci

        logging.info(f"Trasferiti in totale {total} record da {src_schema}.{src_table} a {tgt_schema}.{tgt_table}")
        src_cur.close()
        src_conn.close()
        tgt_cur.close()
        tgt_conn.close()
    except Exception as e:
        logging.error(f"Errore grave nella migrazione: {e}")
        sys.exit(1)

if __name__ == '__main__':
    # Configurazioni connessione (personalizza qui o carica da file/ambiente)
    source_conf = {
        'host': 'HOST_ORIGINE',
        'port': 5432,
        'database': 'DB_ORIGINE',
        'user': 'USER_ORIGINE',
        'password': 'PASSWORD_ORIGINE',
    }
    target_conf = {
        'host': 'HOST_DESTINAZIONE',
        'port': 5432,
        'database': 'DB_DESTINAZIONE',
        'user': 'USER_DESTINAZIONE',
        'password': 'PASSWORD_DESTINAZIONE',
    }
    migrate_table(
        source_conf, target_conf,
        src_schema='schema_origine', src_table='tabella_origine',
        tgt_schema='schema_destinazione', tgt_table='tabella_destinazione',
        chunk_size=500  # Regola la dimensione del chunk a piacere
    )

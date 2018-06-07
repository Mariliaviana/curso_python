import sqlite3



def connect_db(db_name, logger):
    try:
        conn = sqlite3.connect(db_name + '.db')
        logger.info(f'Connetion stablished with DB: {db_name}.db')

        return conn


    except sqlite3.OperationalError:
        logger.error(f'Could not connect with {db_name}.db. Make sure the DB name is right')



#definindo função para criar tabela
def create_table(conn, logger):
    c = conn.cursor()

    try:
        c.execute('CREATE TABLE IF NOT EXISTS chip_seq_meta(cell_type_category TEXT, '
                  'cell_type TEXT NOT NULL, cell_type_track_name TEXT, '
                  'cell_type_short TEXT, assay_category TEXT, assay TEXT NOT NULL, '
                  'assay_track_name TEXT NOT NULL, assay_short TEXT, donor TEXT, '
                  'time_point INTEGER, view TEXT, track_name TEXT NOT NULL, '
                  'track_type TEXT NOT NULL, track_density TEXT NOT NULL, '
                  'provider_institution TEXT, source_server TEXT, '
                  'source_path_to_file TEXT, '
                  'server TEXT, path_to_file TEXT, new_file_name TEXT);')

        logger.info('Table from Chip-Seq metadata was created')

    except sqlite3.OperationalError:
        logger.error('Table from Chip-Seq metadata could not be created')
        

#definindo função para inserir dados na tabela

def insert_data(conn, list_of_data, logger):
    c = conn.cursor()

    try:
        with conn: # DOUBTS
            for data in list_of_data:
                c.execute("INSERT INTO chip_seq_meta VALUES(:cell_type_category, :cell_type, :cell_type_track_name, "
                          ":cell_type_short, :assay_category, :assay, :assay_track_name, "
                          ":assay_short, :donor, :time_point, :view, :track_name, "
                          ":track_type, :track_density, :provider_institution, "
                          ":source_server, :source_path_to_file, :server, :path_to_file, "
                          ":new_file_name);", data)
            logger.info('Data was inserted on the DB')

    except sqlite3.OperationalError:
        logger.error('Data could not be inserted')



##definindo os selects:

def select_celltypes(conn, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute('SELECT DISTINCT cell_type FROM chip_seq_meta')
            all_celltypes = c.fetchall()

            logger.info(f'Selected distinct cell types')

            return all_celltypes

    except sqlite3.OperationalError:
        logger.error('Could not select distinct cell types. Check if the table exists or the column name is correct.')
        
        
        
def select_tracks_from_assay(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute('SELECT DISTINCT track_name, track_type, track_density FROM chip_seq_meta '
                      'WHERE assay = :assay', {'assay': assay})
            all_tracks = c.fetchall()

            logger.info(f'Selected all track info from assay {assay}')

            return all_tracks

    except sqlite3.OperationalError:
        logger.error('Could not select tracks from assay. Check if the table exists or the column name is correct.')




def select_track_names_from_assaytrackname(conn, assaytrackname, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute('SELECT DISTINCT track_name FROM chip_seq_meta '
                      'WHERE assay_track_name = :assaytrackname', {'assaytrackname': assaytrackname})
            all_track_names = c.fetchall()

            logger.info(f'Selected all track names from assay tracknames {assaytrackname}')

            return all_track_names

    except sqlite3.OperationalError:
        logger.error('Could not select track and assay names from assay. '
                     'Check if the table exists or the column name is correct.')




def select_celltypes_from_assay(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute('SELECT DISTINCT cell_type FROM chip_seq_meta '
                      'WHERE assay = :assay', {'assay': assay})
            cell_types_from_assay = c.fetchall()

            logger.info(f'Selected all cell types from assay {assay}')

            return cell_types_from_assay

    except sqlite3.OperationalError:
        logger.error('Could not select cell types from assay. '
                     'Check if the table exists or the column name is correct.')
        
        
        

def update_assay(conn, assay, assaynew, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute('UPDATE chip_seq_meta SET assay = :assaynew '
                      'WHERE assay = :assay', {'assaynew': assaynew, 'assay': assay})

            logger.info(f'New assay value: {assaynew} was updated for assay: {assay}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE New assay value: {assaynew} for assay: {assay}')
        
        
        
        

def update_donor(conn, donor, donornew, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute('UPDATE chip_seq_meta SET donor = :donornew '
                      'WHERE donor = :donor', {'donornew': donornew, 'donor': donor})

            logger.info(f'New donor value: {donornew} was updated for donor: {donor}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE New donor value: {donornew} for donor: {donor}')
        
        
        
        

def delete_trackname(conn, trackname, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute('DELETE FROM chip_seq_meta WHERE track_name = :trackname', {'trackname': trackname})

            logger.info(f'Rows where track name is: {trackname} were deleted')

    except sqlite3.OperationalError:
        logger.error(f'Could not delete track name: {trackname}')

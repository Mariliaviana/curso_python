import argparse
import logging
import os
import sys
from db_util.dbmanip import *
from util.loggerinitializer import *


# Initialize log object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
initialize_logger(os.getcwd(), logger)


def main():
    parser = argparse.ArgumentParser(description="A Tool manipulate Chip-set metadata")

    subparsers = parser.add_subparsers(title='actions',
                                       description='valid actions',
                                       help='Use sqlite-python.py {action} -h for help with each action',
                                       dest='command'
                                       )
####Criando a tabela 
    parser_index = subparsers.add_parser('createdb', help='Create database and tables') #primeiro argumento da função

    parser_index.add_argument("--db", dest='db', default=None, action="store", help="The DB name",
                        required=True)

###Inserindo os Dados

    parser_insert = subparsers.add_parser('insert', help='Insert data on tables')

    parser_insert.add_argument("--file",  default=None, action="store", help="CSV file with the data to be inserted",
                        required=True)

    parser_insert.add_argument("--db", default=None, action="store", help="The DB name",
                        required=True)


    ### SELECT CELLTYPES
    parser_select = subparsers.add_parser('select', help='Select fields from the db')
    parser_select.add_argument("--db", default=None, action="store", help="The DB name", required=True)
    parser_select.add_argument("--celltypes", action="store_true", help="Select all cell types", required=False)

    ### SELECT ALL TRACKS FROM AN ASSAY
    parser_select.add_argument("--alltracks", action="store_true", help="Select all track fields from assay", required=False)
    parser_select.add_argument("--assay", action="store", help="Select an assay for filtering", default=False, required=False)

    ### SELECT TRACK NAMES FROM AN ASSAY TRACK NAME
    parser_select.add_argument('--trackname', action='store_true', help='Select all track names from assay', required=False)
    parser_select.add_argument('--assaytrackname', action='store', help='Provide assay tack name for filtering', required=False)

    ###DEFINIR UPDATE
    parser_update = subparsers.add_parser('update', help='Update a field in a db')
    parser_update.add_argument("--db", default=None, action="store", help="The DB name", required=True)
    parser_update.add_argument("--assay", action="store", help="Select assay to update", default=False, required=False)
    parser_update.add_argument("--assaynew", action="store", help="New value for selected assay", default=False, required=False)
    parser_update.add_argument("--donor", action="store", help="Select donor to update", default=False, required=False)
    parser_update.add_argument("--donornew", action="store", help="New value for selected donor", default=False, required=False)
   
    
    ###DEFINIR DELETE
    parser_delete = subparsers.add_parser('delete', help='Delete rows with a specific track_name')
    parser_delete.add_argument("--db", default=None, action="store", help="The DB name", required=True)
    parser_delete.add_argument("--trackname", default=None, action="store", help="Delete rows where this track name appears", required=False)

    ###
    # Parsear em todos os argumentos
    args = parser.parse_args()

    # Caso o db ja tenha sido criado, conectar
    conn = connect_db(args.db, logger)

    # Se o usuario especificar a opcao, criar banco
    if args.command == 'createdb':

        create_table(conn, logger)

    # Inserir os dados do arquivo csv na tabela criada
    elif args.command == 'insert':
        list_of_data = []

        with open(args.file, 'r') as f:
            for line in f:

                line_dict = dict()

                if line.startswith(','):
                    continue

                values = line.strip().split(',')

                # Colocar cada valor em uma chave
                line_dict['cell_type_category'] = values[0]
                line_dict['cell_type'] = values[1]
                line_dict['cell_type_track_name'] = values[2]
                line_dict['cell_type_short'] = values[3]
                line_dict['assay_category'] = values[4]
                line_dict['assay'] = values[5]
                line_dict['assay_track_name'] = values[6]
                line_dict['assay_short'] = values[7]
                line_dict['donor'] = values[8]
                line_dict['time_point'] = values[9]
                line_dict['view'] = values[10]
                line_dict['track_name'] = values[11]
                line_dict['track_type'] = values[12]
                line_dict['track_density'] = values[13]
                line_dict['provider_institution'] = values[14]
                line_dict['source_server'] = values[15]
                line_dict['source_path_to_file'] = values[16]
                line_dict['server'] = values[17]
                line_dict['path_to_file'] = values[18]
                line_dict['new_file_name'] = values[19]

                # Adicionar o dicionario a lista
                list_of_data.append(line_dict)

        insert_data(conn, list_of_data, logger)

    # DEFINIR OS SELECTS
    #Selecionar os cell_types
    elif args.command == 'select' and args.celltypes is not False:

#seleciona os cell_types de um assay
        if args.assay is not False:
            all_celltypes_from_assay = select_celltypes_from_assay(conn, args.assay, logger)

            for i in all_celltypes_from_assay:
                print(i[0])
                
#selecionando todos os cell_types
        elif args.assay is False:
            all_celltypes = select_celltypes(conn, logger)

            for cell in all_celltypes:
                print(cell[0])

        # TODO dar um aviso sempre que o usuario nao colocar o alltracks e o assay juntos
        # SELECIONA ALL TRACKS
    elif args.command == "select" and args.alltracks is not False and args.assay is not False:

        all_tracks = select_tracks_from_assay(conn, args.assay, logger)

        print('\n| Track name\t| Track type\t| Track density')
        for track in all_tracks:
            print('|','\t| '.join(track))

#SELECIONA UM TRACK NAME DE UM ASSAY TRACK NAME
    elif args.command == 'select' and args.trackname is not False and args.assaytrackname is not False:
        all_track_names = select_track_names_from_assaytrackname(conn, args.assaytrackname, logger)

      
        for track_name in all_track_names:
            print(track_name[0])
            
        
    # UPDATE BANCO

    elif args.command == 'update' and (args.assay is not False and args.assaynew is not False):
        update_assay(conn, args.assay, args.assaynew, logger)


    elif args.command == 'update' and (args.donor is not False and args.donornew is not False):
        update_donor(conn, args.donor, args.donornew, logger)


   #DELETE
    elif args.command == 'delete':
        delete_trackname(conn, args.trackname, logger)





    
if __name__ == '__main__':
    main()
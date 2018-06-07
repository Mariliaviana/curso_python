import argparse
import paramiko
import sys
from util.manip import *


def main():

    parser = argparse.ArgumentParser(description='A tool to run commands and manipulate files in a server ')
    subparser = parser.add_subparsers(title='options',
                                      description='Commands to run',
                                      dest='option',
                                      help='Add a {option}, -h for help')

    ####
    parser_run = subparser.add_parser('run', help='Run commands in a server')
    parser_run.add_argument('-c', '--config', dest='config_file', action='store', default=None, required=False)
    parser_run.add_argument('-r', '--runcmd', dest='cmd', action='store', default=None, required=False)

    ####
    parser_get = subparser.add_parser('get', help='Get files from server')
    parser_get.add_argument('-c', '--config', dest='config_file', action='store', default=None, required=False)
    parser_get.add_argument('-f', '--file', dest='file', action='store', default=None, required=False)
    parser_get.add_argument('-d', '--dir', dest='dir', action='store', default=None, required=False)

    ####
    parser_copy = subparser.add_parser('copy', help='Copy files to server')
    parser_copy.add_argument('-c', '--config', dest='config_file', action='store', default=None, required=False)
    parser_copy.add_argument('-f', '--file', dest='file', action='store', default=None, required=False)
    parser_copy.add_argument('-d', '--dir', dest='dir', action='store', default=None, required=False)

    ####
    args = parser.parse_args()

    if args.option == 'run':
        credentials = get_credentials(args.config_file)
        command = str(args.cmd)
        client = connect_to_client_run(credentials)
        run_command(client, command)

    if args.option == 'get' or args.option == 'copy':
        credentials = get_credentials(args.config_file)
        sftp = connect_to_client_get_copy(credentials)

        if args.option == 'get':
            get_file_from_server(sftp, args.file, args.dir)

        if args.option == 'copy':
            copy_file_to_server(sftp, args.file, args.dir)


if __name__ == '__main__':
    main()
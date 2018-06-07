import sys
import paramiko
import os

def copy_file_to_server(sftp, src, dest):

    if os.path.basename(src) not in dest:

        # Make sure the file name is in the destination
        dest = os.path.join(dest, os.path.basename(src))
        print('adding file name to dest')

    try:
        sftp.put(src, dest)
        print(f"File copied to {dest}")

    except paramiko.SSHException as e:
        print('Problem to copy file {src}')
        raise e

    finally:
        sftp.close()



def get_file_from_server(sftp, src, dest):

    if os.path.basename(src) not in dest:

        # Make sure the file name is in the destination
        dest = os.path.join(dest, os.path.basename(src))
        print('adding file name to dest')

    try:
        sftp.get(src, dest)
        print(f"File copied from {src}")

    except paramiko.SSHException as e:
        print('Problem to fetching file from {src}')
        raise e

    finally:
        sftp.close()

def run_command(client, command):

    try:
        ssh_stdin, ssh_stdout, ssh_stderr = client.exec_command(command)

        error = ssh_stderr.read()

        if len(error) > 0:
            print(f'Command {command} hit this error:{error}')

        for i in ssh_stdout:
            print(i.strip())

    except paramiko.SSHException as e:
        print('Something went wrong with the command')
        raise e


def connect_to_client_run(credentials):

    client = paramiko.SSHClient()   # instatiate object
    client.load_system_host_keys()  # Load known host keys
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # Auto add a client that is not in your policy key

    try:
        client.connect(credentials['server'],
                   username=credentials['username'],
                   password=credentials['password'])


    except paramiko.AuthenticationException as e:
        print('ERROR TO AUTHENTICATE: Check your credentials')
        raise e

    return client

def connect_to_client_get_copy(credentials):

    client = paramiko.SSHClient()   # instantiate object
    client.load_system_host_keys()  # Load known host keys
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # Auto add a client that is not in your policy key

    try:
        transport = paramiko.Transport((credentials['server'], 22))
        transport.connect(username=credentials['username'], password=credentials['password'])
        sftp = paramiko.SFTPClient.from_transport(transport)

        return sftp

    except paramiko.SSHException as e:
        print('Connection Failed on Transport ')
        raise e


def get_credentials(config):

    credentials = dict()

    try:
        with open(config, 'r') as file:

            for line in file:

                key, value = line.strip().split('=')

                if len(value)==0:
                    print(f'***** KEY ERROR: The field "{key}" has no value. Check your config ******')
                    raise

                credentials[key] = value

    except IOError as e:
        raise e

    return credentials
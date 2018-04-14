"""
Module contains functions related to SSH.

Most of this script is adapted from:
https://github.com/paramiko/paramiko/blob/master/demos/forward.py
"""
import os
import select
import socketserver

from paramiko import RSAKey, SSHClient, AutoAddPolicy
from paramiko.ssh_exception import SSHException

from bokchoi import utils


class ForwardServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(socketserver.BaseRequestHandler):

    ssh_transport = None
    host_port = None
    remote_port = None

    def __init__(self, request, client_address, server):
        super(Handler, self).__init__(request, client_address, server)

    def handle(self):

        channel = self.ssh_transport.open_channel('direct-tcpip',
                                                  ('localhost', self.remote_port),
                                                  ('localhost', self.host_port))
        if not channel:
            print('Incoming request was rejected')
            return

        while True:
            r, _, _ = select.select([self.request, channel], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if not data:
                    break
                channel.send(data)
            if channel in r:
                data = channel.recv(1024)
                if not data:
                    break
                self.request.send(data)

        channel.close()
        self.request.close()


class SSH(object):

    def __init__(self, private_key_name):
        self.client = SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(AutoAddPolicy())

        self.public_key, self.key_file_path = self._maybe_generate_keys(private_key_name)

    def forward(self, local_port, remote_host, remote_po, user_name):
        """ Sets up port forwarding to remote host
        :param local_port:              Local port to forward to
        :param remote_host:             Remote host
        :param remote_port:             Remote port
        :param user_name:               User to use in ssh connection
        :return:                        -
        """

        print('Connecting to ssh host {}:{} ...'.format(remote_host, remote_po))

        utils.retry(self.client.connect
                    , SSHException
                    , hostname=remote_host
                    , port=22
                    , username=user_name
                    , key_filename=self.key_file_path)

        class SubHandler(Handler):
            ssh_transport = self.client.get_transport()
            host_port = local_port
            remote_port = remote_po

        server = ForwardServer(('localhost', local_port), SubHandler)

        try:
            server.serve_forever()
        except KeyboardInterrupt:
            server.shutdown()
            server.server_close()
            print('Connection closed')

    def _maybe_generate_keys(self, private_key_name):
        """Get private and public keys. Create if not exists.
        :return:                    Public key
        """
        ssh_dir = os.path.join(os.path.expanduser('~'), '.ssh')
        if not os.path.exists(ssh_dir):
            os.makedirs(ssh_dir)

        key_file_path = os.path.join(ssh_dir, private_key_name)
        try:
            priv = RSAKey.from_private_key_file(key_file_path)
        except FileNotFoundError:
            print('Could not find private key. Creating one.')
            priv = RSAKey.generate(2048)
            priv.write_private_key_file(key_file_path)
        pub = priv.get_base64()
        return pub, key_file_path

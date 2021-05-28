from fabric import Connection, SerialGroup
from bigdata.utils.conf_util import load_hosts


class SSHConnection:
    def __init__(self):
        self.hosts = load_hosts()
        self._localhost = 'localhost'
        self._local_connection = None
        self._connections = None

    @property
    def connection(self):
        if not self._local_connection:
            self._local_connection = Connection(self._localhost, user='root', connect_kwargs={'password': ' '})
        return self._local_connection

    @property
    def connections(self):
        if not self._connections:
            self._connections = SerialGroup(*self.hosts, user='root', connect_kwargs={'password': ' '})
        return self._connections

    def run_cmd_local(self, cmd):
        return self.connection.run(cmd)

    def run_cmd_all_nodes(self, cmd):
        return self.connections.run(cmd)
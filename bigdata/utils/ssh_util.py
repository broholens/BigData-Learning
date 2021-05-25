from fabric import Connection, SerialGroup
from bigdata.utils.conf_util import load_hosts


class SSHConnection:
    def __init__(self):
        self.hosts = load_hosts()
        self._localhost = 'localhost'
        self._local_connection = None
        self._connections = None

    def connection(self):
        if not self._local_connection:
            self._local_connection = Connection(self._localhost)
        return self._local_connection

    def connections(self):
        if not self._connections:
            self._connections = SerialGroup(*self.hosts)
        return self._connections

    def run_cmd_local(self, cmd):
        return self._local_connection.run(cmd)

    def run_cmd_all_nodes(self, cmd):
        for cnx in self._connections:
            yield cnx.run(cmd)
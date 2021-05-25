from bigdata.utils.ssh_util import SSHConnection


class Installer:
    def __init__(self):
        self.ssh = SSHConnection()

    def install_os_dependency(self):
        self.ssh.run_cmd_all_nodes('apt install wget')
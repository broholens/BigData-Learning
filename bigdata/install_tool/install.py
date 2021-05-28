import pathlib
from bigdata.utils.ssh_util import SSHConnection


class Installer:
    def __init__(self):
        self.ssh = SSHConnection()
        self.home_path = '/opt/bigdata'

    def install_os_dependency(self):
        self.ssh.run_cmd_all_nodes('apt install wget ssh pdsh openjdk-8-jdk-headless -y')

    def make_home_path(self):
        self.ssh.run_cmd_all_nodes(f'mkdir -p {self.home_path}')

    def install_hadoop(self, version='3.3.0'):
        # download
        filename = f'hadoop-{version}.tar.gz'
        url = f'https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-{version}/{filename}'
        a = self.ssh.run_cmd_all_nodes(f'wget {url} -O {pathlib.Path(self.home_path) / filename}')
        print(a)

        #


if __name__ == '__main__':
    installer = Installer()
    installer.install_os_dependency()
    installer.make_home_path()
    installer.install_hadoop()
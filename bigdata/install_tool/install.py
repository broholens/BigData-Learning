import pathlib
from bigdata.utils.ssh_util import SSHConnection
from bigdata.utils.conf_util import load_hosts_with_name, load_hostname


class Installer:
    def __init__(self):
        self.ssh = SSHConnection()
        self.home_path = '/opt/bigdata'
        self.hadoop_version = '3.3.0'
        self.kafka_version = '2.8.0'
        self.spark_version = '3.1.1'
        self.spark_hadoop_version = '3.2'
        self.components_downloader = {
            'hadoop': {
                'filename': f'hadoop-{self.hadoop_version}.tar.gz',
                'url': f'https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-{self.hadoop_version}/'
            },
            'spark': {
                'filename': f'spark-{self.spark_version}-bin-hadoop{self.spark_hadoop_version}.tgz',
                'url': f'https://www.apache.org/dyn/closer.lua/spark/spark-{self.spark_version}/'
            },
            'kafka': {
                'filename': f'kafka-{self.kafka_version}-src.tgz',
                'url': f'https://www.apache.org/dyn/closer.cgi?path=/kafka/{self.kafka_version}/'
            }
        }

    def install_os_dependency(self):
        self.ssh.run_cmd_all_nodes('apt install wget ssh pdsh openjdk-8-jdk-headless -y')

    def make_home_path(self):
        self.ssh.run_cmd_all_nodes(f'mkdir -p {self.home_path}')

    def download_components(self):
        for name, value in self.components_downloader.items():
            filename, url = value['filename'], value['url']
            # download
            self.ssh.run_cmd_all_nodes(f'wget {url}{filename} -O {pathlib.Path(self.home_path) / filename}')
            # unzip
            self.ssh.run_cmd_all_nodes(f'cd {self.home_path}; tar -zxvf {filename}')

    def config_hosts(self):
        for host in load_hosts_with_name():
            self.ssh.run_cmd_all_nodes(f'echo "{host}" >> /etc/hosts')

    def install_hadoop(self):
        hostnames = "\n".join(load_hostname())
        self.ssh.run_cmd_all_nodes(f'echo {hostnames} > {self.home_path}/hadoop-{self.hadoop_version}/etc/hadoop/workers')
        self.ssh.run_cmd_all_nodes(f'sed -i "s/localhost/z/g" {self.home_path}/hadoop-{self.hadoop_version}/etc/hadoop/core-site.xml')

    def install_hadoop_on_other_nodes(self):
        self.ssh.run_cmd_local(f'zip -qr hadoop-{self.hadoop_version}.zip {self.home_path}/hadoop-{self.hadoop_version}/*')
        self.ssh.run_cmd_other_nodes(None)

    # def install_hadoop(self):
    #     # http://dblab.xmu.edu.cn/blog/2775-2/#:~:text=Hadoop%20%E9%9B%86%E7%BE%A4%E7%9A%84%E5%AE%89%E8%A3%85%E9%85%8D%E7%BD%AE%E5%A4%A7%E8%87%B4%E5%8C%85%E6%8B%AC%E4%BB%A5%E4%B8%8B%E6%AD%A5%E9%AA%A4%EF%BC%9A%20%EF%BC%881%EF%BC%89%E6%AD%A5%E9%AA%A41%EF%BC%9A%E9%80%89%E5%AE%9A%E4%B8%80%E5%8F%B0%E6%9C%BA%E5%99%A8%E4%BD%9C%E4%B8%BA,Master%EF%BC%9B%20%EF%BC%882%EF%BC%89%E6%AD%A5%E9%AA%A42%EF%BC%9A%E5%9C%A8Master%E8%8A%82%E7%82%B9%E4%B8%8A%E5%88%9B%E5%BB%BAhadoop%E7%94%A8%E6%88%B7%E3%80%81%E5%AE%89%E8%A3%85SSH%E6%9C%8D%E5%8A%A1%E7%AB%AF%E3%80%81%E5%AE%89%E8%A3%85Java%E7%8E%AF%E5%A2%83%EF%BC%9B%20%EF%BC%883%EF%BC%89%E6%AD%A5%E9%AA%A43%EF%BC%9A%E5%9C%A8Master%E8%8A%82%E7%82%B9%E4%B8%8A%E5%AE%89%E8%A3%85Hadoop%EF%BC%8C%E5%B9%B6%E5%AE%8C%E6%88%90%E9%85%8D%E7%BD%AE%EF%BC%9B
    #     self.ssh.run_cmd_all_nodes(f'echo "export HADOOP_HOME={self.home_path}/hadoop-{self.hadoop_version}" >> /root/.bashrc')
    #     self.ssh.run_cmd_all_nodes(f'echo "$HADOOP_HOME/bin:$PATH" >> /root/.bashrc')
    #     self.ssh.run_cmd_all_nodes(f'echo "$HADOOP_HOME/sbin:$PATH" >> /root/.bashrc')
    #     self.ssh.run_cmd_all_nodes('source /root/.bashrc')
    #     hostnames = "\n".join(load_hostname())
    #     self.ssh.run_cmd_all_nodes(f'echo {hostnames} > {self.home_path}/hadoop-{self.hadoop_version}/etc/hadoop/workers')
    #     self.ssh.run_cmd_all_nodes(f'sed -i "s/localhost/z/g" {self.home_path}/hadoop-{self.hadoop_version}/etc/hadoop/core-site.xml')
    #
    # def install_spark(self):
    #     self.ssh.run_cmd_all_nodes(f'echo "export SPARK_HOME={self.home_path}/spark-{self.spark_version}" >> /root/.bashrc')
    #     self.ssh.run_cmd_all_nodes(f'echo "SPARK_HOME/bin:$PATH" >> /root/.bashrc')


if __name__ == '__main__':
    installer = Installer()
    # installer.install_os_dependency()
    # installer.make_home_path()
    # installer.config_hosts()
    installer.download_components()
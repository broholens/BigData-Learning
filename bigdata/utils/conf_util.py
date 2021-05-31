import pathlib


def load_hosts_without_name():
    conf_file = pathlib.Path(__file__).parent.parent / 'conf' / 'hosts.cfg'
    with open(conf_file, 'r') as f:
        return [i.strip().split(' ')[0] for i in f.readlines()]


def load_hosts_with_name():
    conf_file = pathlib.Path(__file__).parent.parent / 'conf' / 'hosts.cfg'
    with open(conf_file, 'r') as f:
        return [i.strip() for i in f.readlines()]


def load_hostname():
    conf_file = pathlib.Path(__file__).parent.parent / 'conf' / 'hosts.cfg'
    with open(conf_file, 'r') as f:
        return [i.strip().split(' ')[-1] for i in f.readlines()]


def load_other_hosts_without_name():
    conf_file = pathlib.Path(__file__).parent.parent / 'conf' / 'hosts.cfg'
    with open(conf_file, 'r') as f:
        return [i.strip().split(' ')[-1] for i in f.readlines()][1:]


if __name__ == '__main__':
    print(load_hosts_with_name())
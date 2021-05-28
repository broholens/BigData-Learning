import pathlib


def load_hosts():
    conf_file = pathlib.Path(__file__).parent.parent / 'conf' / 'hosts.cfg'
    with open(conf_file, 'r') as f:
        return [i.strip() for i in f.readlines()]


if __name__ == '__main__':
    print(load_hosts())
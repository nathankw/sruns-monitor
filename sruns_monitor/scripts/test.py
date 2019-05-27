import argparse

from sruns_monitor.monitor import Monitor
from sruns_monitor.sqlite_utils import Db


def get_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-c", "--conf-file", required=True, help="The JSON configuration file.")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    conf_file = args.conf_file
    m = Monitor(conf_file=conf_file)
    m.start()
    #m.db.update_run(name="hello", payload={Db.TASKS_GCP_TARFILE: "bucketfirst_run.tar.gz"})

if __name__ == "__main__":
    main()




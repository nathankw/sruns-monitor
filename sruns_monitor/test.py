
from sruns_monitor.monitor import Monitor
from sruns_monitor.sqlite_utils import Db


m = Monitor(conf_file="scripts/conf.json")
m.db.update_run(name="hello", payload={Db.TASKS_GCP_TARFILE: "bucketfirst_run.tar.gz"})




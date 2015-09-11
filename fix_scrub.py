import sys
import time
import json
import logging
import subprocess
import logging.handlers


my_logger = logging.getLogger('pg_fixer')


def check_output(cmd, log=True):
    if log:
        my_logger.debug("CMD: %r", cmd)

    p = subprocess.Popen(cmd, shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    out = p.communicate()
    code = p.wait()
    return code == 0, out[0]


def get_scrub_inconsistent_pgs():
    """
    ["pg 28.16b is active+clean+inconsistent, acting [190,196,186]",
     "1 scrub errors",
     "pool cache-bench objects per pg (83742) is more than 33.0735 times cluster average (2532)"]
    """
    ok, data = check_output("ceph health detail -f json")

    if not ok:
        my_logger.error("'ceph health detail -f json' fails!")
        return False, []

    try:
        jdata = json.loads(data)
    except:
        my_logger.error("Can't parse 'ceph health detail -f json' result")
        return False, []

    scrub_error_found = any('scrub errors' in line for line in jdata['detail'])
    if not scrub_error_found:
        return True, []

    inconsistent_pgs = [
        line.split()[1]
        for line in jdata['detail']
        if line.startswith('pg') and 'inconsistent' in line
    ]

    if len(inconsistent_pgs) == 0:
        my_logger.warning("scrub error(s) found, but no inconsistent PGs")
        return False, []

    return True, inconsistent_pgs


DEFAULT_SLEEP_INTERVAL = 120
DEFAULT_TRY_CYCLES = 5
ERROR = 1
OK = 0


def main(argv):
    my_logger.setLevel(logging.DEBUG)
    handler = logging.handlers.SysLogHandler(address='/dev/log')
    my_logger.addHandler(handler)

    try:
        if len(argv) != 3:
            print "Usage: {0} try_cycles sleep_interval".fromat(argv[0])
            return ERROR

        try_cycles = int(argv[1])
        sleep_interval = int(argv[2])

        for i in range(try_cycles):
            ok, pgs = get_scrub_inconsistent_pgs
            if not ok:
                return ERROR

            if ok and pgs == []:
                return OK

            for pg in pgs:
                cmd = "ceph pg deep-scrub " + pg
                ok, _ = check_output(cmd)

                if not ok:
                    my_logger.error(cmd + " fails!")
                    return ERROR

            time.sleep(sleep_interval)

        my_logger.error("Can't fix inconsistence")
        return ERROR
    except:
        my_logger.exception("????")
        return ERROR


if __name__ == "__main__":
    exit(main(sys.argv))

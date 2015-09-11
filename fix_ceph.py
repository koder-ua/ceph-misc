import sys
import time
import shutil
import os.path
import subprocess


ceph_data_dir = "/var/lib/ceph/osd/ceph-{id}/current"
ceph_log_file = "/var/log/ceph/ceph-osd.{id}.log"


def get_out(cmd):
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    return proc.communicate()[0]


def is_osd_running(osd_id):
    cmd = '/usr/bin/ceph-osd --cluster=ceph -i {id} -f'.format(id=osd_id)
    return cmd in get_out("ps aux")


def start_osd(osd_id):
    print "Starting OSD"
    get_out("initctl start ceph-osd id={id}".format(id=osd_id))
    time.sleep(15)


def main(argv):
    osd_id = int(argv[1])
    move_folder = argv[2]

    data_dir = ceph_data_dir.format(id=osd_id)
    log_file = ceph_log_file.format(id=osd_id)

    if not os.path.isdir(data_dir):
        print "No data directory found", data_dir
        return 1

    if not os.path.isdir(move_folder):
        print "No move directory found", move_folder
        return 1

    if not os.path.isfile(log_file):
        print "No log file found", log_file
        return 1

    move_log_file = os.path.join(move_folder,
                                 "{id}_move.log".format(id=osd_id))

    if os.path.isfile(move_log_file):
        move_log = open(move_log_file, "r+")
        move_log.seek(0, os.SEEK_END)
    else:
        move_log = open(move_log_file, "w+")

    print "Move log stored in", move_log_file

    start_osd(osd_id)

    while not is_osd_running(osd_id):
        data = open(log_file).readlines()[::-1]

        try:
            pos = data.index("osd/PG.cc: 2572: FAILED assert(r > 0)\n")
        except ValueError:
            print "ERROR: No failed assertion found, but OSD is still down"
            return 1

        if pos + 2 >= len(data):
            print "ERROR: assertion found, but in line", pos, "can't proceed"
            return 1

        err_pg_line = data[pos + 2]

        if '] enter Reset' not in err_pg_line or err_pg_line.count('pg[') != 1:
            print "ERROR: strage error line\n >>>>> ", err_pg_line, "can't proceed"
            return 1

        pg_id = err_pg_line.split('pg[')[1].split("(")[0]

        src_dir = os.path.join(data_dir, pg_id + "_head")
        dst_dir = os.path.join(move_folder, os.path.basename(src_dir))

        print "Moving {0} => {1}".format(src_dir, dst_dir), "..."
        move_log.write("{0!r} {1!r} {2!r}\n".format(pg_id, src_dir, dst_dir))
        move_log.flush()
        shutil.move(src_dir, dst_dir)
        print "Done"

        start_osd(osd_id)

    print "OSD ready!"
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))

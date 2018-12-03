import tempfile
import select
import os
import re
from subprocess import Popen
from subprocess import PIPE
from datetime import datetime
from datetime import date

# define where to copy from and where to
home_path = '/data/modulation/Raw_Data/combined/'
file = 'copy_log_'+str(date.today())+'.txt'
gsiftp_url = 'gsiftp://gridftp.grid.sara.nl:2811'
srm_url = 'srm://srm.grid.sara.nl:8443'
sara_dir = '/pnfs/grid.sara.nl/data/projects.nl/modulations/Tape/raw_data/Nikhef'
grid_path = gsiftp_url+sara_dir
srm_path = srm_url+sara_dir
srm_expr = re.compile(r"\s+-\s+Checksum value:\s+([a-f0-9]+)")
ls_expr = re.compile(r".*(mx_.+\.tar)")
uberftp = '/cvmfs/grid.cern.ch/centos7-ui-4.0.3-1_umd4v3/usr/bin/uberftp'
srm_ls = '/cvmfs/grid.cern.ch/centos7-ui-4.0.3-1_umd4v3/usr/bin/srmls'
processes = []
errors = {}


class process():
    '''This class starts the tar and copying processes, and provides a function
    for verifying the checksum'''

    def __init__(self, folder):
        self.name = folder
        self.temp_dir = tempfile.mkdtemp()
        self.fifo_path = os.path.join(self.temp_dir, 'fifo_' + folder)
        self.fifo = os.mkfifo(self.fifo_path)

        self._tar_process = Popen(['tar', '-C', home_path, '-cf', '-',
                                  folder], stdout=PIPE)
        self._tee_process = Popen(['tee', self.fifo_path],
                                  stdin=self._tar_process.stdout,
                                  stdout=PIPE, stderr=PIPE)
        self._checksum_process = Popen(['xrdadler32'],
                                       stdin=self._tee_process.stdout,
                                       stdout=PIPE, stderr=PIPE)
        self._copy_process = Popen(['globus-url-copy',
                                    'file://'+self.fifo_path, grid_path+'/'+folder+'.tar'],
                                   stdout=PIPE)
        self._grid_checksum_path = os.path.join(srm_path, folder + '.tar')

    def fileno(self):
        'We need to manually define a fileno for select.select'
        return self._copy_process.stdout.fileno()

    def _checksum_data(self):
        checksum = self._checksum_process.stdout.read()
        return checksum[:8]

    def _checksum_grid(self):
        p = Popen([srm_ls, '-l', self._grid_checksum_path], stdout=PIPE,
                  stderr=PIPE)
        o, errors['srm'] = p.communicate()
        checksum = None
        for line in o.split('\n'):
            m = srm_expr.match(line)
            if m:
                checksum = m.group(1)
                break
        return checksum

    def _error(self):
        if self.checksum_data == self.checksum_grid:
            return 0
        else:
            return 1


def get_local_folders():
    'this function checks which local folders need to be copied'
    local_dir = os.listdir(home_path)
    folders = []
    for dir in local_dir:
        if os.path.isdir(home_path+dir):
            folders.append(dir)
    folders.sort()
    folders.pop(-1)
    return folders


def get_grid_tar():
    'this function checks which tar files are already on the grid'
    p = Popen([uberftp, '-ls', grid_path], stdout=PIPE, stderr=PIPE)
    o, errors['uberftp_ls'] = p.communicate()
    files = []
    for line in o.split('\n'):
        m = ls_expr.match(line)
        if m:
            tar_name = m.group(1)
            folder = tar_name.split('.')[0]
            files.append(folder)
    return files


print('present on grid: ', get_grid_tar())

# this generates a list of directories that need to be copied
todolist = []
for f in get_local_folders():
    grid_tar = get_grid_tar()
    if f not in grid_tar:
        todolist.append(f)
print('directories to copy: ', todolist)

# start up the first five processes
total_todo = len(todolist)
for i in range(5):
    new_process = process(todolist[0])
    todolist.pop(0)
    processes.append(new_process)
f = open(file, 'a+')

no_done = 0
# each time a process is done, we start a new one
while no_done != total_todo:
    done, _, _ = select.select(processes, [], [])
    if done:
        for p in done:
            p.checksum_data = p._checksum_data()
            p.checksum_grid = p._checksum_grid()
            p.error = p._error()
            no_done += 1
            # save the properties and of the copied file in a txt file
            f.write('{}\t{}\t{}\t{}\t{}\n'.format(str(datetime.now()),
                    p.name, p.error, p.checksum_grid, p.checksum_data))
            f.flush()
            processes.remove(p)
            if todolist:
                new_process = process(todolist[0])
                todolist.pop(0)
                processes.append(new_process)
f.close()

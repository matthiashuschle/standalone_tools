#!/usr/bin/env python
"""This little program manages launching services within a detached screen session. 
If the service crashes, the session waits for keystroke before exiting, so errors remain visible. 
If a screen session of the desired service already exists, you must choose to not start a new 
instance or replace it with a new one. In the latter case, the existing process and all children 
are killed softly in a INT - TERM - KILL chain."""

import os
import subprocess
import sys
import re
import signal
import time
from collections import namedtuple

MODULES = {
    'airflow_scheduler': {
        'virtual_env': 'ds',
        'command': 'airflow scheduler'
        },
    'airflow_webserver': {
        'virtual_env': 'ds',
        'command': 'airflow webserver',
        'cleanup': lambda x: get_child_pid(x)[:1]
        },
    'jupyter': {
        'virtual_env': 'ds',
        'command': 'jupyter notebook'
        },
    'mongo': {
        'virtual_env': None,
        'command': 'mongo_start.sh'
        },
    'webservices': {
        'virtual_env': 'ds',
        'command': 'uwsgi -C666 -s /tmp/uwsgi.sock -w lof_webserver:app'
        }
    }

Signal = namedtuple('Signal', ['name', 'signal'])
SIGINT = Signal(name='-INT', signal=signal.SIGINT)
SIGTERM = Signal(name='-TERM', signal=signal.SIGTERM)
SIGKILL = Signal(name='-KILL', signal=signal.SIGKILL)


def get_screen_sessions():
    """ get name and pid of active screen sessions.

    :return: {name: pid}
    :rtype: dict
    """
    pattern = re.compile('\s+(\d+)\.(\w+)\s+')
    sessions = subprocess.getoutput('screen -ls')
    return dict((y, x) for x, y in set(pattern.findall(sessions)))


def check_screen_pid(pid):
    """ check if a screen session with given pid is active.

    :param int pid: process ID
    :return: is active?
    :rtype: bool
    """
    return pid in [int(x) for x in get_screen_sessions().values()]


def terminate_attempt(pid, killsignal=SIGINT):
    """ Send kill signal to process, ignoring nonexistent processes.

    :param int pid: process ID
    :param Signal killsignal: Signal as defined in namedtuple
    :return: None
    """
    print('kill', killsignal.name, pid)
    try:
        os.kill(pid, killsignal.signal)
    except ProcessLookupError:
        pass


def check_running_pid(pid):
    """ check if :const:`pid` is in the process list

    :param int pid: process ID
    :return: process exists?
    :rtype: bool
    """
    return pid in [child for parent, child in get_running_gid_pid()]


def wait_killed(pid, maxwait=3., step=0.1):
    """ wait for a process to disappear.

    :param int pid: process ID
    :param float maxwait: maximum waiting time
    :param float step: scan interval
    :return: process disappeared?
    :rtype: bool
    """
    remain = maxwait
    while remain > 0.:
        if not check_running_pid(pid):
            return True
        remain -= step
        time.sleep(step)
    return False


def get_child_pid(pid):
    """ Get process ID of all children.

    :param int pid: process ID
    :return: process IDs of all children
    :rtype: list
    """
    return [child for parent, child in get_running_gid_pid() if parent == pid]


def get_running_gid_pid():
    """ generator for (parent, child) PID tuples.

    :return: PPID, PID
    :rtype: tuple
    """
    processes = subprocess.getoutput('ps -eo "%P %p"')
    pattern = re.compile('\s*(\d+)\s+(\d+)')
    for parent, child in pattern.findall(processes):
        yield int(parent), int(child)


def terminate(module_name):
    """ Terminate a running screen session and invoke clean-up.

    :param module_name: session name
    :return: successful?
    :rtype: bool
    """
    pid = get_pid(module_name)
    try:
        bash_pid = get_child_pid(pid)[0]
        main_pid = get_child_pid(bash_pid)[0]
    except IndexError:
        return False
    cleanup_pids = MODULES[module_name].get('cleanup', lambda x: [])(main_pid)
    # kill main
    if not terminate_loop(main_pid):
        return False
    # cleanup
    for cleanup_pid in cleanup_pids:
        if not terminate_loop(cleanup_pid):
            return False
    # kill bash
    if not terminate_loop(bash_pid):
        return False
    # terminate screen session
    os.system('screen -S {module_name} -X quit'.format(module_name=module_name))
    return not check_screen_pid(pid)


def terminate_loop(pid):
    """ kill a process. First gently, then relentless. No pity, no mercy, no regret.

    :param int pid: PID
    :return: kill successful?
    :rtype: bool
    """
    for ntry in range(3):
        terminate_attempt(pid, SIGTERM)
        if wait_killed(pid, maxwait=2.):
            return True
    # kill harder
    terminate_attempt(pid, SIGKILL)
    if wait_killed(pid, maxwait=0.5):
        return True
    return False


def get_pid(module_name):
    """ Get the PID of  a screen session with given name.

    :param str module_name: name of the screen session
    :return: PID
    :rtype: int
    """
    screen_sessions = get_screen_sessions()
    if module_name in screen_sessions:
        return int(screen_sessions[module_name])
    return None


def main(module_name):
    try:
        module = MODULES[module_name]
    except KeyError:
        return fail()
    pid = get_pid(module_name)
    if pid is not None:
        sys.stderr.write('screen session "%s" is already active.\n' % module_name)
        if input('terminate? (Y/n): ').lower() in ('', 'y', 'yes'):
            if not terminate(module_name):
                sys.stderr.write('terminating failed!\n')
                return None
        else:
            return None
    call = 'screen -S %s -dm bash --norc -c "' % module_name
    if module['virtual_env'] is not None:
        call += 'source activate %s;' % module['virtual_env']
    call += '%s;' % module['command']
    call += 'read -p \\"Press any key to continue... \\" -n1 -s"'
    print(call)
    os.system(call)


def fail():
    print('possible arguments:', MODULES.keys())

if __name__ == '__main__':
    if len(sys.argv) < 2:
        fail()
    else:
        main(*sys.argv[1:])

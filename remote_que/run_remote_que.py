import os
import subprocess
import pandas as pd
import time
import numpy as np
from typing import List, Union, Tuple
import re
import itertools
from shutil import copyfile
import csv

from remote_que.logger import logger
from remote_que.config import QUE_FILE_HEADER, QUE_FILE_HEADER_TYPE
from remote_que.config import DEFAULT_EDITOR, QUE_FILE_HELP
from remote_que.config import get_que_file
from remote_que.config import get_started_file, get_running_file, get_crash_file, get_lock_file
from remote_que.config import get_finished_file, get_crash_start_file

from remote_que.utils import check_if_process_is_running
from remote_que.resource_management import ResourceAvailability
from remote_que.run_process import SingleMachineSlot


STATE_QUE = 0
STATE_CRASHED_START = 1
STATE_CRASHED = 1
STATE_STARTED = 1
STATE_RUNNING = 1
STATE_FINISHED = 1


def edit_que_data(results_folder: str):
    # First remove lock file if it exists (to block QueManager from reading new procs)
    que_file = get_que_file(results_folder)

    lock_file = get_lock_file(results_folder)
    if os.path.isfile(lock_file):
        os.remove(lock_file)

    # -- Can open que file for edit now.

    # If que does not exist, write header file
    if not os.path.isfile(que_file):
        with open(que_file, "w") as f:
            f.write(QUE_FILE_HEADER)

    original_que = read_remote_que(results_folder)

    # Open default editor
    return_code = subprocess.call(f"{DEFAULT_EDITOR} {que_file}", shell=True)

    if return_code == 0:
        # Try read row by row and validate, log not working rows and remove
        try:
            que_data = read_remote_que(results_folder)
        except RuntimeError:
            return_code = 666

    if return_code != 0:
        logger.warning(f"[ERROR] An exception occurred when writing or reading QUE FILE "
                       f"(@ {que_file}). - Current edited file was writen (@ {que_file}_failed)\n"
                       f"--- REVERTING TO PREVIOUS QUE FILE ---\n"
                       f"[ERROR] Fix que file! (error code: {return_code})")
        logger.info(QUE_FILE_HELP)

        # Write current failed file to failed & rewrite old file
        copyfile(que_file, que_file + "_failed")

        # Write back old csv file
        original_que.to_csv(que_file, index=False)
    else:
        # Validate que data. It was  just written
        # TODO validate data

        # Run match special pattern and interpret
        multiply = []
        for que_idx, data in que_data.iterrows():
            cmd = data["shell_command"]

            repl_data = []
            splits = []
            split = cmd
            while True:
                match = re.search(r"\[{([^}]*)}\]", split)

                if match is None:
                    break
                interp = eval(match[1])

                if not isinstance(interp, list):
                    interp = [interp]

                repl_data.append(interp)
                span = match.span()
                splits.append(split[:span[0]])
                split = split[span[1]:]

            if len(repl_data) <= 0:
                continue

            cmds = []
            for combination in itertools.product(*repl_data):
                new_cmd = ""
                for i, sp in enumerate(combination):
                    new_cmd += splits[i] + str(sp)
                if len(combination) < len(splits):
                    new_cmd += splits[-1]
                cmds.append(new_cmd)
            multiply.append((que_idx, cmds))

        # Append new commands
        for que_idx, cmds in multiply:
            for new_cmd in cmds:
                new_idx = len(que_data)
                que_data.loc[new_idx] = que_data.loc[que_idx]
                que_data.loc[new_idx, "shell_command"] = new_cmd

        # Remove multiplied indexes
        for que_idx, _ in multiply:
            que_data = que_data.drop(que_idx)

        for que_idx, data in que_data.iterrows():
            # Allocate new id to newly added command
            if data["command_id"] == 0:
                que_data.loc[que_idx, "command_id"] = int(time.time() * 1000)
                time.sleep(0.1)

        # Write preprocessed new data
        que_data.to_csv(que_file, index=False)

        logger.info("[DONE] New que saved! Here is the que sorted by priority:")
        logger.info(f"\n{que_data.sort_values('que_priority')}")

    # Generate new lock file
    with open(lock_file, "w") as f:
        f.write(str(time.time()))

    return return_code == 0


def read_remote_que(results_folder: str) -> pd.DataFrame:
    que_file = get_que_file(results_folder)

    return_code = 0

    if not os.path.isfile(que_file):
        return_code = 9
    else:
        header_columns = set(QUE_FILE_HEADER.split(","))

        # read and validate line by line csv que
        no_columns = len(header_columns)

        try:
            # Read text lines
            with open(que_file, "r") as f:
                que_lines = f.readlines()

            correct_lines = []
            correct_lines_data = []
            blacklisted_lines = []

            columns = None
            for line in que_lines:
                csv_interpret = list(csv.reader([line]))[0]

                if columns is None:
                    if len(csv_interpret) == no_columns:
                        columns = csv_interpret
                    else:
                        # File is corrupt from header -> must delete all
                        blacklisted_lines = que_file
                        break
                    continue

                # Validate types
                valid = True
                line_data = []
                for i, (k, v) in enumerate(QUE_FILE_HEADER_TYPE.items()):
                    if v != str:
                        r = None
                        try:
                            r = eval(csv_interpret[i])
                        except RuntimeError:
                            pass

                        if isinstance(r, v):
                            valid = False
                            break

                        line_data.append(r)
                    else:
                        line_data.append(v)

                if valid:
                    correct_lines.append(line)
                    correct_lines_data.append(line_data)
                else:
                    blacklisted_lines.append(line)

            # Write blacklisted lines to crash_starts
            if len(blacklisted_lines) > 0:
                write_lines = "\n".join(blacklisted_lines) + "\n"
                logger.warning(f"Cannot read lines: \n: {write_lines}")
                with open(get_crash_start_file(results_folder), "a") as f:
                    f.writelines(blacklisted_lines)

            que_data = pd.DataFrame(correct_lines, columns=columns)
        except RuntimeError:
            return_code = 2

    # check columns
    if set(que_data.columns.values) != header_columns:
        return_code = 3

    # Check first column is int
    if len(que_data) > 0 and que_data["que_priority"].dtype != int:
        return_code = 4

    if return_code > 0:
        print(f"[ERROR] An exception occurred when reading QUE FILE (@{que_file}). \n"
              f"[ERROR] Fix que file! (error code: {return_code})")
        print(QUE_FILE_HELP)

        exit(2)

    return que_data


def append_to_csv_file(data: pd.DataFrame, file_path: str):
    # TODO problem when header does not match -> when things change
    if isinstance(data, pd.Series):
        data = pd.DataFrame([data])

    header = False
    if not os.path.isfile(file_path):
        header = True

    with open(file_path, 'a') as f:
        data.to_csv(f, header=header, index=False)


def sample_gpus(gpus: pd.DataFrame, no_gpus: int) -> Tuple[pd.DataFrame, str, List[str]]:
    machines = gpus.machine.unique()
    machine = np.random.choice(machines)
    gpus = gpus[gpus.machine == machine]
    select = gpus.head(no_gpus)
    return select, machine, list(select["index"].values)


def filter_out_gpus(gpus: pd.DataFrame, filter_gpus: List[pd.DataFrame]):
    filter_out = pd.concat(filter_gpus)["unique_gpu"].values
    gpus = gpus[~gpus["unique_gpu"].isin(filter_out)]
    return gpus


def read_running_que_stats():
    pass


def running_que_stats():
    pass


class QueManager:
    def __init__(self, results_folder: str, loop_sleep: int = 10):
        # Generate remote que folder
        self._que_lock_file = get_lock_file(results_folder)
        self._started_file = get_started_file(results_folder)
        self._crashed_file = get_crash_file(results_folder)
        self._crashed_start_file = get_crash_start_file(results_folder)
        self._running_file = get_running_file(results_folder)
        self._finished_file = get_finished_file(results_folder)

        if os.path.isdir(results_folder):
            logger.info("Result folder exists")
            # clean files to restart que manager
        else:
            os.makedirs(results_folder)

        assert os.path.isdir(results_folder), f"Cannot create results folder {results_folder}"

        self.results_folder = results_folder

        self._command_id_crashes = dict({})
        self._command_id_max_crash = 20
        self._loop_wait_time = loop_sleep

        # Check remote_que is not running and remote que is available
        if not self.remote_que_available or check_if_process_is_running("remote_que"):
            print("[WARNING] Remote que is locked. Other remote_que might be running. \n\t....")
            cmd = input("Are you sure you want to continue? If yes, write <yes> and press Enter "
                       "...")

            if cmd.lower() != "yes":
                exit(1)

        # Initialize resource manager
        self._resource_manager = ResourceAvailability(machines=["0.0.0.0"])

        # Wonderful -> Can open que for edit and start running
        rreturn_code = edit_que_data(self.results_folder)
        assert rreturn_code, "Did not edit correctly que file"

        # Session variables
        self._running_que = []  # type: List[SingleMachineSlot]

    def clean(self):
        if os.path.isfile(self._que_lock_file):
            os.remove(self._que_lock_file)

    @property
    def remote_que_available(self):
        return not os.path.isfile(self._que_lock_file)

    @property
    def remote_que_locked(self):
        return os.path.isfile(self._que_lock_file)

    def start_command(self, que_data: pd.Series, machine: str, gpus: List[str]) -> \
            Tuple[bool, SingleMachineSlot]:
        logger.info(f"Starting: {que_data.to_dict()}")
        proc = SingleMachineSlot(gpus)
        self._running_que.append(proc)

        command = que_data["shell_command"]
        command_id = que_data["command_id"]

        is_running = proc.start_command(command_id, command, que_data)

        return is_running, proc

    def processed_que(self, que_data: pd.Series, from_state: int, to_state: int):
        command_id = que_data["command_id"]

        self._command_id_crashes.pop(command_id, None)

    def run_que(self):
        resource_m = self._resource_manager

        while True:
            if not self.remote_que_locked:
                time.sleep(1)

            que_data = read_remote_que(self.results_folder)
            que_data = que_data.sort_values("que_priority")

            # -- Check all procs in que (ordered by priority) and see if any can be started
            crashed_start_procs = []
            started_procs = []
            blocked_gpus = []
            started_true_procs = []

            for qi, qdata in que_data.iterrows():
                necessary_resource = qdata["preferred_resource"]
                no_gpus = necessary_resource["no_gpus"]
                command_id = qdata["command_id"]

                try:
                    available_gpus = resource_m.get_availability(necessary_resource)
                except RuntimeError as e:
                    logger.warning(f"[ERROR] Crashed availability {e}:: {qdata}")
                    # After max attempt to start process move it to crashed
                    if command_id in self._command_id_crashes:
                        self._command_id_crashes[command_id] += 1
                        if self._command_id_crashes[command_id] > self._command_id_max_crash:
                            crashed_start_procs.append(qi)
                            self._command_id_crashes.pop(command_id)
                    else:
                        self._command_id_crashes[command_id] = 1

                    continue

                if len(available_gpus) <= 0:
                    continue

                # Filter already blocked resources
                available_gpus = filter_out_gpus(available_gpus, blocked_gpus)

                # Sample no_gpus
                gpu_sample, machine, gpus = sample_gpus(available_gpus, no_gpus)

                if len(gpus) != no_gpus:
                    logger.warning(f"[ERROR] Selecting available gpus did not work "
                                   f"{available_gpus} - {no_gpus}")
                    continue

                start_result, last_proc = self.start_command(que_data, machine, gpus)
                logger.info(f'STARTED proc: {que_data["command_id"]} - success '
                            f'{start_result} - ({que_data.to_dict()})')

                started_procs.append(qi)
                started_true_procs.append(last_proc)
                blocked_gpus.append(gpu_sample)

            # -- Wait until currently started procs confirm start
            for proc in started_true_procs:
                # Wait for proc to start - depends which heuristic
                proc.wait_start()
                if proc.crashed:
                    proc_id = proc.id
                    # get que id
                    for qi, row in que_data.loc[started_procs].iterrows():
                        if row["command_id"] == proc_id:
                            crashed_start_procs.append(proc_id)

                    crashed_start_procs.append(proc)

            # -- Clean que_data and write what has been processed
            append_to_csv_file(que_data.loc[started_procs], self._started_file)
            append_to_csv_file(que_data.loc[crashed_start_procs], self._crashed_start_file)

            # -- Clean que_data
            for sqi in started_procs:
                if sqi in que_data:
                    self.processed_que(que_data.loc[sqi], STATE_QUE, STATE_STARTED)
                    que_data = que_data.drop(sqi)

            for cqi in crashed_start_procs:
                if cqi in que_data:
                    self.processed_que(que_data.loc[cqi], STATE_QUE, STATE_CRASHED)
                    que_data = que_data.drop(cqi)

            # -- Add new running procs to running file
            for proc in started_true_procs:
                if proc.is_running:
                    append_to_csv_file(pd.DataFrame([proc.que_data]), self._running_file)

            # -- Clean finished / crashed procs (from running que, and running file)
            remove_proc_idx = []
            for ip, proc in enumerate(self._running_que):
                if not proc.is_running:
                    return_code = proc.kill()

                    # Add to finished docs
                    if return_code == 0:
                        append_to_csv_file(pd.DataFrame([proc.que_data]), self._finished_file)
                    else:
                        append_to_csv_file(pd.DataFrame([proc.que_data]), self._crashed_file)
                    logger.info(f'FINISHED proc: {proc.id} - with return code: {return_code} '
                                f' - ({proc.que_data.to_dict()})')

                    # Remove from running file
                    self.remove_running_id_from_file(proc.id)

                    proc.clean()
                    remove_proc_idx.append(ip)

            for ip in remove_proc_idx[::-1]:
                del self._running_que[ip]

            time.sleep(self._loop_wait_time)

            self.consistency_check()

    def remove_running_id_from_file(self, command_id: int):
        if not os.path.isfile(self._running_que):
            return
        # Remove from running que
        # TODO should be cleanner & more save
        running = pd.read_csv(self._running_que)
        running_id = running[running["command_id"] == command_id].index
        for i in running_id:
            running = running.drop(i)
        running.to_csv(self._running_que, index=False)

    def consistency_check(self):
        # TODO Should check running file if procs are still in class (e.g. may have been killed)
        pass


def start_remote_que(remote_que_file: str, results_folder: str,
                     gpu_ids: List[int], procs_per_gpu: Union[int, List[int]]):
    import os
    os.spawnl(os.P_DETACH, 'some_long_running_command')

    que_manager = QueManager(remote_que_file, results_folder, gpu_ids, procs_per_gpu)

    log_file = "test"

    # Get object index
    logger.add_filehandler(log_file)


def argparse_menu():
    import argparse
    from argparse import RawTextHelpFormatter

    parser = argparse.ArgumentParser(
        formatter_class=RawTextHelpFormatter,
        description=f'Start remote que manager... \n\n{QUE_FILE_HELP}'
    )
    parser.add_argument('results_folder', type=str,
                        help='Que manager results folder.')
    parser.add_argument('--loop-sleep', default=10, type=int,
                        help='How many seconds to pause between que checks.')

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = argparse_menu()
    que = QueManager(**args.__dict__)




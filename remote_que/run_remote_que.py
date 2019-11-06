import os
import subprocess
import pandas as pd
import time
import numpy as np
from typing import List, Union
import re
import itertools
from shutil import copyfile

from remote_que.logger import logger
from remote_que.config import QUE_FILE_HEADER, QUE_FILE_NAME, LOCK_FILE_NAME, DEFAULT_RESOURCE
from remote_que.config import DEFAULT_EDITOR, QUE_FILE_HELP
from remote_que.utils import check_if_process_is_running
from remote_que.resource_management import ResourceAvailability
from remote_que.run_process import SingleMachineSlot


def edit_que_data(results_folder: str):
    # First remove lock file if it exists (to block QueManager from reading new procs)
    que_file = os.path.join(results_folder, QUE_FILE_NAME)

    lock_file = os.path.join(results_folder, LOCK_FILE_NAME)
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

    if return_code > 0:
        print(f"[ERROR] An exception occurred when writing or reading QUE FILE (@{que_file}). \n"
              f"[ERROR] Fix que file! (error code: {return_code})")
        print(QUE_FILE_HELP)
        copyfile(que_file, que_file + "_failed")

        # Write back old csv file
        original_que.to_csv(que_file, index=False)

        exit(2)
    else:
    que_data = read_remote_que(results_folder)

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
        que_data.drop(que_idx)

    for que_idx, data in que_data.iterrows():
        # Allocate new id to newly added command
        if data["command_id"] == 0:
            data["command_id"] = int(time.time() * 1000)

    # Write preprocessed new data
    que_data.to_csv(que_file, index=False)

    # Generate new lock file
    with open(lock_file, "w") as f:
        f.write(str(time.time()))

    print("[DONE] New que saved! Here is the que sorted by priority: \n")
    print(que_data.sort_values('que_priority'))

    return True


def read_remote_que(results_folder: str):
    que_file = os.path.join(results_folder, QUE_FILE_NAME)
    return_code = 0

    try:
        que_data = pd.read_csv(que_file)
    except:
        return_code = 2

    # check columns
    header_columns = set(QUE_FILE_HEADER.split())
    if set(que_data.columns) != header_columns:
        return_code = 3

    # Check first column is int
    if que_data["que_priority"].dtype != int:
        return_code = 4

    if return_code > 0:
        print(f"[ERROR] An exception occurred when reading QUE FILE (@{que_file}). \n"
              f"[ERROR] Fix que file! (error code: {return_code})")
        print(QUE_FILE_HELP)

        exit(2)

    return que_data


def sample_gpus(gpus: pd.DataFrame, no_gpus: int) -> List[pd.DataFrame, str, List[str]]:
    machines = gpus.machine.unique()
    machine = np.random.choice(machines)
    gpus = gpus[gpus.machine == machine]
    select = gpus.head(no_gpus)
    return select, machine, list(select["index"].values)


class QueManager:
    def __init__(self, remote_que_file: str, results_folder: str):

        assert os.path.isfile(remote_que_file), f"{remote_que_file} is not a file!"

        # Generate remote que folder
        os.makedirs(results_folder)
        assert os.path.isdir(results_folder), f"Cannot create results folder {results_folder}"

        self.results_folder = results_folder

        self._que_lock_file = os.path.join(results_folder, ".lock_que")

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
        self._running_que = []

    @property
    def remote_que_available(self):
        return not os.path.isfile(self._que_lock_file)

    @property
    def remote_que_locked(self):
        return os.path.isfile(self._que_lock_file)

    def start_command(self, que_data: pd.Series, machine: str, gpus: List[str]) -> bool:
        logger.info(f"Starting: {que_data.to_dict()}")
        proc = SingleMachineSlot(gpus)
        command = que_data["shell_command"]
        command_id = que_data["command_id"]
        proc.start_command(command)
        return True

    def run_que(self):
        que_data = None  # type: pd.DataFrame
        resource_m = self._resource_manager

        while True:
            if self.remote_que_locked:
                que_data = read_remote_que(self.results_folder)
                que_data = que_data.sort_values("que_priority")

                # check all procs in que (ordered by priority) and see if any can be started
                crashed_procs = []
                started_procs = []

                for qi, qdata in que_data.iterrows():
                    necessary_resource = qdata["preferred_resource"]
                    no_gpus = necessary_resource["no_gpus"]

                    try:
                        available_gpus = resource_m.get_availability(necessary_resource)
                    except RuntimeError as e:
                        logger.warning(f"[ERROR] Get availability {e}:: {qdata}")
                        crashed_procs.append(qi)
                        continue

                    if len(available_gpus) <= 0:
                        continue

                    # Sample no_gpus
                    gpu_sample, machine, gpus = sample_gpus(available_gpus, no_gpus)

                    if len(gpus) != no_gpus:
                        logger.warning(f"[ERROR] Selecting available gpus did not work "
                                       f"{available_gpus} - {no_gpus}")
                        continue








def start_remote_que(remote_que_file: str, results_folder: str,
                     gpu_ids: List[int], procs_per_gpu: Union[int, List[int]]):

    que_manager = QueManager(remote_que_file, results_folder, gpu_ids, procs_per_gpu)

    log_file = "test"

    # Get object index
    logger.add_filehandler(log_file)

if __name__ == "__main__":
    edit_que_data("./")




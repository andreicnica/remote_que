import os
import psutil
import subprocess
import pandas as pd
import time
import nvgpu

from typing import List, Union

LOCK_FILE_NAME = ".lock_que"
QUE_FILE_NAME = "que.csv"
DEFAULT_EDITOR = "gedit"

QUE_FILE_HEADER = "que_priority, shell_command, preferred_gpu, num_gpus, user"
QUE_FILE_HELP = f"__QUE FILE HELP__:\n" \
                f"\t Que file should be a parsable comma delimited file with header: \n" \
                f"\t\t{QUE_FILE_HEADER}\n\n" \
                f"\t Preferred gpu can be set to -1, else process will wait for preferred gpu to " \
                f"be available"


def check_if_process_is_running(process_name: str):
    ''' Check if there is any running process that contains the given name processName. '''

    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:
            # Check if process name contains the given name string.
            if process_name.lower() in proc.name().lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
        return False


def edit_que_data(results_folder: str):
    # First remove lock file if it exists (to block QueManager from reading new procs)
    que_file = os.path.join(results_folder, QUE_FILE_NAME)

    lock_file = os.path.join(results_folder, LOCK_FILE_NAME)
    if os.path.isfile(lock_file):
        os.remove(lock_file)

    # Can open que file for edit now.

    # If que does not exist, write header file
    if not os.path.isfile(que_file):
        with open(que_file, "w") as f:
            f.write(QUE_FILE_HEADER)

    # Open default editor
    return_code = subprocess.call(f"{DEFAULT_EDITOR} {que_file}", shell=True)

    que_data = read_remote_que(results_folder)

    if return_code > 0:
        print(f"[ERROR] An exception occurred when writing or reading QUE FILE (@{que_file}). \n"
              f"[ERROR] Fix que file! (error code: {return_code})")
        print(QUE_FILE_HELP)

        exit(2)

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


class QueManager:
    def __init__(self, remote_que_file: str, results_folder: str,
                     gpu_ids: List[int], procs_per_gpu: Union[int, List[int]]):

        assert os.path.isfile(remote_que_file), f"{remote_que_file} is not a file!"

        assert isinstance(gpu_ids, list) and len(
            gpu_ids) > 0, f"GPU ids argument not correct {gpu_ids}"
        assert isinstance(procs_per_gpu, list) or isinstance(procs_per_gpu, int), \
            f"Procs / gpu argument not correct {procs_per_gpu}"

        if isinstance(procs_per_gpu, list):
            assert len(procs_per_gpu) == len(gpu_ids), "len(list(Procs / gpu)) != len(gpu_ids)"

        # Generate remote que folder
        os.makedirs(results_folder)
        assert os.path.isdir(results_folder), f"Cannot create results folder {results_folder}"

        self.results_folder = results_folder
        self.gpu_ids = gpu_ids

        self.available_gpus = [x["index"] for x in nvgpu.gpu_info()]
        assert self.gpu_ids <= self.available_gpus, \
            f"Selected GPUS ({self.gpu_ids}) not included in available GPUS: {self.available_gpus}"

        self._que_lock_file = os.path.join(results_folder, ".lock_que")

        # Check remote_que is not running and remote que is available
        if not self.remote_que_available or check_if_process_is_running("remote_que"):
            print("[WARNING] Remote que is locked. Other remote_que might be running.\n\t....")
            cmd = input("Are you sure you want to continue? If yes, write <yes> and press Enter "
                       "... ")

            # TODO check if there exist any subprocess run_remote
            if cmd.lower() != "yes":
                exit(1)

        # Wonderful -> Can open que and start running
        rreturn_code = edit_que_data(self.results_folder)
        assert rreturn_code, "Did not edit correctly que file"

    @property
    def remote_que_available(self):
        return not os.path.isfile(self._que_lock_file)

    @property
    def remote_que_locked(self):
        return os.path.isfile(self._que_lock_file)

    def run_que(self):
        que_data = None  # type: pd.DataFrame

        while True:
            if self.remote_que_locked:
                que_data = read_remote_que(self.results_folder)
                que_data = que_data.sort_values("que_priority")


def start_remote_que(remote_que_file: str, results_folder: str,
                     gpu_ids: List[int], procs_per_gpu: Union[int, List[int]]):

    que_manager = QueManager(remote_que_file, results_folder, gpu_ids, procs_per_gpu)


if __name__ == "__main__":
    edit_que_data("./")




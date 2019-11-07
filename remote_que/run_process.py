from typing import List
import os
import time
from subprocess import Popen
import pandas as pd

from remote_que.logger import logger
from remote_que.config import DEFAULT_CONFIRM_START_TIMEOUT


class SingleMachineSlot:
    def __init__(self, gpus: List[str], stdout_folder: str, log_start_confirm: str = None,
                 wait_time_start: int = 1, max_wait_start: int = 600):
        self.gpus = ",".join([str(x) for x in gpus])
        self.stdout_folder = stdout_folder

        self._wait_time_start = wait_time_start
        self._max_wait_start = max_wait_start

        if not os.path.isdir(stdout_folder):
            os.makedirs(stdout_folder)

        self._crt_stdout_file = None
        self._crt_stderr_file = None
        self._proc = None
        self._confirmed_start = False
        self._found_start_log = True
        self._log_start_confirm = log_start_confirm
        self._command_id = None
        self._que_data = None

    def start_command(self, command_id: int, command: str, que_data: pd.Series) -> bool:
        if self.is_running:
            return False

        self._command_id = command_id
        self._que_data = que_data

        fld = self.stdout_folder

        self._crt_stdout_file = sof = open(os.path.join(fld, f"proc_{command_id}_out"), "w")
        self._crt_stderr_file = sef = open(os.path.join(fld, f"proc_{command_id}_err"), "w")

        command = f"CUDA_VISIBLE_DEVICES={self.gpus} {command}"
        self._proc = Popen(command, shell=True, stdout=sof, stderr=sef)

        time.sleep(self._wait_time_start)
        return self.is_running

    @property
    def que_data(self) -> pd.Series:
        return self._que_data

    @property
    def confirmed_start(self) -> bool:
        return self._confirmed_start

    def wait_start(self) -> None:
        """ It sets confirmation_set to True, depends how much it waits """
        if self._confirmed_start:
            return

        if self._log_start_confirm is None:
            time.sleep(DEFAULT_CONFIRM_START_TIMEOUT)
            self._confirmed_start = True
            return
        else:
            max_timeot = self._max_wait_start
            start_check = time.time()
            log_start_confirm = self._log_start_confirm

            while time.time() - start_check < max_timeot:
                file_content = []

                try:
                    self._crt_stdout_file.flush()
                    with open(self._crt_stdout_file, "r") as f:
                        file_content = f.readlines()
                except RuntimeError:
                    pass

                for line in file_content:
                    if log_start_confirm in line:
                        self._confirmed_start = True
                        self._found_start_log = True
                        break

                # Process has stopped or not running any more -> we consider it has started :)
                if not self.is_running:
                    self._confirmed_start = True

                if self._confirmed_start:
                    break

                time.sleep(1)

            self._confirmed_start = True

    @property
    def id(self):
        return self._command_id

    @property
    def is_running(self) -> bool:
        if self._proc is None:
            return False

        return self._proc.poll() is None

    @property
    def crashed(self) -> bool:
        if self._proc is None:
            return False

        poll_res = self._proc.poll()
        return self._proc.poll() is not None and poll_res != 0

    @property
    def finished(self) -> bool:
        if self._proc is None:
            return True

        return self._proc.poll() is not None

    def clean(self):
        # called before del proc
        pass

    def kill(self) -> int:
        if self._proc is None:
            return 0

        self._proc.kill()

        try:
            self._crt_stdout_file.flush()
            self._crt_stderr_file.flush()
        except RuntimeError as e:
            logger.warning(f"[SingleMachineSlot] Exception was handled while flushing files ({e})")

        self._crt_stdout_file.close()
        self._crt_stderr_file.close()

        return_code = self._proc.poll()

        self._proc = None
        self._crt_stdout_file = None
        self._crt_stderr_file = None

        return return_code

from typing import List
import os
import time
from subprocess import Popen

from remote_que.logger import logger
from remote_que.config import DEFAULT_CONFIRM_START_TIMEOUT


class SingleMachineSlot:
    def __init__(self, gpus: List[str], stdout_folder: str, log_start_confirm: str = None,
                 wait_time_start: int = 1):
        self.gpus = ",".join([str(x) for x in gpus])
        self.wait_time_start = wait_time_start
        self.stdout_folder = stdout_folder

        if not os.path.isdir(stdout_folder):
            os.makedirs(stdout_folder)

        self._crt_stdout_file = None
        self._crt_stderr_file = None
        self._proc = None
        self._confirmed_start = False
        self._log_start_confirm = log_start_confirm

    def start_command(self, command_id: int, command: str) -> bool:
        if self.is_running:
            return False

        fld = self.stdout_folder

        self._crt_stdout_file = sof = open(os.path.join(fld, f"proc_{command_id}_out"), "w")
        self._crt_stderr_file = sef = open(os.path.join(fld, f"proc_{command_id}_err"), "w")

        command = f"CUDA_VISIBLE_DEVICES={self.gpus} {command}"
        self._proc = Popen(command, shell=True, stdout=sof, stderr=sef)

        time.sleep(self.wait_time_start)
        return self.is_running

    @property
    def confirmed_start(self) -> bool:
        return self._confirmed_start

    @property
    def wait_start(self) -> None:
        if self._log_start_confirm is None:
            time.sleep(DEFAULT_CONFIRM_START_TIMEOUT)
            self._confirmed_start = True
        else:


    @property
    def is_running(self) -> bool:
        if self._proc is None:
            return False

        return self._proc.poll() is None

    @property
    def finished(self) -> bool:
        if self._proc is None:
            return True

        return self._proc.poll() is not None

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

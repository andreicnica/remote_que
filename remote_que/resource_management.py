from typing import List
import pandas as pd
import nvgpu

from remote_que.logger import logger
from remote_que.config import DEFAULT_RESOURCE
from remote_que.utils import get_gpu_pids


class ResourceAvailability:
    def __init__(self, machines: List[str]):
        # TODO should implement for multiple machines
        self.machines = machines

    def get_availability(self, resource: dict):
        resource = DEFAULT_RESOURCE.update(resource)
        # TODO Check resource values types

        if resource["no_gpus"] <= 0:
            return []

        gpus = self.gpu_stats

        if len(gpus) <= 0:
            return gpus

        # You may select preferred gpu index
        if resource["preferred_gpu"] != -1:
            gpus = gpus[gpus["index"] == resource["preferred_gpu"]]

        if len(gpus) <= 0:
            return gpus

        # Select gpu that have less <max_procs_on_gpu> processes running on the GPU already
        if resource["max_procs_on_gpu"] > 0:
            max_pr = resource["max_procs_on_gpu"]
            for machine in gpus.machine.unique():
                machine_select = gpus.machine == machine
                gpu_pids = get_gpu_pids(machine)

                if len(gpu_pids) <= 0:
                    continue

                gpu_pid_cnt = gpu_pids.groupby("index").size()
                rem_gpu_idx = gpu_pid_cnt[gpu_pid_cnt > max_pr].index
                for gpu_idx in rem_gpu_idx:
                    gpus = gpus[~(machine_select & gpus.index == gpu_idx)]
                    if len(gpus) <= 0:
                        return gpus

        # Select GPUS with minimum memory available
        if resource["min_mem"] > 0:
            gpus = gpus[gpus.mem_free > resource["min_mem"]]

        if len(gpus) <= 0:
            return gpus

        # Select machines with a minimum of resource["no_gpus"] available
        no_gpus_machine = gpus.groupby("machine").size()
        sel_machines = no_gpus_machine[no_gpus_machine > 0].index
        gpus = gpus[gpus.index.isin(sel_machines)]
        return gpus

    @property
    def gpu_stats(self) -> pd.DataFrame:
        x = pd.DataFrame.from_dict(nvgpu.gpu_info())
        x["machine"] = 0
        x["mem_free"] = x["mem_total"] - x["mem_used"]
        return x

from typing import List
import pandas as pd
import nvgpu
import numpy as np

from remote_que.config import DEFAULT_RESOURCE
from remote_que.utils import get_gpu_pids


class ResourceAvailability:
    def __init__(self, machines: List[str]):
        # TODO should implement for multiple machines
        self.machines = machines

    def get_availability(self, resource_search: dict) -> pd.DataFrame:
        """
            Dataframe header:
            index type uuid  mem_used  mem_total mem_used_percent  machine  mem_free
        """
        assert isinstance(resource_search, dict), "Check resource availability no  dict"

        resource = DEFAULT_RESOURCE.copy()
        resource.update(resource_search)

        # TODO Check resource values types
        if resource["no_gpus"] <= 0:
            return []

        gpus = self.gpu_stats

        if len(gpus) <= 0:
            return gpus

        # You may select preferred gpu index
        if resource["preferred_gpu"] != -1:
            gpus = gpus[gpus["index"] == str(resource["preferred_gpu"])]

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
                print("start - ")
                print(machine_select)
                print(gpus.index)
                print("===")
                for gpu_idx in rem_gpu_idx:
                    print(1, gpu_idx)
                    print("_----------")
                    print(np.array(gpus.index))
                    print(f"-{gpu_idx}-")
                    print(2, np.array(gpus.index.values) == gpu_idx)
                    print("_----------")
                    print(3, machine_select & (gpus.index == gpu_idx))
                    print(4, ~(machine_select & (gpus.index == gpu_idx)))
                    gpus = gpus[~(machine_select & (gpus.index == gpu_idx))]
                    if len(gpus) <= 0:
                        return gpus

        # Select GPUS with minimum memory available
        if resource["min_free_mem"] > 0:
            gpus = gpus[gpus.mem_free > resource["min_free_mem"]]

        if len(gpus) <= 0:
            return gpus

        # Select machines with a minimum of resource["no_gpus"] available
        no_gpus_machine = gpus.groupby("machine").size()
        sel_machines = no_gpus_machine[no_gpus_machine >= resource["no_gpus"]].index
        gpus = gpus[gpus.index.isin(sel_machines)]

        return gpus

    @property
    def gpu_stats(self) -> pd.DataFrame:
        x = pd.DataFrame.from_dict(nvgpu.gpu_info())
        x["machine"] = 0
        x["mem_free"] = x["mem_total"] - x["mem_used"]
        x["unique_gpu"] = x.apply(lambda y: (y["machine"], y["index"]), axis=1)
        return x


if __name__ == "__main__":
    # test
    resource = ResourceAvailability("test")

    def _test(x):
        print("-" * 150)
        print(x)
        print(resource.get_availability(x))
        print("-" * 150)

    _test({})
    _test({"preferred_gpu": 0})
    _test({"preferred_gpu": 5})
    _test({"max_procs_on_gpu": 1})
    _test({"min_free_mem": 5000})
    _test({"no_gpus": 2})

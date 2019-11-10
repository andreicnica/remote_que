import psutil
import subprocess
import pandas as pd


def check_if_process_is_running(process_name: str) -> bool:
    """ Check if there is any running process that contains the given name processName """

    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:
            # Check if process name contains the given name string.
            if process_name.lower() in proc.name().lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


def get_csv_from_string(out: str) -> pd.DataFrame:
    a = [x for x in out.split("\n") if len(x) > 0]
    a = pd.DataFrame([x.split(",") for x in a])
    return a


def get_gpu_pids(machine: str) -> dict:
    """ Dictionary with list of working pids for each used gpu_id  (FOR COMPUTE PROCS) """

    # TODO implement for external machine command

    # Get pid of Compute processes
    process = subprocess.Popen(
        ['nvidia-smi --query-compute-apps=gpu_uuid,pid,used_memory --format=csv,noheader,nounits'],
        shell=True, stdout=subprocess.PIPE)
    out, err = process.communicate()
    procs = get_csv_from_string(out.decode('ascii'))
    if len(procs) > 0:
        procs.columns = ["gpu_uuid", "pid", "used_memory"]

    # Get gpu gpu_uuid
    process = subprocess.Popen(
        ['nvidia-smi --query-gpu=gpu_uuid,index --format=csv,noheader,nounits'],
        shell=True, stdout=subprocess.PIPE)
    out, err = process.communicate()
    gpus = get_csv_from_string(out.decode('ascii'))
    if len(gpus) > 0:
        gpus.columns = ["gpu_uuid", "index"]

    # Join GPUs on gpu_uuid to get index
    gpus = gpus.set_index("gpu_uuid")
    if len(procs) > 0:
        procs["index"] = gpus.loc[procs["gpu_uuid"].values].values
        procs["machine"] = machine

    return procs



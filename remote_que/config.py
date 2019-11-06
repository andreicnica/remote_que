LOCK_FILE_NAME = ".lock_que"
QUE_FILE_NAME = "que.csv"
DEFAULT_EDITOR = "gedit"

QUE_FILE_HEADER = "que_priority, shell_command, preferred_resource, user, command_id"

QUE_FILE_HELP = f"__QUE FILE HELP__:\n" \
                f"\t Que file should be a parsable comma delimited file with header: \n" \
                f"\t\t{QUE_FILE_HEADER}\n\n" \
                f"\t PREFERRED_RESOURCE: preferred_gpu can be set to -1, else process will wait " \
                f"for preferred_resource to be available\n" \
                f"\t SHELL COMMAND: \n" \
                f"\t\t - can have python code within [{{pattern}}] -> it will be evaluated and \n" \
                f"\t\t\t list elements will distributed to new commands (e.g. [{{{[1,2,3]}}}])\n" \
                f"\t USER: owner of process\n" \
                f"\t COMMAND_ID: leave 0 when adding new line - it will interpreted at runtime"

DEFAULT_RESOURCE = dict({
    "preferred_gpu": -1,  # Index of GPU (or -1 for any gpu)
    "max_procs_on_gpu": 4,  # -1 if any number of processes can run on GPU already
    "min_free_mem": -1,
    "no_gpus": 1,
    # TODO implement selection of machine
})


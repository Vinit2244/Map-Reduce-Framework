# =============================== IMPORTS ===============================
import sys
import os
import argparse
import time
from pathlib import Path
import grpc
from concurrent import futures
import threading
import uuid

master_folder_abs_path = Path(__file__).parent.resolve()

sys.path.append(str(master_folder_abs_path / "../generated"))
import master_pb2
import master_pb2_grpc as master_grpc
import worker_pb2
import worker_pb2_grpc as worker_grpc


# =============================== GLOBALS ===============================
# Colors to make the output look pretty and more readable
RED = '\033[0;31m'
GREEN = '\033[0;32m'
RESET = '\033[0m'

HEARTBEAT_INTERVAL_S = 2                # Time interval in seconds after which a worker is considered dead
master_port = None                      # Port number to use for master server
workers_list_lock = threading.Lock()    # Lock to ensure thread safety while updating the workers list

ALPHABET = "abcdefghijklmnopqrstuvwxyz"


# ============================== CLASSES ================================
# Stores the information about each worker
class Worker:
    def __init__(self, port, ip, _time):
        self.port = port
        self.ip = ip
        self.last_heartbeat_time = _time


# Stores the information about each task (word count or inverted index)
class Task:
    def __init__(self, task_id, task_type, mappers, reducers, file_paths):
        self.task_id = task_id          # Each task has a unique id generated using UUID
        self.task_type = task_type      # Type of task: word_count or inverted_index
        self.mappers = mappers          # List of mappers assigned to the task
        self.reducers = reducers        # List of reducers assigned to the task
        self.file_paths = file_paths    # List of file paths to be processed
        self.started = False            # Denotes if the task has started or not
        self.mappers_completed = [None] * len(mappers)      # 0: Incomplete, 1: Complete
        self.reducers_completed = [None] * len(reducers)    # Denotes the number of reducing tasks assigned (+1 for each assigned task and -1 for each completed task), None for not assigned


# Manages the workers and assigns roles to them
class WorkerManager:
    def __init__(self):
        self.workers = []

    def update_worker_status(self, worker_port, worker_ip, _time):
        # If the worker exists already then just update the time
        with workers_list_lock:
            for worker in self.workers:
                if worker.port == worker_port and worker.ip == worker_ip:
                    worker.last_heartbeat_time = _time
                    break
            else:
                # If the worker does not exist already then add a new worker
                new_worker = Worker(worker_port, worker_ip, _time)
                self.workers.append(new_worker)

    def assign_roles(self, n_reducers):
        with workers_list_lock:
            # First find all the active workers and then assign roles to them
            active_workers = []
            mappers = []
            reducers = []
            for worker in self.workers:
                if time.time() - worker.last_heartbeat_time <= HEARTBEAT_INTERVAL_S:
                    active_workers.append(worker)
            n_mappers = len(active_workers) - n_reducers
            
            # There should be at least 1 mapper
            if n_mappers < 1:
                print(f"{RED}Not enough workers available to perform the task. Required = {n_reducers + 1} minimum, available only = {len(active_workers)}{RESET}")
                return None, None
            
            # Assign roles to the workers (first n_mappers as mappers and the rest as reducers)
            idx = 0
            for active_worker in active_workers:
                if idx < n_mappers:
                    mappers.append(active_worker)
                else:
                    reducers.append(active_worker)
                idx += 1

            return mappers, reducers


# Manages the tasks and updates the completion status of mappers and reducers
class TaskManager:
    def __init__(self):
        self.tasks = []
    
    def add_task(self, task_type, mappers, reducers, file_paths):
        uid = uuid.uuid4()
        task_id = str(uid)
        new_task = Task(task_id, task_type, mappers, reducers, file_paths)
        self.tasks.append(new_task)
        return task_id
    
    def update_mapper_completion(self, task_id, mapper_ip, mapper_port, value):
        for task in self.tasks:
            if task.task_id == task_id:
                for idx, mapper in enumerate(task.mappers):
                    if mapper.ip == mapper_ip and mapper.port == mapper_port:
                        task.mappers_completed[idx] = value
                        task.started = True
                        break
    
    def update_reducers_completion(self, task_id, reducer_ip, reducer_port, value):
        for task in self.tasks:
            if task.task_id == task_id:
                for idx, reducer in enumerate(task.reducers):
                    if reducer.ip == reducer_ip and reducer.port == reducer_port:
                        if task.reducers_completed[idx] is None:
                            task.reducers_completed[idx] = 0
                        task.reducers_completed[idx] += value
                        break
    
    def check_mappers_completion(self, task_id):
        for task in self.tasks:
            if task.task_id == task_id:
                if task.started:
                    for completion_status in task.mappers_completed:
                        if completion_status == 0:
                            return False
                    return True
                else:
                    return False
    
    def check_reducers_completion(self, task_id):
        for task in self.tasks:
            if task.task_id == task_id:
                if task.started:
                    for completion_status in task.reducers_completed:
                        if completion_status != 0 and completion_status is not None:
                            return False
                else:
                    return False
                return True

    def check_task_completion(self, task_id):
        for task in self.tasks:
            if task.task_id == task_id:
                if self.check_mappers_completion(task_id) and self.check_reducers_completion(task_id):
                    return True
                return False

    def get_reducer_ip_port(self, task_id, reducer_idx):
        for task in self.tasks:
            if task.task_id == task_id:
                reducer = task.reducers[reducer_idx]
                return reducer.ip, reducer.port
        return None, None


# ============================== SERVICER ===============================
class MasterServicer(master_grpc.MasterServicer):
    # Listening for heartbeats from the workers
    def Heartbeat(self, request, context):
        worker_port = request.port
        worker_ip = request.ip
        _time = time.time()

        worker_manager.update_worker_status(worker_port, worker_ip, _time)
        return master_pb2.EmptyResponse()

    # Receiving the completion status of the tasks from the workers
    def ReportTaskCompletion(self, request, context):
        worker_port = request.port
        worker_ip = request.ip
        task_id = request.task_id
        worker_role = request.role
        worker_output_file_paths = request.output_file_paths
        worker_work = request.work

        if worker_role == "mapper":
            task_manager.update_mapper_completion(task_id, worker_ip, worker_port, 1)
        elif worker_role == "reducer":
            task_manager.update_reducers_completion(task_id, worker_ip, worker_port, -1)
            for task in task_manager.tasks:
                if task.task_id == task_id:
                    break
        
        # If a mapper has sent completion then forward those paths to reducer to reduce
        if worker_role == "mapper":
            # As soon as a master completes it's work send the paths to the relevant reducers
            for output_file_path in worker_output_file_paths:
                tokenised_path = output_file_path.split("_") # File name format: `<mapper_port>_<task_id>_reducer_<reducer_idx>`
                reducer_idx = int(tokenised_path[-1][:-4])
                reducer_ip, reducer_port = task_manager.get_reducer_ip_port(task_id, reducer_idx)

                # Sending the reduce request to the reducer
                reducer_channel = grpc.insecure_channel(f"{reducer_ip}:{reducer_port}")
                reducer_stub = worker_grpc.WorkerStub(reducer_channel)
                request_obj = worker_pb2.SendDataAndTaskRequest()
                request_obj.role = "reducer"
                letters_per_reducer = len(ALPHABET) // n_reducers

                if reducer_idx == n_reducers - 1:
                    request_obj.work = f"{worker_work} {ALPHABET[letters_per_reducer * reducer_idx::]}"
                else:
                    request_obj.work = f"{worker_work} {ALPHABET[letters_per_reducer * reducer_idx:letters_per_reducer * (reducer_idx + 1)]}"
                
                request_obj.n_reducers = reducer_idx
                task_manager.update_reducers_completion(task_id, reducer_ip, reducer_port, 1)
                request_obj.task_id = task_id
                request_obj.file_paths.extend([output_file_path,])
                request_obj.mapper_ip = worker_ip
                request_obj.mapper_port = worker_port
                reducer_stub.SendDataAndTask(request_obj)
    
        return master_pb2.EmptyResponse()


worker_manager = WorkerManager()
task_manager = TaskManager()


# ============================== FUNCTIONS ==============================
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def wait_for_enter():
    print()
    print("Press Enter to continue...", end="", flush=True)
    while True:
        char = sys.stdin.read(1)    # Reads one character at a time
        if char == "\n":            # Only proceed if Enter is pressed
            break


def find_files_with_extension(folder_path, extension):
    matching_files = []
    folder_path = os.path.abspath(folder_path)
    
    for item in os.listdir(folder_path):
        item_path = f"{folder_path}/{item}"
    
        if os.path.isfile(item_path) and item_path[-len(extension)::].lower() == extension:
            matching_files.append(item_path)
        
        # Recursively find the paths to files within subfolders as well
        elif os.path.isdir(item_path):
            matching_files.extend(find_files_with_extension(item_path, extension))
    
    return matching_files


def check_path(path):
    if path is None:
        folder_path = input("Enter the folder path: ").strip()

        if not os.path.exists(folder_path):      # Check if the folder exists
            print(f"{RED}Invalid path. Please try again.{RESET}")
            return None
        else:
            if not os.path.isdir(folder_path):    # Check if it's a folder
                print(f"{RED}The given path is not a folder. Please try again.{RESET}")
                return None
    else:
        return path


def do_work(work_type, path, n_reducers):
    folder_path = check_path(path)
    if folder_path is None:
        wait_for_enter()
        return
    
    extension = ".txt"

    # Find all the text files in the given folder
    files = find_files_with_extension(folder_path, extension)
    if not files:
        print(f"{RED}No files found with the given extension.{RESET}")
        wait_for_enter()
        return
    
    mappers, reducers = worker_manager.assign_roles(n_reducers)
    if mappers is None or reducers is None:
        wait_for_enter()
        return
    
    task_id = task_manager.add_task(work_type, mappers, reducers, files)

    # Assign the files to the mappers
    total_files = len(files)
    files_per_mapper = total_files // len(mappers)
    remaining_files = total_files % len(mappers) # First 'remaining_files' mappers will get 1 extra file
    n_mappers_with_extra_files = remaining_files

    file_idx_start = 0
    for mapper_idx, mapper in enumerate(mappers):
        if mapper_idx < n_mappers_with_extra_files:
            n_files_to_send = files_per_mapper + 1
        else:
            n_files_to_send = files_per_mapper
        
        file_idx_end = file_idx_start + n_files_to_send # Exclusive
        file_paths_to_send = files[file_idx_start:file_idx_end]
        file_idx_start = file_idx_end

        with grpc.insecure_channel(f"{mapper.ip}:{mapper.port}") as mapper_channel:
            mapper_stub = worker_grpc.WorkerStub(mapper_channel)
            request_obj = worker_pb2.SendDataAndTaskRequest()

            request_obj.role = "mapper"
            request_obj.work = work_type
            request_obj.n_reducers = n_reducers
            request_obj.task_id = task_id
            request_obj.file_paths.extend(file_paths_to_send)
            mapper_stub.SendDataAndTask(request_obj)
            task_manager.update_mapper_completion(task_id, mapper.ip, mapper.port, 1)

    # Wait for the task to get complete
    while not task_manager.check_task_completion(task_id):
        time.sleep(1)
    
    print(f"{GREEN}Task completed successfully.{RESET}")
    wait_for_enter()


# ================================ MAIN =================================
if __name__ == "__main__":
    clear_screen()

    # Added them as arguments as well for ease of testing
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-p", "--path", default=None, type=str, help="Path to the folder containing the text files.")
    argparser.add_argument("-r", "--reducers", default=2, type=int, help="Number of reducers to use.")
    argparser.add_argument("-port", "--port", default=50052, type=int, help="Port number to use for worker registration.")
    
    args = argparser.parse_args()
    path = args.path
    n_reducers = args.reducers
    master_port = args.port

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    master_grpc.add_MasterServicer_to_server(MasterServicer(), server)

    server.add_insecure_port(f"localhost:{master_port}")
    
    server.start()
    print(f"Master server started at port = {master_port}")

    # Wait for the heartbeat thread to start and the workers to register
    time.sleep(HEARTBEAT_INTERVAL_S)

    # Printing Menu
    while True:
        clear_screen()
        
        print("Select task: (type 'exit' to exit)")
        print("  1. Word Count")
        print("  2. Inverted Index")
        print()

        op = input("Enter option: ").strip().lower()
        if n_reducers == -1:
            n_reducers = input("Enter number of reducers: ").strip()
            try:
                n_reducers = int(n_reducers)
                if n_reducers < 1:
                    print(f"{RED}Number of reducers must be greater than 0. Please try again.{RESET}")
                    wait_for_enter()
                    continue
            except:
                print(f"{RED}Invalid number of reducers. Please try again.{RESET}")
                wait_for_enter()
                continue
        if op == 'exit':
            break
        elif op == '1':
            do_work("word_count", path, n_reducers)
        elif op == '2':
            do_work("inverted_index", path, n_reducers)
        else:
            print(f"{RED}Invalid option. Please try again.{RESET}")
            wait_for_enter()
    
    server.wait_for_termination()

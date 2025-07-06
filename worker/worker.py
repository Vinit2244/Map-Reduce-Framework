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
import logging

sys.path.append("generated")
import master_pb2
import master_pb2_grpc as master_grpc
import worker_pb2
import worker_pb2_grpc as worker_grpc


# =============================== GLOBALS ===============================
my_id = None                # ID of this worker (Used for logging)
my_port = None              # Port of this worker    
master_port = None          # Port of the master server
master_ip = None            # IP of the master server

HEARTBEAT_INTERVAL_S = 1    # Interval between sending heartbeats


# =============================== CLASSES ===============================
class WorkerServicer(worker_grpc.WorkerServicer):
    # Function to receive data and task from the master
    def SendDataAndTask(self, request, context):
        logging.info(f"Received data and task request from master")

        my_role = request.role              # I am mapper or reducer
        work = request.work                 # Word count or inverted index
        n_reducers = request.n_reducers     # Note: Denotes reducer index in case of reducer; else number of reducers
        task_id = request.task_id           # Task ID
        file_paths = request.file_paths     # List of file paths I need to process
        mapper_ip = request.mapper_ip       # Mapper IP (Only for reducers)
        mapper_port = request.mapper_port   # Mapper port (Only for reducers)

        # If I am a mapper I need to produce those intermediate key value pairs in my local files
        if my_role == "mapper":
            # Creating empty files for intermediate data storage
            for i in range(n_reducers):
                with open(f"./worker/intermediate_files/{my_port}_{task_id}_reducer_{i}.txt", "w") as f:
                    f.write("")
            
            # Mapping which reducer will produce which words based on beginning alphabet
            # so that based on that the mapper can divide the words amount intermediate files
            alphabet_to_reducer_mapping = {}

            letters_per_reducer = 26 // n_reducers
            extra_letters = 26 % n_reducers
            current_letter = 0

            # The remaining letters will be assigned to the first equal number of reducers
            for i in range(n_reducers):
                num_letters = letters_per_reducer + (1 if i < extra_letters else 0)
                for _ in range(num_letters):
                    alphabet_to_reducer_mapping[chr(ord('a') + current_letter)] = i
                    current_letter += 1

            # Master has given me the path to files (Assuming that the data required is already with me)
            for file_path in file_paths:
                # Reading each file
                with open(file_path, "r") as f:
                    lines = f.readlines()
                    # Going line by line
                    for line in lines:
                        # Going word by word
                        for word in line.strip().lower().split():
                            # If the word is empty
                            if not word:
                                continue
                            # If the word in nto empty
                            with open(f"./worker/intermediate_files/{my_port}_{task_id}_reducer_{alphabet_to_reducer_mapping[word.lower()[0]]}.txt", "a") as f:
                                if work == "word_count":
                                    f.write(f"{word} 1\n")  
                                elif work == "inverted_index":
                                    f.write(f"{word} {file_path}\n")

            # Notify the master about the completion of the task
            master_channel = grpc.insecure_channel(f"{master_ip}:{master_port}")
            master_stub = master_grpc.MasterStub(master_channel)
            request_obj = master_pb2.ReportTaskCompletionRequest()
            request_obj.task_id = task_id
            request_obj.ip = "localhost"
            request_obj.port = my_port
            request_obj.role = my_role
            request_obj.work = work
            request_obj.output_file_paths.extend([str(Path(f"./worker/intermediate_files/{my_port}_{task_id}_reducer_{i}.txt").resolve()) for i in range(n_reducers)])
            master_stub.ReportTaskCompletion(request_obj)

        # If I am a reducer I will read the intermediate files and produce the final output
        elif my_role == "reducer":
            work_split = work.split(" ", 1)

            if len(work_split) < 2:
                logging.error(f"Invalid work format: {work}")
                return worker_pb2.EmptyResponse()
            
            work = work_split[0]
            letters = work_split[1]
            word_measures = {}

            # Read existing values from the output file if it exists
            output_file_path = f"./worker/output_files/reducer_{n_reducers}_{task_id}.txt"

            if os.path.exists(output_file_path):
                try:
                    with open(output_file_path, "r") as f:
                        lines = f.readlines()
                        for line in lines:
                            word, measure = line.strip().split(" ", 1)  # Splitting by first space (As the file path names can have spaces)
                            if work == "word_count":
                                measure = int(measure)
                            elif work == "inverted_index":
                                measure = eval(measure)
                            word_measures[word] = measure        # Int for word count and list for inverted index
                except:
                    pass

            for file_path in file_paths:
                with grpc.insecure_channel(f"{mapper_ip}:{mapper_port}") as channel:
                    mapper_stub = worker_grpc.WorkerStub(channel)
                    request_obj = worker_pb2.GetKeyValuePairsRequest()

                    request_obj.file_path = file_path
                    response = mapper_stub.GetKeyValuePairs(request_obj)

                    for key_value in response.key_value_pairs:
                        key, value = key_value.strip().split(" ", 1)
                        key = key.lower()
                        if work == "word_count":
                            value = int(value)
                        elif work == "inverted_index":
                            value = str(value)

                        if key[0] in letters:
                            if key not in word_measures:
                                if work == "word_count":
                                    word_measures[key] = 0
                                elif work == "inverted_index":
                                    word_measures[key] = []
                            if work == "word_count":
                                word_measures[key] += value
                            elif work == "inverted_index":
                                word_measures[key].append(value)

            with open(output_file_path, "w") as f:
                for word, measure in word_measures.items():
                    if work == "word_count":
                        f.write(f"{word} {measure}\n")
                    elif work == "inverted_index":
                        measure = list(set(measure))
                        f.write(f"{word} {str(measure)}\n")

            # Notify the master about the completion of the task
            master_channel = grpc.insecure_channel(f"{master_ip}:{master_port}")
            master_stub = master_grpc.MasterStub(master_channel)
            request_obj = master_pb2.ReportTaskCompletionRequest()
            request_obj.task_id = task_id
            request_obj.ip = "localhost"
            request_obj.port = my_port
            request_obj.role = my_role
            request_obj.work = work
            request_obj.output_file_paths.extend([str(Path(f"./worker/output_files/reducer_{n_reducers}.txt").resolve()),])
            master_stub.ReportTaskCompletion(request_obj)

        return worker_pb2.EmptyResponse()

    def GetKeyValuePairs(self, request, context):
        file_path = request.file_path
        with open(file_path, "r") as f:
            key_value_pairs = f.readlines()
        response_obj = worker_pb2.GetKeyValuePairsResponse()
        response_obj.key_value_pairs.extend(key_value_pairs)
        return response_obj


# =============================== THREADS ===============================
def send_heartbeat():
    while True:
        try:
            with grpc.insecure_channel(f"{master_ip}:{master_port}") as channel:
                master_stub = master_grpc.MasterStub(channel)
                heartbeat_request = master_pb2.HeartbeatRequest(ip="localhost", port=my_port)

                master_stub.Heartbeat(heartbeat_request)
                logging.info(f"Heartbeat sent from Worker {my_id} to port {master_port}")
        except:
            logging.error(f"Retrying to send heartbeat...")
        time.sleep(HEARTBEAT_INTERVAL_S)  # Wait before sending the next heartbeat


# =============================== FUNCTIONS ===============================
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker_grpc.add_WorkerServicer_to_server(WorkerServicer(), server)

    server.add_insecure_port(f"localhost:{my_port}")
    server.start()
    logging.info(f"Worker {my_id} started on port {my_port}")

    server.wait_for_termination()


# =============================== MAIN ===============================
if __name__ == "__main__":
    clear_screen()
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--id", type=int, required=True)
    argparser.add_argument("--port", type=int, required=True)
    argparser.add_argument("--master_port", type=int, required=True)
    argparser.add_argument("--master_ip", default="localhost", type=str)
    args = argparser.parse_args()

    my_id = args.id
    my_port = args.port
    master_port = args.master_port
    master_ip = args.master_ip

    logging.basicConfig(
        filename=f"./worker/logs/worker_{my_id}.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.start()

    serve()
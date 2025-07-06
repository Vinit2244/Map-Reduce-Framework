# Map-Reduce Framework using gRPC

A distributed Map-Reduce framework implementation using gRPC for communication between master and worker nodes. The framework supports word counting and inverted indexing operations on text files.

## How to Run

### Prerequisites
- Python 3.x
- gRPC Python packages (`grpcio`, `grpcio-tools`)

### Starting the Master Server
```bash
python master/master.py [-p PATH] [-r REDUCERS] [-port PORT]

Arguments:
  -p, --path      Path to the folder containing input text files (compulsory)
  -r, --reducers  Number of reducers to use (default: 2)
  -port, --port   Port number for master server (default: 50052)
```

### Starting Worker Nodes
```bash
python worker/worker.py --id ID --port PORT --master_port MASTER_PORT [--master_ip MASTER_IP]

Arguments:
  --id           Unique identifier for the worker (Make sure to keep it unique for each spawned worker server)
  --port         Port number for this worker
  --master_port  Port number of the master server
  --master_ip    IP address of master server (default: localhost)
```

## Functionality

### Supported Operations
1. **Word Count**: Counts the frequency of each word across all input files
2. **Inverted Index**: Creates an index mapping each word to the list of files it appears in

### Architecture
- **Master Node**: Coordinates task distribution and manages worker nodes
- **Worker Nodes**: Can act as either mappers or reducers based on master's assignment
- **Communication**: Uses gRPC for all inter-node communication
- **Fault Tolerance**: Implements heartbeat mechanism to detect worker failures

### Output Format

#### Word Count
```
word1 count1
word2 count2
...
```

#### Inverted Index
```
word1 [file1.txt, file2.txt, ...]
word2 [file3.txt, file4.txt, ...]
...
```

Note: Each of these mappings will be distributed across multiple files with each of the reducers. As in the paper I am not merging the final outputs in a single file.

## Implementation Details

### Assumptions
1. Input data is provided as a folder containing multiple `.txt` files (or has subfolders which has `.txt` files)
2. Files are of approximately equal size for simplified work distribution (I am directly distributing the files rather than reading and distributing inside content)
3. Each worker has a unique port and IP combination (used as UID)
4. Map workers save intermediate files as `<mapper_port>_<task_id>_reducer_<reducer_idx>`
5. Word distribution among reducers is based on first letter (can be modified to use hash)
6. All words in input files start with alphabetic characters

### Technical Implementation
1. **Task Distribution**:
   - Files are distributed evenly among mappers
   - Words are distributed among reducers based on their first letter

2. **Communication Protocol**:
   - Heartbeat mechanism for worker health monitoring
   - Task assignment and completion reporting
   - File content transfer between nodes

3. **File Management**:
   - Intermediate files for mapper output
   - Final output files for reducer results
   - Logging for debugging and monitoring

## Directory Structure
```
.
├── master/         # Master node implementation
├── worker/         # Worker node implementation
├── proto/          # Protocol buffer definitions
├── generated/      # Generated gRPC code
├── test/           # Test files
└── README.md       # This file
```


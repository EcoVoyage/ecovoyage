# EcoVoyage

EcoVoyage is a Python package for planning eco-friendly travel experiences.

## Features

- Sustainable travel planning
- Carbon footprint calculation
- Eco-friendly accommodation recommendations
- Parallel data downloading with DAG-based workflow

## Installation

```bash
pixi run install
```

## Usage

```python
import ecovoyage

# Example code will go here
```

### Downloading Data

EcoVoyage can download GTFS and OSM data using a directed acyclic graph (DAG) workflow:

```bash
# Download with default settings (3 concurrent workers)
pixi run run -- --download

# Download with 5 concurrent workers
pixi run run -- --download --workers 5
```

## Development

Run tests:

```bash
pixi run test
```

Run the main module:

```bash
pixi run run
```

### DAG-based Workflows

EcoVoyage includes a DAG implementation for managing complex workflows:

```python
from ecovoyage.dag import DAG, Task

# Create a DAG
dag = DAG("my_workflow")

# Create tasks
task1 = Task("task1", lambda: "Hello")
task2 = Task("task2", lambda x: f"{x} World", x=None)

# Add tasks to the DAG
dag.add_task(task1)
dag.add_task(task2)

# Set up dependencies
task2.add_dependency(task1)

# Run initial execution
dag.execute()

# Update task2 parameters with task1 results
task2.kwargs["x"] = task1.result

# Run final execution
results = dag.execute()
print(results["task2"])  # Outputs: "Hello World"
``` 
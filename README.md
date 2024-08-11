# Data Processing Project

This project contains scripts for data extraction, transformation, and loading (ETL). It can be run in two different ways, either using a Jupyter notebook for debugging or a Python script for automated execution.

## Getting Started

### Prerequisites

- Docker
- Docker Compose

## Running the Project

### 1. Using debug.ipynb (for debugging and understanding the code)

1. Start the Docker container:
   ```
   docker-compose up
   ```

2. Open your web browser and navigate to `http://localhost:8888`

3. Open and run `debug.ipynb` from the `notebook/` directory

This method allows you to run the script block by block, helping you understand how the ETL process works.

### 2. Using scripts/main.py (for automated execution)

1. Give execution permissions to the run script:
   ```
   chmod +x run_job.sh
   ```

2. Run the script:
   ```
   ./run_job.sh main.py
   ```

This method will automatically execute the program using the appropriate Docker commands.

## Project Structure

- `notebook/debug.ipynb`: Jupyter notebook for debugging and understanding the ETL process
- `scripts/main.py`: Python script containing the same code as `debug.ipynb`, designed for automated execution
- `run_job.sh`: Shell script containing Docker commands to run `main.py`

## Additional Information

For more details on the Docker commands used to run `scripts/main.py`, please refer to the `run_job.sh` script.

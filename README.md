# Fithub Task Assigment

This project contains scripts for data extraction, transformation, and loading (ETL). It can be run in two different ways, either using a Jupyter notebook for debugging or a Python script for automated execution.

Task Assigment PDF : 

[DE Test Assigment](https://docs.google.com/document/d/105YAr8idwVv-VadDH8Q6qis48So0y9790t5JYCewapc/edit)

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

for a document manual deeper explanation goes to this link : [Google Docs](https://docs.google.com/document/d/1IUDtgsw7sQ8tNkZ165cAWprMvJPHv0hapoGpincXjRw/edit?addon_store)

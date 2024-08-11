FROM jupyter/pyspark-notebook:latest

USER root

# Install additional dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create directories
RUN mkdir /home/jovyan/data /home/jovyan/notebooks /home/jovyan/scripts

# Set permissions
RUN chown -R jovyan:users /home/jovyan/data /home/jovyan/notebooks /home/jovyan/scripts

USER jovyan

# Set the working directory
WORKDIR /home/jovyan

# Expose ports for Jupyter and Spark UI
EXPOSE 8888 4040
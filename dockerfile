# Use the official lightweight Spark image with Python support
FROM apache/spark:4.1.2-python3

# Set the working directory inside the container
WORKDIR /app

# Switch to root user temporarily to install packages and set permissions
USER root

# Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

EXPOSE 8888

# Switch back to the non-root spark user for security
USER spark

# CRITICAL FIX: Override Jupyter's system runtime and config paths 
# so it doesn't look for a home folder at '/nonexistent'
ENV JUPYTER_CONFIG_DIR=/app/.jupyter
ENV JUPYTER_DATA_DIR=/tmp/jupyter_data
ENV JUPYTER_RUNTIME_DIR=/tmp/jupyter_runtime
ENV IVY_HOME=/tmp/.ivy2

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser"]
# Use Python 3.9 as the base image
FROM python:3.9-slim

# Install necessary build dependencies
RUN apt-get update && \
    apt-get install -y gcc libpq-dev git && \
    rm -rf /var/lib/apt/lists/*

# Configure Git with AWS CodeCommit credential helper
RUN git config --global credential.helper '!aws codecommit credential-helper $@' && \
    git config --global credential.UseHttpPath true

# Clone the GitHub repository (replace with your repository URL if needed)
RUN git clone https://github.com/FireCyan/vacquishcovid19_postgres.git /app

# Set the working directory
WORKDIR /app

# Copy requirements.txt into the image
COPY requirements.txt /app/

# checkout to a particular branch
# RUN git checkout docker_dev

# Install Python dependencies globally
RUN pip3 install -r requirements.txt

# Copy the rest of the application code into the image
COPY . /app

# Expose any ports if necessary
EXPOSE 8052

# Start the application
CMD ["python3", "covid_app/app.py"]

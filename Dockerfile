# Use Python 3.7 as the base image
FROM python:3.7-slim

# Install necessary build dependencies
RUN apt-get update && \
    apt-get install -y gcc libpq-dev git && \
    rm -rf /var/lib/apt/lists/*

# Configure Git with AWS CodeCommit credential helper
RUN git config --global credential.helper '!aws codecommit credential-helper $@' && \
    git config --global credential.UseHttpPath true

# Clone the GitHub repository (replace with your repository URL if needed)
RUN git clone https://github.com/FireCyan/vacquishcovid19_postgres.git /app

# checkout to a particular branch
# WORKDIR /app
# RUN git checkout docker_dev

# Set the working directory and copy the application code into the container
WORKDIR /app
COPY . /app

# Install Python dependencies globally
RUN pip3 install -r requirements.txt

# Expose any ports if necessary
EXPOSE 8052

# Start the application
CMD ["python3", "covid_app/app.py"]
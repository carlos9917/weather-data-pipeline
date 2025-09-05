# Use a slim Python base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Quarto
RUN curl -L https://github.com/quarto-dev/quarto-cli/releases/download/v1.4.553/quarto-1.4.553-linux-amd64.deb -o quarto.deb && \
    apt-get install -y ./quarto.deb && \
    rm quarto.deb

# Create a non-root user
RUN useradd -m appuser
USER appuser

# Set working directory
WORKDIR /home/appuser/app

# Copy project files
COPY . .

# Install Python dependencies
RUN /root/.cargo/bin/uv pip install --no-cache-dir -r requirements.txt

# Make entrypoint script executable
COPY entrypoint.sh /home/appuser/app/entrypoint.sh
RUN chmod +x /home/appuser/app/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/home/appuser/app/entrypoint.sh"]

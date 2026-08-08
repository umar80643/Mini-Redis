FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
COPY miniredis miniredis
RUN pip install --no-cache-dir . && useradd -r app && mkdir -p /app/data && chown -R app /app
USER app
EXPOSE 6379 9121
HEALTHCHECK CMD python -c "import socket;s=socket.create_connection(('127.0.0.1',6379),2);s.sendall(b'*1\r\n$4\r\nPING\r\n');assert b'PONG' in s.recv(64)"
CMD ["python","-m","miniredis"]

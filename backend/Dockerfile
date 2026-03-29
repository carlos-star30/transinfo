FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& update-ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

COPY . /app
RUN chmod +x /app/backend/entrypoint.sh

EXPOSE 8000

CMD ["/app/backend/entrypoint.sh"]

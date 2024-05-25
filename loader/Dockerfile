FROM ubuntu:22.04
RUN apt-get update
RUN apt-get install -y sudo
RUN apt-get install -y curl
RUN apt-get install -y lsb-release
RUN apt-get install -y python3.10
RUN apt-get install -y pip
RUN apt-get install -y python3.10-venv
RUN apt-get install -y unixodbc
RUN curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17

RUN mkdir -p /loader_app
WORKDIR /loader_app
COPY requirements.txt .
COPY mtg_transform.py .
COPY main.py .
ENV VIRTUAL_ENV=/loader_app/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN pip install -r requirements.txt

# CMD ["python", "main.py"]

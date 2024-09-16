FROM python:3.8.10

LABEl maintainer "Job Maathuis <j.maathuis@zgt.nl>"

RUN apt-get update && apt-get install -y \
    sudo \
    curl \
    pandoc \
    pandoc-citeproc \
    libcurl4-gnutls-dev \
    libcairo2-dev \
    libxt-dev \
    libssl-dev \
    libssh2-1-dev \
    libssl1.1

# Installing pacakges to get odbc connection to microsoft sql server (drivers, system pacakges, etc.)
RUN sudo su
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN exit
RUN sudo apt-get update && apt install -y apt-utils
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
RUN sudo apt-get remove -y odbcinst 
RUN sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN sudo ACCEPT_EULA=Y apt-get install mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN apt-get update
RUN apt-get install -y vim
SHELL ["/bin/bash", "-c"]

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install schedule==1.1.0

EXPOSE 5555

CMD ["python3", "/app/py/back_end.py"]
FROM redis/redis-stack-server

RUN apt update
RUN apt-get -y install wget
RUN wget https://github.com/redis-field-engineering/redis-smart-cache-cli/releases/download/v0.0.13/redis-smart-cache-cli_Linux_x86_64.tar.gz
RUN tar -xzvf redis-smart-cache-cli_Linux_x86_64.tar.gz
RUN cp smart-cache-cli /usr/local/bin
RUN rm redis-smart-cache-cli_Linux_x86_64.tar.gz

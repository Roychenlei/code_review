FROM python:2.7
MAINTAINER JackonYang <jyang@zippia.com>


# http://stackoverflow.com/questions/23524976/capturing-output-of-python-script-run-inside-a-docker-container
ENV PYTHONUNBUFFERED=0

RUN apt-get update
RUN apt-get install -y supervisor


RUN mkdir ~/.pip/ && echo "[global] \n\
index-url = http://mirrors.aliyun.com/pypi/simple/\n\
\n\
[install]\n\
trusted-host = mirrors.aliyun.com" > ~/.pip/pip.conf

# upgrade pip to latest version
RUN pip install --upgrade pip

RUN pip install lxml==3.8.0
RUN pip install bleach==2.0.0
RUN pip install requests==2.17.3

RUN pip install redis==2.10.5
RUN pip install pymongo==3.5.0
RUN pip install elasticsearch==5.4.0
RUN pip install elasticsearch-dsl==5.3.0
RUN pip install kafka-python==1.3.3

RUN pip install numpy==1.13.1
RUN pip install scipy==0.19.1
RUN pip install scikit-learn==0.19
RUN pip install nltk==3.1

COPY ./requirements.txt /src/requirements.txt
WORKDIR /src

RUN pip install -r requirements.txt

# ADD deploy/supervisor/* /etc/supervisor/conf.d/

# ENTRYPOINT ["/usr/bin/supervisord"] does not work.
# --> "Error: positional arguments are not supported"
# http://stackoverflow.com/questions/22465003/error-positional-arguments-are-not-supported
# CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/supervisord.conf"]

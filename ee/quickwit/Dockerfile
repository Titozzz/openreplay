FROM quickwit/quickwit

COPY *.yaml /quickwit/
COPY entrypoint.sh /quickwit/
COPY consumer.py /quickwit/
COPY requirements.txt /quickwit/
COPY utils /quickwit/utils
WORKDIR /quickwit

RUN apt-get update
RUN apt-get install python3 python3-pip -y
RUN apt-get clean
RUN pip install -r requirements.txt

EXPOSE 7280

ENTRYPOINT ./entrypoint.sh

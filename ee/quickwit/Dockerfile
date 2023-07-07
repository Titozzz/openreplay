FROM quickwit/quickwit

# RUN apk -y update \
# RUN apk add clang pkgconfig openssl-dev cmake \
#        && rm -rf /var/lib/apt/lists/* \
#        && curl -L https://install.quickwit.io | sh \
#	&& mv quickwit-* quickwit \
RUN apt-get -y update \
    && apt-get -y install python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /quickwit
COPY *.yaml /quickwit/
COPY entrypoint.sh /quickwit/
COPY consumer.py /quickwit/
COPY requirements.txt /quickwit/

RUN pip install -r requirements.txt

ENV filter="true" \
	encrypted="false"

EXPOSE 7280

ENTRYPOINT ./entrypoint.sh

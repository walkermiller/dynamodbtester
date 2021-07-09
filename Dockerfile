FROM scratch
COPY dydb /dydbtester
ENV AWS_REGION=us-east-1
ENTRYPOINT ["/dydbtester"]


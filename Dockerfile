FROM golang:alpine as build
RUN apk --no-cache add ca-certificates

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY dydb /dydbtester
ENV AWS_REGION=us-east-1
ENTRYPOINT ["/dydbtester"]


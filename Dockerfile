FROM golang:alpine as build
RUN apk --no-cache add ca-certificates
WORKDIR app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY main.go . 
RUN go build -o dydbtester

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build app/dydbtester /dydbtester
ENV AWS_REGION=us-east-1
ENTRYPOINT ["/dydbtester"]


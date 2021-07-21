FROM golang:alpine as build
RUN apk --no-cache add ca-certificates
ENV CGO_ENABLED=0
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY main.go . 
RUN go build -o /dydbtester

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /dydbtester /dydbtester
ENTRYPOINT ["/dydbtester"]


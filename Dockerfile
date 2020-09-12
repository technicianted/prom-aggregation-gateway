FROM golang:1.14-alpine AS build
WORKDIR /src/
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/prom-aggregation-gateway ./cmd/prom-aggregation-gateway 

FROM scratch
COPY --from=build /bin/prom-aggregation-gateway /
ENTRYPOINT ["/prom-aggregation-gateway"]

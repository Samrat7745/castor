# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=0.3.0
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /castor ./cmd/castor

FROM gcr.io/distroless/static-debian12
COPY --from=build /castor /castor
EXPOSE 10100
# uid 1000 like the Java image
USER 1000:1000
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 CMD ["/castor", "healthcheck"]
ENTRYPOINT ["/castor"]

FROM golang:1.25 AS build
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 go build -trimpath -ldflags '-s -w' -o /out/echo-bot ./main.go

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/echo-bot /echo-bot
USER nonroot
ENTRYPOINT ["/echo-bot"]

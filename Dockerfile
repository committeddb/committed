FROM alpine

WORKDIR /app
ADD committed /app/

ENTRYPOINT ["./committed"]
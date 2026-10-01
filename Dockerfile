# Build a static binary so the runtime image needs no libc or shell.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/minitsdb .
# Created here because the runtime image has no shell to run mkdir. A named
# volume mounted over /data inherits this directory's ownership, which lets
# the non-root runtime user write the WAL and blocks.
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/minitsdb /minitsdb
COPY --from=build --chown=nonroot:nonroot /out/data /data
VOLUME /data
EXPOSE 8080
# Flags given to `docker run` after the image name are appended, so
# `docker run minitsdb -flush 5` works.
ENTRYPOINT ["/minitsdb", "-wal", "/data/wal.log"]

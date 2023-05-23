FROM shoothzj/compile:go AS build
COPY . /opt/perf/compile
WORKDIR /opt/perf/compile
RUN go build -o pf-producer .


FROM shoothzj/base

COPY --from=build /opt/perf/compile/pf-producer /opt/perf/pf-producer

CMD ["/usr/bin/dumb-init", "/opt/perf/pf-producer"]

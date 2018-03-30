# VCD-java-client

### Getting started

Assuming you have a running VCD cluster running locally:

```bash
$ make run
```

or

```bash
$ docker run --net=host --env CLIENTS=3 \
                        --env OPS=10000 \
                        --env CONFLICTS=true \
                        --env ZK=127.0.0.1:2181 \
                        -ti vitorenesduarte/vcd-java-client
```

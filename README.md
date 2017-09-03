# VCD-java-client

### Getting started

Assuming you have VCD running locally on port __6000__:

```bash
$ make run
```

or

```bash
$ docker run --net=host --env HOST=127.0.0.1 \
                        --env PORT=6000 \
                        --env OPS=10000 \
                        --env CONFLICT_PERCENTAGE=100 \
                        -ti vitorenesduarte/vcd-java-client
```

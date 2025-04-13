# Quarkus ORAS OCI Registry

An experimental Quarkus OCI registry using [ORAS Java SDK](https://github.com/oras-project/oras-java) to store content as OCI layout.

Right now only blob push/pull are supported (monolithic POST or POST/PUT).

<p align="left">
<a href="https://oras.land/"><img src="https://oras.land/img/oras.svg" alt="banner" width="200px"></a>
</p>

## Usage

Build

```shell
mvn clean install
```

Build native 

```shell
mvn clean install -Pnative
```

### Run


Dev mode
```shell
quarkus dev
```
Native image
```shell
target/oras-registry-1.0.0-SNAPSHOT-runner
```

Then try to push a blob

```shell
$ oras blob push localhost:8080/test README.md --debug
```

Get the blob (adapt digest)

```shell
http http://localhost:8080/v2/test/blobs/sha256:80fb2e31a8407bbbb34a2950537b659801604772a0e1ee861fe940808006932c
```
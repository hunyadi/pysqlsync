# Oracle dialect for pysqlsync

## Testing

Refer to [Oracle Database Free Release Quick Start](https://www.oracle.com/database/free/get-started/) for setting up Oracle in Docker.

Specifically, pull the Docker image with the command:
```sh
docker pull container-registry.oracle.com/database/free:latest
```

Next, start the Docker container with the command:
```sh
docker run -d --name oracle-db -e "ORACLE_PWD=<?YourStrong@Passw0rd>" -p 1521:1521 container-registry.oracle.com/database/free:latest
```

In order to inspect database startup logs, run:
```sh
docker logs oracle-db
```

Finally, run unit and integration tests.

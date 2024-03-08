# Microsoft SQL Server dialect for pysqlsync

## Testing

Follow the steps of [running a SQL Server Linux container image with Docker](https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-ver16&pivots=cs1-bash) to start testing with Microsoft SQL Server in a Linux/MacOS environment.

Specifically, pull the Docker image with the command:
```sh
docker pull mcr.microsoft.com/mssql/server:2022-latest
```

Next, start the Docker container with the command:

```sh
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=<?YourStrong@Passw0rd>" -p 1433:1433 --name sql1 --hostname sql1 -d mcr.microsoft.com/mssql/server:2022-latest
```

In order to inspect database startup logs, run:
```sh
docker logs sql1
```

Finally, run unit and integration tests.

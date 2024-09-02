set -e

PYTHON=python3

# PostgreSQL and MySQL
TEST_INTEGRATION=1 TEST_POSTGRESQL=1 TEST_MYSQL=1 $PYTHON -m unittest discover

# MySQL
# CONTAINER=mysql-db
# docker ps -q --filter "name=$CONTAINER" | xargs -r docker stop
# docker ps -aq --filter "name=$CONTAINER" | xargs -r docker rm
# docker run -d --name $CONTAINER -p 3306:3306 -e "MYSQL_ROOT_PASSWORD=<?YourStrong@Passw0rd>" -e MYSQL_DATABASE=levente_hunyadi mysql:8.0
# sleep 30
# TEST_INTEGRATION=1 TEST_MYSQL=1 $PYTHON -m unittest discover
# docker stop $CONTAINER && docker rm $CONTAINER

# Oracle
CONTAINER=oracle-db
docker ps -q --filter "name=$CONTAINER" | xargs -r docker stop
docker ps -aq --filter "name=$CONTAINER" | xargs -r docker rm
docker run -d \
    -e "ORACLE_PWD=<?YourStrong@Passw0rd>" \
    -p 1521:1521 \
    --name $CONTAINER container-registry.oracle.com/database/free:latest
sleep 30
TEST_INTEGRATION=1 TEST_ORACLE=1 $PYTHON -m unittest discover
docker stop $CONTAINER && docker rm $CONTAINER

# Microsoft SQL Server
CONTAINER=sql1
docker ps -q --filter "name=$CONTAINER" | xargs -r docker stop
docker ps -aq --filter "name=$CONTAINER" | xargs -r docker rm
docker run -d \
    -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=<?YourStrong@Passw0rd>" \
    --hostname sql1 -p 1433:1433 \
    --name $CONTAINER mcr.microsoft.com/mssql/server:2022-latest
sleep 30
TEST_INTEGRATION=1 TEST_MSSQL=1 $PYTHON -m unittest discover
docker stop $CONTAINER && docker rm $CONTAINER

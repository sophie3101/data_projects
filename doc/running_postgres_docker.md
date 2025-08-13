docker pull postgres

1. start postgres instance
	
docker run --name  <> -e POSTGRES_PASSWORD=mysecretpassword -d postgres

example: 
docker run --name my-postgres \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -e POSTGRES_USER=myuser \
  -e POSTGRES_DB=mydb \
  -p 5434:5432 \
  -v pgdata:/var/lib/postgresql/data \
  -d postgres

`pgdata` is the name of the docker volumn
to map host vol to docker container vol do:
docker run --name my-postgres \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -e POSTGRES_USER=myuser \
  -e POSTGRES_DB=mydb \
  -p 5434:5432 \
  -v "$(pwd)":/var/lib/postgresql/data \
  -d postgres

`docker ps` to verify if the instance is running 
2. Now that your PostgreSQL container is running, you can connect to it using a PostgreSQL client

2a. use `psql` or `pgcli` to interact with the server

to use pgcli: `pip install pgcli`

to connect
pgcli -h localhost -p 54334 -U myuser -d mydb

to use psql:
psql -h localhost -p 5432 -U u249637 -d postgres

or 
    `docker exec -it my-postgres psql -U myuser -d mydb `  (to run inside the container)
 
 `-i` means Keeps STDIN open so the container can receive input
 `t` termainl interface 
where It tells psql to connect as the `postgres` user, which is the default superuser created by the Docker image.

or 
`psql -h localhost -p 5434 -U myuser -d mydb`

2. use GUI like pgAdmin

\q	Quit the psql shell
\dt	List tables
\d tablename	Show table schema
\i file.sql	Run a SQL script (inside psql)
\! command	Run a shell command from psql


copy file from local to docker container
docker cp sakila-db/* docker-postgres:/tmp/

#3 how to view binary logs
- get location of logs: 
`psql -h localhost -p 5432 -U u249637 -d postgres -c "SHOW data_directory;"`

 data_directory          
---------------------------------
 /opt/homebrew/var/postgresql@14
(1 row)

- hence the log is in: /opt/homebrew/var/postgresql@14/pg_wal 
- find pgwaldump location: which pg_waldump
  /opt/homebrew/bin/pg_waldump
-`/opt/homebrew/bin/pg_waldump  /opt/homebrew/var/postgresql@14/pg_wal/00000001000000000000000A`
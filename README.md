# Generic Data Pipeline

## _Overall Project Idea_
An attempt to write ETL solution based on Python in entirity.

This generic pipeline will parse a JSON file dowload from a public dataset (link mentioned in appendix)

## Features
- Parse JSON in prescribed format
- Apply DQ rules, generated bad data and good data file
- Generate control file based on header information
- Load the data into postgresql using Pandas
- Postgresql is hosted as docker container

## Docker 
- Major gotcha using docker container and where lot of time wasted was
wrong configuration of port forwarding in *docker-compose.yaml
```
ports:
      - "5431:5432"
```
Here, docker container listens on port 5432 (the right argument) and it gets forwarded to localhost on port 5431 (left argument). This was understood in other way round causing the connection attempt to port 5431 on container. This is an important point worth noting.

### Docker commands:
```sh
docker ps
docker-compose up
docker-compose down --volumes
docker stop $(docker ps -qa) && docker system prune -af --volumes && docker compose up
docker volume ls
docker info
pgcli -h localhost -p 5431 -u gdp_db_user -d gdp_db
```
### Code flow        

    >  file_process_invoker --> file_parser_gdp --> ingest_gdp_data


| Module | Details |
| ------ | ------ |
| file_process_invoker | Passes input file and calls parser |  
| file_parser_gdp | Parses file and generates bad/contrl and data file |
| ingest_gdp_data | Ingests data into postgresql |


## Appendix     


> dataset info: https://github.com/jdorfman/awesome-json-datasets#gdp

> direct link: http://api.worldbank.org/countries/IND/indicators/NY.GDP.MKTP.CD?per_page=5000&format=json
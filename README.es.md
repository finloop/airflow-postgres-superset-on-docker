# Flujo de aire + Postgres + Superset

## Arquitectura

![](docs/Architecture.jpg)

## Correr

    git clone git@github.com:finloop/airflow-postgres-superset-on-docker.git
    cd airflow-postgres-superset-on-docker

Para ejecutar los servicios necesitará el id del usuario actual, que debe colocarse
en un archivo `.env` y asignar a una variable `AIRFLOW_UID`. En el identificador de Linux del usuario actual
se puede descargar mediante el comando:

```sh
id -u
```

Archivo de ejemplo `.env` se ve así:

```text
AIRFLOW_UID=1000
```

Y por último, lanzamos todos los servicios:

```sh
docker-compose up
```

Después de ejecutarlos, debe colocar los datos de ejemplo en la base de datos. Hacemos esto:

```sh
python init-postgres.py
```

### Sitios web

*   [pgAdmin](http://localhost:5050)
*   [Corriente de aire](http://localhost:5053)
*   [Superconjunto](http://localhost:5054)

### Otros servicios disponibles para el anfitrión

*   cliente-postgres: [anfitrión local:5051](\[localhost:5051])
*   almacén-postgres: [localhost:5052](\[localhost:5052])

### Mapas en Apache Superset

Para que los mapas en Apache Superset funcionen, necesitará una clave para la API. Puede ser
consíguelo gratis aquí: [Caja de mapas](www.mapbox.com/). Debe colocarse
en un archivo `superset/docker/.env-non-dev` como `MAPBOX_API_KEY`. A continuación
sentido:

```txt
MAPBOX_API_KEY='YOUR_API_KEY_FROM_MAPBOX1234567890'
```

## Base de clientes

Para utilizar la base de datos cliente (no habilite otros servicios) utilice el comando:

    docker-compose up client-postgres pgadmin

Dirección de base de datos (para pgAdmina): `client-postgres:5432`

Configuración de la base de datos y pgAdmin:

```text
POSTGRES_USER: postgres
POSTGRES_PASSWORD: postgres
POSTGRES_DB: postgres
PGADMIN_DEFAULT_EMAIL: admin@admin.com
PGADMIN_DEFAULT_PASSWORD: admin
```

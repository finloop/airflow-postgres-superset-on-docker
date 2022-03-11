# Airflow + Postgres + Superset

## Architektura

![](docs/Architecture.png)

### Serwisy WWWW

- [pgAdmin](http://localhost:5050)
- [Airflow](http://localhost:5053)
- [Superset](http://localhost:5054)

### Pozostałe serwisy dostępne dla hosta
- client-postgres: [localhost:5051]([localhost:5051])
- warehouse-postgres: [localhost:5052]([localhost:5052])
## Baza klienta
Aby korzystać z bazy klienta (nie włączająć pozostałych serwisów) należy użyć polecenia:
```
docker-compose up client-postgres pgadmin
```

Adres bazy (dla pgAdmina): `client-postgres:5432`

Konfiguracja bazy i pgAdmin:
``` text
POSTGRES_USER: postgres
POSTGRES_PASSWORD: postgres
POSTGRES_DB: postgres
PGADMIN_DEFAULT_EMAIL: admin@admin.com
PGADMIN_DEFAULT_PASSWORD: admin
```
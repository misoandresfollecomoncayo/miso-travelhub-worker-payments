# miso-travelhub-worker-payments

Worker del proyecto **TravelHub** (MISO). Consume el topic de pagos en Kafka publicado por `miso-travelhub-service-payments` y persiste cada evento en la tabla `payment` de PostgreSQL.

## Stack

- Python 3.12
- FastAPI + Uvicorn (solo para `/health` — Cloud Run exige HTTP server)
- aiokafka (consumer async)
- SQLAlchemy 2.x async + asyncpg
- Pydantic v2 / pydantic-settings
- pytest + pytest-cov

## Arquitectura

```
┌──────────────────┐       publish        ┌──────────────────┐
│  Service         │ ───────────────────▶ │  Kafka (VM GCP)  │
│  payments        │   topic=payments-    │  bootstrap=...   │
│  (otro proyecto) │        queue         │  topic=payments- │
└──────────────────┘                      │       queue      │
                                          └────────┬─────────┘
                                                   │ consume (group)
                                                   ▼
                                          ┌──────────────────┐
                                          │  ESTE WORKER     │
                                          │  Cloud Run       │
                                          │  • /health       │
                                          │  • bg task:      │
                                          │    consume loop  │
                                          └────────┬─────────┘
                                                   │ insert (idempotent)
                                                   ▼
                                          ┌──────────────────┐
                                          │  Cloud SQL       │
                                          │  payment table   │
                                          └──────────────────┘
```

### Garantías

- **At-least-once delivery**. Los offsets se commitean **después** del insert en DB.
- **Idempotencia** vía `transaction_id` — `PaymentRepository.create_from_webhook` hace `SELECT WHERE transaction_id = ?` antes de insertar, así que reentregas no duplican filas.
- **Poison messages** (JSON inválido, payload sin campos requeridos) se loguean y se commitean para no quedar en loop infinito.
- **Errores transitorios** del handler dejan el offset sin commitear; el supervisor del consumer reinicia el loop con backoff y la próxima reasignación re-entrega el mensaje desde el último commit confirmado.

## Estructura

```
app/
├── api/v1/
│   ├── endpoints/health.py    # /health y /health/consumer
│   └── router.py
├── core/config.py             # Settings (env vars)
├── db/
│   ├── models.py              # Payment ORM
│   └── session.py             # async engine + sessionmaker
├── repositories/
│   └── payment_repository.py  # create_from_webhook idempotente
├── schemas/payment_webhook.py # Pydantic — mismo contrato que el producer
├── services/
│   ├── kafka_consumer.py      # AIOKafkaConsumer + supervisor loop
│   └── payment_event_handler.py  # bridge consumer → repository
└── main.py                    # create_app + lifespan (spawns consumer task)
tests/
```

## Endpoints HTTP

| Método | Ruta                        | Descripción                                          |
|--------|-----------------------------|------------------------------------------------------|
| GET    | `/api/v1/health`            | Liveness — siempre 200 si el contenedor está arriba  |
| GET    | `/api/v1/health/consumer`   | 200 si consumer `running`/`disabled`, 503 si `errored`/`stopped`. Devuelve `{state, processed, invalid, errors, topic, group}` |

No hay endpoints de negocio: este servicio no recibe llamadas, solo consume de Kafka.

## Variables de entorno

Ver [.env.example](.env.example) para la lista completa.

| Variable                          | Default                              | Notas                                                              |
|-----------------------------------|--------------------------------------|--------------------------------------------------------------------|
| `APP_ENV`                         | `development`                        |                                                                    |
| `APP_DEBUG`                       | `false`                              |                                                                    |
| `DATABASE_URL`                    | —                                    | `postgresql+asyncpg://user:pass@host:5432/db`                      |
| `DATABASE_ECHO`                   | `false`                              | Log de queries (solo dev)                                          |
| `KAFKA_ENABLED`                   | `false`                              | Si `false`, el worker arranca pero no consume                      |
| `KAFKA_BOOTSTRAP_SERVERS`         | —                                    | `host:9092` (coma-separados si hay varios)                         |
| `KAFKA_TOPIC`                     | `payments-queue`                     | Debe coincidir con el del producer                                 |
| `KAFKA_GROUP_ID`                  | `miso-travelhub-worker-payments`     | Misma group_id en todas las instancias → balanceo de particiones   |
| `KAFKA_CLIENT_ID`                 | `miso-travelhub-worker-payments`     |                                                                    |
| `KAFKA_AUTO_OFFSET_RESET`         | `earliest`                           | `earliest` en primer deploy procesa históricos                     |
| `KAFKA_SESSION_TIMEOUT_MS`        | `30000`                              |                                                                    |
| `KAFKA_MAX_POLL_INTERVAL_MS`      | `300000`                             |                                                                    |
| `KAFKA_SECURITY_PROTOCOL`         | `PLAINTEXT`                          | `PLAINTEXT` \| `SSL` \| `SASL_PLAINTEXT` \| `SASL_SSL`             |
| `KAFKA_SASL_MECHANISM`            | —                                    | Si protocol incluye SASL (`PLAIN`/`SCRAM-SHA-256`/...)             |
| `KAFKA_SASL_USERNAME`             | —                                    | Si SASL                                                            |
| `KAFKA_SASL_PASSWORD`             | —                                    | Si SASL — desde Secret Manager en producción                       |
| `KAFKA_RESTART_BACKOFF_SECONDS`   | `5.0`                                | Tras error inesperado en el consume loop                           |

## Ejecución local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # configura DATABASE_URL y KAFKA_*
uvicorn app.main:app --reload
```

El consumer arranca dentro del lifespan de FastAPI. Sin `KAFKA_ENABLED=true` no procesa mensajes.

## Con Docker

```bash
docker compose up --build
```

## Tests

```bash
pytest                                       # con coverage + fail-under=80
pytest --cov=app --cov-report=html           # HTML en htmlcov/
```

Los tests **no requieren broker Kafka ni base de datos**: monkeypatchean `aiokafka.AIOKafkaConsumer` con un fake y mockean `PaymentRepository` en los tests del handler. Cobertura actual: 93.63%.

## Operación

### Cold start

Al arrancar la revisión Cloud Run:

1. FastAPI levanta y `/health` responde 200 inmediatamente.
2. Lifespan construye `KafkaPaymentConsumer` con el handler de DB.
3. `consumer.start()` abre la conexión al broker.
4. `consumer.spawn()` arranca el consume loop en background.
5. `/health/consumer` reporta `running` cuando todo está OK.

Si el broker no es alcanzable, el contenedor **no muere** — `/health` sigue OK, `/health/consumer` reporta `stopped`/`errored`. Esto evita rolling restarts cuando hay un blip de red.

### Para Cloud Run

Este servicio es un **worker en background**, no recibe tráfico HTTP de usuarios. Para que Cloud Run no escale a 0 mientras procesa Kafka, se despliega con `--min-instances=1` (idealmente `--cpu-always-allocated`).

### Escalado horizontal

Múltiples instancias con el mismo `KAFKA_GROUP_ID` se reparten particiones del topic automáticamente. Particionar por `transactionId` en el producer (lo que ya hace `miso-travelhub-service-payments`) garantiza que todos los eventos de la misma transacción aterricen en la misma partición → en el mismo consumer → orden preservado.

### Networking

- Si Kafka tiene IP privada → Cloud Run necesita Direct VPC egress o un connector (mismas flags que el otro proyecto).
- Si Cloud SQL tiene IP privada → mismo VPC, o usar el socket Unix via `--add-cloudsql-instances`.
- Permisos sobre la subnet/connector: `roles/compute.networkUser` para el runtime SA y para el service agent (`service-<PROJECT_NUMBER>@serverless-robot-prod.iam.gserviceaccount.com`).
- Firewall: permitir tcp:9092 desde la subnet de Cloud Run hacia la VM de Kafka.

### Permisos del runtime SA

- `roles/cloudsql.client` (si usa Cloud SQL Auth Proxy via socket Unix)
- `roles/secretmanager.secretAccessor` sobre `DATABASE_URL` y `KAFKA_SASL_PASSWORD`
- `roles/compute.networkUser` sobre la subnet de egress (Direct VPC)

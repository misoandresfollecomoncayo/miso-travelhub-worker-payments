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
| `NOTIFICATION_ENABLED`            | `false`                              | Si `false`, las notificaciones booking-paid se omiten              |
| `NOTIFICATION_SERVICE_URL`        | `https://notification-services-154299161799.us-central1.run.app` | Cloud Run sibling                            |
| `NOTIFICATION_SERVICE_PATH`       | `/api/v1/notifications/send-notification` |                                                              |
| `NOTIFICATION_TIMEOUT_SECONDS`    | `5`                                  | Timeout HTTP para la llamada                                       |
| `EMAIL_NOTIFICATION_ENABLED`      | `false`                              | Si `false`, los eventos `payment.completed` (email) se omiten      |
| `EMAIL_NOTIFICATION_URL`          | `https://notification-services-ridyy4wz4q-uc.a.run.app/api/v1/notifications/events` | URL completa (host + path)         |
| `EMAIL_NOTIFICATION_TIMEOUT_SECONDS` | `5`                               | Timeout HTTP para la llamada                                       |

## Notificación booking-paid

Después de **persistir exitosamente** un pago con `status=APPROVED` (es decir, sólo cuando no es un duplicado y la pasarela aprobó la transacción), el worker hace un POST best-effort a:

```
POST {NOTIFICATION_SERVICE_URL}{NOTIFICATION_SERVICE_PATH}
Content-Type: application/json

{"booking_id": "<invoiceId del payload>", "status": "PAID"}
```

Reglas:

- Sólo se dispara para `APPROVED`. `DECLINED`, `PENDING`, `FAILED` y `REFUNDED` no notifican.
- **No** se vuelve a notificar en duplicados (re-entregas de Kafka): el repositorio retorna `None` y se omite la llamada para evitar notificar al usuario dos veces.
- Es **best-effort**: si el notification service responde 5xx, hay timeout o falla la conexión, se loguea pero el offset igual se commitea — la DB ya tiene el pago como fuente de verdad.

## Notificación payment.completed (email)

En paralelo a la notificación push, el worker emite un **segundo POST** al endpoint de eventos del notification-services para disparar el pipeline de email:

```
POST {EMAIL_NOTIFICATION_URL}
Content-Type: application/json

{
  "event_type": "payment.completed",
  "user_id": "<viajeroId resuelto desde la tabla reserva>",
  "payload": {
    "payment_id": "<transactionId>",
    "booking_id": "<invoiceId>",
    "amount": "<amount>",
    "currency": "<currency>",
    "provider": "PROVIDER DE PRUEBA"
  }
}
```

Pasos del flujo:

1. Mismas pre-condiciones que la notificación push: `status=APPROVED` y no es duplicado.
2. El worker abre una **segunda sesión read-only** en la misma DB para resolver el viajero:
   ```sql
   SELECT "viajeroId" FROM reserva WHERE id = :booking_id
   ```
3. Si no existe la reserva o el `viajeroId` es null → se loguea y se omite el email (no se rompe el handler).
4. Si todo va bien, se POST-ea el evento. Failures (timeout, 5xx) se loguean y siguen — el offset se commitea igual.

Las **dos notificaciones (push + email) son independientes**: si la push falla, el email todavía intenta enviarse, y viceversa. El provider está hardcodeado a `"PROVIDER DE PRUEBA"` en [`app/services/email_notification_client.py`](app/services/email_notification_client.py).

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

## Despliegue (CI/CD)

Pipeline en [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml). En cada push a `main` (o disparo manual desde la pestaña Actions):

1. **Job `test`** — instala dependencias y corre `pytest`, que con el `addopts` del [`pytest.ini`](pytest.ini) aplica `--cov=app --cov-fail-under=80`. Si la cobertura baja del 80% el job falla y nada se despliega.
2. **Job `deploy`** (solo si `test` pasa) — autentica contra GCP, hace `docker build` + push a Artifact Registry, y luego `gcloud run deploy` con flags pensadas para un worker en background.

### Flags importantes del deploy

Estas se aplican incondicionalmente porque son intrínsecas a este worker:

| Flag                          | Por qué                                                                 |
|-------------------------------|-------------------------------------------------------------------------|
| `--no-cpu-throttling`         | El consume loop sigue procesando entre requests. Sin esto Cloud Run pausa la CPU cuando no hay tráfico HTTP y el worker se "congela". |
| `--min-instances=1` (default) | Para que el worker no escale a cero mientras hay mensajes que consumir. Override con `vars.MIN_INSTANCES`. |
| `--max-instances=3` (default) | Techo conservador para no levantar más consumers de los necesarios. Override con `vars.MAX_INSTANCES`. |
| `--allow-unauthenticated`     | Para que los health checks de Cloud Run lleguen a `/health`. Si tu política lo prohíbe, cambia el workflow a `--no-allow-unauthenticated`. |
| `--port=8000`                 | Puerto en el que FastAPI atiende `/health` y `/health/consumer`.        |

### GitHub Actions Variables

Configurar en `Settings → Secrets and variables → Actions → Variables` (scope **Repository**):

| Variable                       | Ejemplo / propósito                                                       |
|--------------------------------|---------------------------------------------------------------------------|
| `GCP_PROJECT_ID`               | `gen-lang-client-0930444414`                                              |
| `GCP_REGION`                   | `us-central1`                                                             |
| `AR_REPOSITORY`                | Repo en Artifact Registry donde se empuja la imagen                       |
| `SERVICE_NAME`                 | `payments-worker` (distinto al del producer)                              |
| `RUNTIME_SERVICE_ACCOUNT`      | `payments-worker-runtime@…iam.gserviceaccount.com`                        |
| `MIN_INSTANCES`                | opcional — default `1`                                                    |
| `MAX_INSTANCES`                | opcional — default `3`                                                    |
| `VPC_NETWORK`                  | `travelhub-vpc` (si Kafka/SQL son IP privada)                             |
| `VPC_SUBNET`                   | `subnet-services` (misma región que Cloud Run)                            |
| `VPC_CONNECTOR`                | alternativa a `VPC_NETWORK`+`VPC_SUBNET` (Serverless VPC Access)          |
| `CLOUD_SQL_INSTANCE`           | `PROJECT:REGION:INSTANCE` (si conectas a Cloud SQL via socket Unix)       |
| `KAFKA_ENABLED`                | `true` en producción                                                      |
| `KAFKA_BOOTSTRAP_SERVERS`      | `10.10.3.3:9092`                                                          |
| `KAFKA_TOPIC`                  | `payments-queue` — **debe coincidir con el del producer**                 |
| `KAFKA_GROUP_ID`               | `miso-travelhub-worker-payments`                                          |
| `KAFKA_CLIENT_ID`              | opcional                                                                  |
| `KAFKA_AUTO_OFFSET_RESET`      | `earliest`                                                                |
| `KAFKA_SECURITY_PROTOCOL`      | `PLAINTEXT` o `SASL_*`                                                    |
| `KAFKA_SASL_MECHANISM`         | si aplica                                                                 |
| `KAFKA_SASL_USERNAME`          | si aplica                                                                 |
| `KAFKA_SASL_PASSWORD_SECRET`   | **nombre del secreto** en Secret Manager (no el password en claro)        |
| `NOTIFICATION_ENABLED`         | `true` para activar las notificaciones booking-paid                       |
| `NOTIFICATION_SERVICE_URL`     | `https://notification-services-154299161799.us-central1.run.app`          |
| `NOTIFICATION_SERVICE_PATH`    | `/api/v1/notifications/send-notification`                                 |
| `NOTIFICATION_TIMEOUT_SECONDS` | timeout HTTP (default `5`)                                                |
| `EMAIL_NOTIFICATION_ENABLED`   | `true` para emitir eventos `payment.completed` al pipeline de email       |
| `EMAIL_NOTIFICATION_URL`       | `https://notification-services-ridyy4wz4q-uc.a.run.app/api/v1/notifications/events` |
| `EMAIL_NOTIFICATION_TIMEOUT_SECONDS` | timeout HTTP (default `5`)                                          |

### GitHub Actions Secrets

| Secret           | Propósito                                                              |
|------------------|------------------------------------------------------------------------|
| `GCP_SA_KEY`     | JSON del SA con `roles/run.admin`, `roles/iam.serviceAccountUser`, `roles/artifactregistry.writer`, `roles/storage.admin` |

`DATABASE_URL` se monta como secret de Cloud Run (`DATABASE_URL=DATABASE_URL:latest` en el workflow) — vive en Secret Manager, no en GitHub.

### Setup IAM en GCP (una sola vez)

```bash
PROJECT=gen-lang-client-0930444414
RUNTIME_SA=payments-worker-runtime@${PROJECT}.iam.gserviceaccount.com
PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format='value(projectNumber)')

# Crear el SA del runtime si aún no existe
gcloud iam service-accounts create payments-worker-runtime --project=$PROJECT

# Acceso al secret DATABASE_URL (compartido con el producer)
gcloud secrets add-iam-policy-binding DATABASE_URL \
  --project=$PROJECT \
  --member=serviceAccount:$RUNTIME_SA \
  --role=roles/secretmanager.secretAccessor

# Cloud SQL client (si conectas via Unix socket)
gcloud projects add-iam-policy-binding $PROJECT \
  --member=serviceAccount:$RUNTIME_SA \
  --role=roles/cloudsql.client

# Network user sobre subnet-services para Direct VPC egress
gcloud compute networks subnets add-iam-policy-binding subnet-services \
  --region=us-central1 --project=$PROJECT \
  --member=serviceAccount:$RUNTIME_SA \
  --role=roles/compute.networkUser

# Y al service agent de Cloud Run — el que más se olvida
gcloud compute networks subnets add-iam-policy-binding subnet-services \
  --region=us-central1 --project=$PROJECT \
  --member="serviceAccount:service-${PROJECT_NUMBER}@serverless-robot-prod.iam.gserviceaccount.com" \
  --role=roles/compute.networkUser
```

### Cómo disparar un deploy

- Automático: `git push origin main`.
- Manual: pestaña **Actions** → workflow **Deploy worker to Cloud Run** → **Run workflow**.

El step **"Debug — show resolved deploy vars"** del job de deploy imprime los valores de cada variable antes del despliegue. Si ves alguna vacía que esperabas, la causa más común es que esté en scope Environment en lugar de Repository.

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

# Instrucciones para agente Claude — Setup WIF (GitHub Actions → GCP) sin Service Account keys

> **Para qué sirve este archivo:** se lo paso a un compañero del equipo TravelHub (MISW4501/4502, Uniandes — Grupo 9) para que su agente Claude Code configure desde cero la autenticación entre su repo de GitHub y un proyecto GCP, usando **Workload Identity Federation (WIF)**. La org policy `iam.disableServiceAccountKeyCreation` está enforced en los proyectos del grupo, así que **no se pueden crear keys descargables de Service Account**. WIF es la alternativa oficial: GitHub emite un OIDC token, GCP lo canjea por credenciales short-lived de una SA específica.
>
> Patrón validado en producción por los 5 servicios del grupo (user-services, pms-integration-services, pms-sync-worker, notification-services, inventory-services).

---

## Antes de empezar, AGENTE: pídele al usuario estos 4 datos

Antes de ejecutar nada, recopilá los siguientes datos del usuario y pegálos como variables al inicio del script. **NO procedas hasta tenerlos los 4 confirmados.**

| Variable | Pregunta exacta para el usuario | Ejemplo |
|---|---|---|
| `PROJECT` | "¿A qué proyecto GCP vas a desplegar? Pasame el `projectId`." | `travelhub-prod-492116` (PROD del grupo) o tu propio proyecto |
| `COMPA_REPO` | "¿Cuál es la URL de tu repo de GitHub? Necesito `<org>/<repo>` exacto." | `misoandresfollecomoncayo/miso-travelhub-service-payments` |
| `SA_NAME` | "¿Qué nombre le ponemos a la Service Account? Sugerencia: `github-deploy-<nombre-corto-del-servicio>`." | `github-deploy-payments` |
| `WORKFLOW_NAME` | "¿Qué hace tu workflow? Cloud Run? Cloud Deploy canary? ¿Solo build?" | `Cloud Deploy canary` (define qué roles dar) |

> Si el usuario no tiene los permisos para correr esto (típicamente owner/admin del proyecto), avisalo antes y pedile que se loguee con la cuenta que sí los tiene (`gcloud auth login <email>`).

---

## Lo que vas a crear en GCP (mapa)

```
Project ($PROJECT)
│
├── 1. Workload Identity Pool `github-pool`         (si no existe, idempotente)
│   └── 2. Provider OIDC `github-provider`           (apunta a token.actions.githubusercontent.com)
│       └── attribute_condition: assertion.repository == $COMPA_REPO
│
├── 3. Service Account `$SA_NAME@$PROJECT.iam.gserviceaccount.com`
│
├── 4. IAM bindings de la SA (los roles para que pueda hacer lo suyo)
│
└── 5. Workload Identity binding (permite a GitHub Actions del repo
       impersonar la SA, sin keys)
```

---

## Script completo (idempotente — se puede correr múltiples veces)

```bash
# === 0. Variables (LLENALAS con lo que te dijo el usuario) ===
PROJECT="<llenar>"                                 # ej: travelhub-prod-492116
COMPA_REPO="<llenar>"                              # ej: misoandresfollecomoncayo/miso-travelhub-service-payments
SA_NAME="<llenar>"                                 # ej: github-deploy-payments
REGION="us-central1"

PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
WIF_PROVIDER="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-pool/providers/github-provider"

echo "Variables resueltas:"
echo "  PROJECT=$PROJECT"
echo "  PROJECT_NUMBER=$PROJECT_NUMBER"
echo "  COMPA_REPO=$COMPA_REPO"
echo "  SA_EMAIL=$SA_EMAIL"
echo "  WIF_PROVIDER=$WIF_PROVIDER"

# === 1. Habilitar APIs necesarias (idempotente) ===
gcloud services enable \
  iamcredentials.googleapis.com \
  iam.googleapis.com \
  sts.googleapis.com \
  run.googleapis.com \
  clouddeploy.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  --project="$PROJECT"

# === 2. Workload Identity Pool ===
gcloud iam workload-identity-pools describe github-pool \
  --project="$PROJECT" --location=global >/dev/null 2>&1 \
  || gcloud iam workload-identity-pools create github-pool \
       --project="$PROJECT" --location=global \
       --display-name="GitHub Actions pool"

# === 3. WIF Provider OIDC ===
# Si ya existe y querés cambiar el attribute_condition, primero borrar:
#   gcloud iam workload-identity-pools providers delete github-provider \
#     --project="$PROJECT" --location=global --workload-identity-pool=github-pool
gcloud iam workload-identity-pools providers describe github-provider \
  --project="$PROJECT" --location=global --workload-identity-pool=github-pool >/dev/null 2>&1 \
  || gcloud iam workload-identity-pools providers create-oidc github-provider \
       --project="$PROJECT" --location=global \
       --workload-identity-pool=github-pool \
       --display-name="GitHub provider" \
       --issuer-uri="https://token.actions.githubusercontent.com" \
       --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.ref=assertion.ref" \
       --attribute-condition="assertion.repository=='${COMPA_REPO}'"

# === 4. Service Account ===
gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT" >/dev/null 2>&1 \
  || gcloud iam service-accounts create "$SA_NAME" \
       --project="$PROJECT" \
       --display-name="GitHub deploy for $COMPA_REPO"

# === 5. Roles de la SA — ajustá la lista según el workflow ===
# Mínimo para Cloud Deploy canary + Cloud Run + Artifact Registry:
ROLES=(
  roles/run.admin
  roles/clouddeploy.releaser
  roles/clouddeploy.operator
  roles/iam.serviceAccountUser
  roles/cloudbuild.builds.editor
  roles/artifactregistry.writer
  roles/secretmanager.secretAccessor
  roles/logging.logWriter
  roles/storage.admin
)
for ROLE in "${ROLES[@]}"; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" --condition=None >/dev/null
done

# === 6. WIF binding: tu repo de GitHub puede impersonar esta SA ===
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project="$PROJECT" \
  --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-pool/attribute.repository/${COMPA_REPO}"

# === 7. Mostrar los valores que el usuario tiene que pegar en su workflow ===
echo
echo "============================================================"
echo "✅ Setup completado. Pegá estos valores en tu .github/workflows/ci.yml:"
echo "============================================================"
echo "  workload_identity_provider: ${WIF_PROVIDER}"
echo "  service_account:            ${SA_EMAIL}"
echo "============================================================"
```

---

## Workflow GitHub Actions de ejemplo (copy-paste mínimo)

Decile al usuario que cree `.github/workflows/deploy-prod.yml` en su repo con esto (cambiando `<WIF_PROVIDER>` y `<SERVICE_ACCOUNT>` por los valores del script):

```yaml
name: Deploy to Prod (Cloud Deploy Canary)
on:
  push:
    branches: [main]

permissions:
  contents: read
  id-token: write          # ← OBLIGATORIO: sin esto GitHub no emite OIDC token

env:
  PROJECT: <PROJECT>
  REGION: us-central1
  WIF_PROVIDER: <WIF_PROVIDER>
  SERVICE_ACCOUNT: <SERVICE_ACCOUNT>
  SERVICE_NAME: <tu-servicio>

jobs:
  deploy-prod:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - id: auth
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ env.WIF_PROVIDER }}
          service_account: ${{ env.SERVICE_ACCOUNT }}

      - uses: google-github-actions/setup-gcloud@v2

      - name: Docker auth
        run: gcloud auth configure-docker ${{ env.REGION }}-docker.pkg.dev --quiet

      - name: Build + push image
        run: |
          IMAGE="${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT }}/${{ env.SERVICE_NAME }}/${{ env.SERVICE_NAME }}:${{ github.sha }}"
          docker build -t "$IMAGE" .
          docker push "$IMAGE"

      - name: Create Cloud Deploy release
        run: |
          gcloud deploy releases create prod-${GITHUB_SHA::8}-$(date +%Y%m%d%H%M%S) \
            --project=${{ env.PROJECT }} --region=${{ env.REGION }} \
            --delivery-pipeline=${{ env.SERVICE_NAME }} \
            --images=${{ env.SERVICE_NAME }}=${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT }}/${{ env.SERVICE_NAME }}/${{ env.SERVICE_NAME }}:${{ github.sha }} \
            --skaffold-file=skaffold.yaml
```

> Si el usuario no tiene `clouddeploy.yaml` ni `skaffold.yaml` todavía, hay que crearlos. Avisale que ese es un paso aparte. El template más simple para Cloud Run lo encuentra en `https://cloud.google.com/deploy/docs/deploy-app-run`.

---

## Verificación post-setup (corré esto después)

```bash
# A. Verificar que la SA existe y tiene los roles
gcloud projects get-iam-policy "$PROJECT" \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${SA_EMAIL}" \
  --format="value(bindings.role)"
# Debe listar al menos: roles/run.admin, roles/clouddeploy.releaser, roles/iam.serviceAccountUser

# B. Verificar el WIF binding sobre la SA
gcloud iam service-accounts get-iam-policy "$SA_EMAIL" --project="$PROJECT"
# Debe mostrar un binding roles/iam.workloadIdentityUser con principalSet de tu repo

# C. Verificar el provider OIDC
gcloud iam workload-identity-pools providers describe github-provider \
  --project="$PROJECT" --location=global --workload-identity-pool=github-pool \
  --format="value(attributeCondition)"
# Debe imprimir: assertion.repository=='<COMPA_REPO>'

# D. Test end-to-end: push una rama feature al repo y ve los logs del workflow.
#    El step "google-github-actions/auth@v2" debe terminar con:
#    "Successfully created credentials file at /home/runner/work/_temp/..."
```

---

## Troubleshooting

| Error en el workflow | Causa probable | Fix |
|---|---|---|
| `Permission 'iam.serviceAccounts.getAccessToken' denied on resource <SA>` | El attribute_condition no matchea el repo, o el WIF binding no apunta al repo correcto | Revisar paso 3 (provider) + paso 6 (binding) del script |
| `The caller does not have permission` al hacer `gcloud deploy releases create` | Faltan roles a la SA | Agregar el role que necesite con `gcloud projects add-iam-policy-binding` |
| `Workload Identity Pool '...' is locked or deleted` | El pool fue creado y borrado antes — Google lo deja "soft-deleted" 30 días | Usar otro nombre de pool o `gcloud iam workload-identity-pools undelete` |
| Workflow no muestra `id-token` permissions | Olvidaste `permissions: id-token: write` en el workflow | Agregarlo a nivel job o workflow |
| `Quota 'workloadIdentityPools'` exceeded | Límite por proyecto (típicamente 100 pools) | Reusar `github-pool` existente en lugar de crear uno nuevo |

---

## Por qué este enfoque y no Service Account keys

1. **Org policy enforcement**: `iam.disableServiceAccountKeyCreation` bloquea `gcloud iam service-accounts keys create` a nivel organización. No es opcional sin escalación.
2. **Seguridad**: las SA keys son credenciales **long-lived** (no expiran salvo rotación manual). Si se filtran (commit accidental, log, screenshot), el atacante tiene acceso persistente.
3. **WIF emite credenciales short-lived** (1h por defecto). Si se filtran, expiran rápido.
4. **No hay secrets que rotar**: nada queda guardado en GitHub Secrets; el token se emite y consume en cada workflow run.
5. **Auditabilidad**: Cloud Audit Logs muestra exactamente qué repo+rama gatilló cada operación.

---

## Resumen para el agente

1. Pedile al usuario los 4 datos al inicio (PROJECT, COMPA_REPO, SA_NAME, WORKFLOW_NAME).
2. Copiá el script en una sesión bash, pegá las variables, y corrélo.
3. Mostrale al usuario los 2 valores finales que tiene que pegar en su `.github/workflows/*.yml`.
4. Sugerile que añada el bloque `permissions: id-token: write` antes que cualquier otra cosa.
5. Verificá con los 4 comandos de la sección "Verificación post-setup".
6. Si algo falla, consultá la tabla de Troubleshooting.

Si el usuario te pide tocar la org policy `iam.disableServiceAccountKeyCreation` para crear una key igual — **no lo hagas**. La política existe a propósito y este flujo WIF resuelve el caso de uso sin necesidad de keys. Si insiste, pedile que el cambio venga del dueño del proyecto y se documente como excepción temporal.

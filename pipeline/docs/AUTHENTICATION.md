# Authentication Setup — CO₂ ELY Energystack Pipeline

## Overview

This pipeline uses **cluster identity** for all ADLS access — the same pattern as
`bosch_ely_adb_batch` (TBP). No credentials appear in code, YAML, or job parameters.

```
┌─────────────────────────────────────────────────────────────────────┐
│ Auth Chain                                                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  databricks.yml                                                     │
│    run_as:                                                          │
│      service_principal_name: sp-co2ely-energystack                  │
│           │                                                         │
│           ▼                                                         │
│  Job Cluster (SINGLE_USER mode)                                     │
│    → cluster starts under SP identity                               │
│           │                                                         │
│           ▼                                                         │
│  Spark Runtime                                                      │
│    → auto-fetches Azure AD token for the SP                         │
│           │                                                         │
│           ▼                                                         │
│  abfss://co2elyd-data@stpsbdodxdev2datalake.dfs.core.windows.net/   │
│    → Storage Blob Data Contributor role grants read/write            │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Service Principal Details

| Property | Value |
| --- | --- |
| Display name | `sp-co2ely-energystack` |
| Tenant ID | `0ae51e19-07c8-4e4b-bb6d-648ee58410f4` |
| Client (Application) ID | `57029129-abb0-4ec9-9b85-25fffb4af624` |
| Azure RBAC role | Storage Blob Data Contributor |
| Storage account | `stpsbdodxdev2datalake` |
| Container | `co2elyd-data` |

## Setup Checklist

### 1. Azure AD App Registration ✅

The SP `sp-co2ely-energystack` is already registered in Azure AD with a client secret.
It already has **Storage Blob Data Contributor** on `stpsbdodxdev2datalake`.

### 2. Register SP in Databricks Workspace

The SP must be added to each Databricks workspace where the pipeline runs.
Without this, `run_as: service_principal_name` will fail with "SP not found".

**How to add:**

- Admin Console → Service Principals → Add Service Principal
- Enter Application ID: `57029129-abb0-4ec9-9b85-25fffb4af624`
- OR: Azure AD SCIM provisioning (automatic sync from Azure AD group)

**Workspaces to register in:**

| Environment | Workspace URL | Status |
| --- | --- | --- |
| dev | `adb-1032635496032522.2.azuredatabricks.net` | ☐ TODO |
| qa | `adb-7376334951991000.0.azuredatabricks.net` | ☐ TODO |
| prod | `adb-5407587042408609.9.azuredatabricks.net` | ☐ TODO |

### 3. Unity Catalog Permissions

The SP needs UC permissions for:

- The tracking Delta table (`co2elyd_dev.converter.file_tracking`)
- The bronze schema (if writing Delta tables downstream)

```sql
-- Create the converter schema (one-time, requires catalog admin)
CREATE SCHEMA IF NOT EXISTS co2elyd_dev.converter;

-- Grant SP permissions on converter schema
GRANT USE CATALOG ON CATALOG co2elyd_dev TO `sp-co2ely-energystack`;
GRANT USE SCHEMA ON SCHEMA co2elyd_dev.converter TO `sp-co2ely-energystack`;
GRANT CREATE TABLE ON SCHEMA co2elyd_dev.converter TO `sp-co2ely-energystack`;
GRANT MODIFY ON SCHEMA co2elyd_dev.converter TO `sp-co2ely-energystack`;
GRANT SELECT ON SCHEMA co2elyd_dev.converter TO `sp-co2ely-energystack`;

-- Grant SP permissions on bronze schema (for Auto Loader ingest)
GRANT USE SCHEMA ON SCHEMA co2elyd_dev.bronze TO `sp-co2ely-energystack`;
GRANT CREATE TABLE ON SCHEMA co2elyd_dev.bronze TO `sp-co2ely-energystack`;
GRANT MODIFY ON SCHEMA co2elyd_dev.bronze TO `sp-co2ely-energystack`;
GRANT SELECT ON SCHEMA co2elyd_dev.bronze TO `sp-co2ely-energystack`;
```

### 4. Storage Credential + External Location (UC Governance)

If the workspace enforces Unity Catalog governance on external data (recommended),
create a Storage Credential and External Location so UC can govern access to ADLS.

```sql
-- Option A: Storage Credential with SP client secret (via secret scope)
CREATE STORAGE CREDENTIAL IF NOT EXISTS co2ely_adls_credential
WITH (
  AZURE_SERVICE_PRINCIPAL (
    DIRECTORY_ID   = '0ae51e19-07c8-4e4b-bb6d-648ee58410f4',
    APPLICATION_ID = '57029129-abb0-4ec9-9b85-25fffb4af624',
    CLIENT_SECRET  = '{{secrets/kv-databricks-secret-scope/co2ely-client-secret}}'
  )
);

-- Option B: Storage Credential with Azure Managed Identity (preferred, no secret rotation)
-- Requires Azure Access Connector linked to the workspace with a user-assigned managed identity.
-- CREATE STORAGE CREDENTIAL IF NOT EXISTS co2ely_adls_credential
-- WITH (AZURE_MANAGED_IDENTITY (ACCESS_CONNECTOR_ID = '/subscriptions/.../accessConnectors/...'));

-- External Location pointing to the container
CREATE EXTERNAL LOCATION IF NOT EXISTS co2ely_data_location
URL 'abfss://co2elyd-data@stpsbdodxdev2datalake.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL co2ely_adls_credential)
COMMENT 'CO2 ELY measurement data (raw XLSX/CSV + Parquet output)';

-- Grant access to the SP
GRANT READ_FILES ON EXTERNAL LOCATION co2ely_data_location TO `sp-co2ely-energystack`;
GRANT WRITE_FILES ON EXTERNAL LOCATION co2ely_data_location TO `sp-co2ely-energystack`;
```

**Note:** If the workspace does NOT enforce UC governance on external storage
(i.e., passthrough mode), the SP's Azure RBAC alone is sufficient and
Step 4 can be skipped. Check with your workspace admin.

### 5. Cluster Policy Permissions

The SP needs "Can Use" permission on the cluster policies referenced in job YAML:

| Policy Name | Used By |
| --- | --- |
| `Advanced job cluster without autoscaling` | Converter job (static single-node) |
| `Advanced job cluster with autoscaling` | Bronze/Silver/Gold jobs (scaled) |

**How to grant:**

- Admin Console → Cluster Policies → Select policy → Permissions tab
- Add `sp-co2ely-energystack` with "Can Use" level

### 6. Secret Scope (for Storage Credential only)

If using Option A in Step 4 (SP client secret), store the secret in Azure Key Vault
and create a Databricks secret scope backed by it:

```bash
# Create Azure Key Vault-backed secret scope (one-time)
databricks secrets create-scope \
  --scope kv-databricks-secret-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<vault> \
  --dns-name https://<vault>.vault.azure.net/

# Store the client secret in Key Vault
az keyvault secret set \
  --vault-name <vault> \
  --name co2ely-client-secret \
  --value "<client-secret-value>"
```

## How It Works at Runtime

1. **Job starts** → Databricks resolves `run_as: sp-co2ely-energystack`
2. **Cluster creates** → `data_security_mode: SINGLE_USER` means all Spark operations
   execute under the SP's identity
3. **Spark reads `abfss://`** → Hadoop ABFS driver requests Azure AD token using the
   SP's credentials (managed internally by Databricks runtime)
4. **Azure AD returns token** → SP is authenticated
5. **ADLS checks RBAC** → SP has Storage Blob Data Contributor → access granted
6. **Data flows** → read/write succeeds, no credentials in code

## Environments & Storage Mapping

The pipeline auto-detects the environment from the workspace URL
(configured in `src/_0_convert/common.py :: ENVIRONMENT_CONFIG`):

| Workspace | Environment | ADLS Domain | Container |
| --- | --- | --- | --- |
| `adb-1032635496032522.2` | dev | `stpsbdodxdev2datalake.dfs.core.windows.net` | `co2elyd-data` |
| `adb-7376334951991000.0` | qa | `stpsbdodxqadatalake.dfs.core.windows.net` | `co2elyd-data` |
| `adb-5407587042408609.9` | prod | `stpsbdodxproddatalake.dfs.core.windows.net` | `co2elyd-data` |

**Note:** The SP needs Storage Blob Data Contributor on ALL environment storage accounts,
or use separate SPs per environment.

## Comparison with TBP (bosch_ely_adb_batch)

| Aspect | TBP | This pipeline |
| --- | --- | --- |
| Service Principal | `sp-pemely-databricks` | `sp-co2ely-energystack` |
| Storage accounts | `stpsbdodxdev/qa/prod` + `datalake` | `stpsbdodxdev2datalake` |
| Containers | `pemely-data`, `pemely-dev`, `pemely-ops` | `co2elyd-data` |
| Auth mechanism | run_as + SINGLE_USER (same) | run_as + SINGLE_USER (same) |
| Credentials in code | None | None |
| Credentials in YAML | None | None |
| Environment detection | `spark.databricks.workspaceUrl` → dict | Same |

## Troubleshooting

### "Service principal not found"

→ SP not registered in the workspace. Add via Admin Console → Service Principals.

### "403 Forbidden" on ADLS access

→ SP doesn't have Storage Blob Data Contributor on the target storage account.
Check: Azure Portal → Storage Account → IAM → Role assignments.

### "PERMISSION_DENIED" on Delta table

→ UC grants missing. Run the SQL in Step 3 as a catalog admin.

### "External location not found" / "No matching external location"

→ External Location not created (Step 4), OR the URL doesn't match exactly.
The URL must be a prefix of the path being accessed.

### "Cluster policy not accessible"

→ SP doesn't have "Can Use" on the policy. Grant via Admin Console (Step 5).

### Works in `dev_user` but fails in `dev` target

→ `dev_user` runs as your user identity (development mode, no `run_as`).
`dev` target uses `run_as: SP` — the SP needs all the permissions above.
Your personal user may have broader access than the SP.

## Contact

- **Azure/SP admin:** TBD — who manages Azure AD app registrations
- **UC metastore admin:** TBD — who can create storage credentials + external locations
- **Workspace admin:** Peter Nouws (TbP/MFE2.2)

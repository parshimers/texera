{{/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/}}

{{/*
Object-storage (S3) resolution helpers.

When storage.s3.endpoint is set the services talk to that external
S3-compatible store (credentials come from storage.s3.existingSecret, or a
chart-generated "<release>-s3-credentials" Secret). When it is empty the
services fall back to the in-cluster RustFS Service and its auto-generated
"<release>-rustfs-secret" Secret, so the default install is unchanged.
*/}}

{{/* S3 endpoint URL. */}}
{{- define "texera.s3.endpoint" -}}
{{- $clusterAddress := include "texera.slurm.clusterAddress" . -}}
{{- if .Values.storage.s3.endpoint -}}
{{- .Values.storage.s3.endpoint -}}
{{- else if and $clusterAddress .Values.rustfs.enabled -}}
{{- /* Every client shares this one: Lakekeeper hands the warehouse's endpoint to whoever reads a
       table, a Slurm unit included, so the unit cannot be given a different one of its own. */ -}}
{{- printf "http://%s:9000" $clusterAddress -}}
{{- else -}}
{{- printf "http://%s-rustfs-svc:9000" .Release.Name -}}
{{- end -}}
{{- end -}}

{{/* Where Slurm nodes reach this cluster's services; empty unless they are to be published. */}}
{{- define "texera.slurm.clusterAddress" -}}
{{- $slurm := .Values.workflowComputingUnitManager.slurm -}}
{{- if $slurm.enabled -}}
{{- $slurm.clusterAddress -}}
{{- end -}}
{{- end -}}

{{/*
The administrator's job JSON, with the published addresses added to job.environment. They go first
because the manager keeps the last entry for a name, which leaves the administrator's own winning.
*/}}
{{- define "texera.slurm.jobJson" -}}
{{- $slurm := .Values.workflowComputingUnitManager.slurm -}}
{{- $address := include "texera.slurm.clusterAddress" . -}}
{{- if $address -}}
{{- $body := mustFromJson $slurm.jobJson -}}
{{- $job := required "workflowComputingUnitManager.slurm.jobJson has no \"job\" object" $body.job -}}
{{- $fileService := printf "http://%s:%v/api" $address .Values.fileService.service.port -}}
{{- $published := list
      (printf "STORAGE_JDBC_URL=jdbc:postgresql://%s:5432/texera_db?currentSchema=texera_db,public" $address)
      (printf "STORAGE_ICEBERG_CATALOG_REST_URI=http://%s:%v/catalog" $address .Values.lakekeeper.catalog.service.externalPort)
      (printf "STORAGE_LAKEFS_ENDPOINT=http://%s:%v/api/v1" $address .Values.lakefs.service.port)
      (printf "FILE_SERVICE_GET_DATASET_PRESIGNED_URL_ENDPOINT=%s/dataset/presign-download" $fileService)
      (printf "FILE_SERVICE_GET_MODEL_PRESIGNED_URL_ENDPOINT=%s/model/presign-download" $fileService)
      (printf "FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT=%s/dataset/did/upload" $fileService) -}}
{{- $_ := set $job "environment" (concat $published ($job.environment | default list)) -}}
{{- mustToRawJson $body -}}
{{- else -}}
{{- $slurm.jobJson -}}
{{- end -}}
{{- end -}}

{{/* Name of the Secret holding the S3 credentials. */}}
{{- define "texera.s3.secretName" -}}
{{- if .Values.storage.s3.endpoint -}}
{{- .Values.storage.s3.existingSecret | default (printf "%s-s3-credentials" .Release.Name) -}}
{{- else -}}
{{- printf "%s-rustfs-secret" .Release.Name -}}
{{- end -}}
{{- end -}}

{{/* Secret data key for the S3 access key id. */}}
{{- define "texera.s3.accessKeyIdKey" -}}
{{- if .Values.storage.s3.endpoint -}}access-key-id{{- else -}}RUSTFS_ACCESS_KEY{{- end -}}
{{- end -}}

{{/* Secret data key for the S3 secret access key. */}}
{{- define "texera.s3.secretAccessKeyKey" -}}
{{- if .Values.storage.s3.endpoint -}}secret-access-key{{- else -}}RUSTFS_SECRET_KEY{{- end -}}
{{- end -}}

{{/*
Observability emission env for a Scala service pod.

Renders OTEL_EXPORTER_OTLP_ENDPOINT (pointing at the in-cluster OTel
Collector Service) plus TEXERA_OTEL_ALLOWED_HOSTS. OtelInit validates the
endpoint host against an allowlist whose default is localhost only, so the
collector's Service name must be added explicitly or the SDK rejects it and
emits nothing. Renders nothing unless both the observability stack and the
collector are enabled, so the default install is unchanged. Include inside a
container's `env:` list, e.g. `{{- include "texera.observability.env" . | nindent 12 }}`.
*/}}
{{- define "texera.observability.env" -}}
{{- if and .Values.observability.enabled .Values.observability.collector.enabled }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: "http://{{ .Release.Name }}-otel-collector:{{ .Values.observability.collector.grpcPort }}"
- name: TEXERA_OTEL_ALLOWED_HOSTS
  value: "{{ .Release.Name }}-otel-collector"
{{- end }}
{{- end -}}

{{/*
The audience the mounter's service-account tokens are bound to. Fixed rather than
configurable: it is one half of a credential contract between access-control-service and the
mounter -- the projected token declares it and the mounter's TokenReview requires it -- so
the two must always agree, and there is no deployment in which a different value is useful.
*/}}
{{- define "texera.mounter.audience" -}}
texera-mounter
{{- end -}}

{{/*
The service-account username allowed to request a mount from the per-node mounter, in the
form MOUNTER_ALLOWED_CALLERS expects. This is access-control-service and nothing else: it
is the deployment's authorization authority for computing units, so it is the one component
that can decide whether a given user may mount into a given CU. Deliberately not
configurable -- widening it is a security decision, not a deployment preference, and a
values override would let an install quietly hand the privileged mounter to another caller.
*/}}
{{- define "texera.mounter.allowedCallers" -}}
{{- printf "system:serviceaccount:%s:%s" .Release.Namespace .Values.accessControlService.serviceAccountName -}}
{{- end -}}

{{/* Jupyter base path, as exactly one leading slash and no trailing one. Several places
append the uid to it, so a bare value would render "http://<origin>jupyter/7". */}}
{{- define "texera.jupyter.basePath" -}}
{{- printf "/%s" (trimAll "/" .Values.jupyterPool.basePath) -}}
{{- end -}}

{{/* Grafana datasource provisioning body. Kept as a helper so the Deployment's
checksum/config annotation can hash just this content; hashing the template that
holds the annotation would make it reference itself and recurse. */}}
{{- define "texera.observability.grafanaDatasources" -}}
apiVersion: 1
datasources:
  # Metrics: VictoriaMetrics speaks the Prometheus query API. Default.
  - name: Metrics
    type: prometheus
    uid: metrics
    access: proxy
    url: http://{{ .Release.Name }}-victoriametrics:{{ .Values.observability.metrics.port }}
    isDefault: true
    editable: false
{{- if .Values.observability.logs.enabled }}
  # Logs: Loki, LogQL over OTLP-ingested logs.
  - name: Loki
    type: loki
    uid: loki
    access: proxy
    url: http://{{ .Release.Name }}-loki:{{ .Values.observability.logs.port }}
    editable: false
{{- end }}
{{- if .Values.observability.traces.enabled }}
  # Traces: Jaeger query API / UI backend.
  - name: Jaeger
    type: jaeger
    uid: jaeger
    access: proxy
    url: http://{{ .Release.Name }}-jaeger:{{ .Values.observability.traces.queryPort }}
    editable: false
{{- end }}
{{- end -}}

{{/* Loki server config body. A helper for the same reason as the Grafana one,
and so the checksum/config annotation hashes the config instead of its template.
Retention is enforced here: the compactor deletes chunks older than
observability.logs.retentionPeriod, which Loki otherwise ignores. */}}
{{- define "texera.observability.lokiConfig" -}}
# No multi-tenancy: one implicit tenant for the in-cluster stack.
auth_enabled: false
server:
  http_listen_port: {{ .Values.observability.logs.port }}
  grpc_listen_port: 9096
common:
  instance_addr: 127.0.0.1
  # Every component's data path defaults under this prefix, the PVC at /loki.
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory
schema_config:
  configs:
    - from: 2024-01-01
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h
limits_config:
  # Required for OTLP ingest (OTLP attributes -> structured metadata).
  allow_structured_metadata: true
  reject_old_samples: false
  # Age after which log chunks are eligible for deletion by the compactor.
  retention_period: {{ .Values.observability.logs.retentionPeriod }}
compactor:
  working_directory: /loki/compactor
  # Without this the retention_period above is parsed but never enforced,
  # so logs accumulate until the volume fills.
  retention_enabled: true
  delete_request_store: filesystem
{{- end -}}

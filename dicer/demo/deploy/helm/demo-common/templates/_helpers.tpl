{{/*
Common labels from values.
*/}}
{{- define "demo-common.labels" -}}
{{- range $key, $value := .Values.labels }}
{{ $key }}: {{ $value }}
{{- end }}
{{- end }}

{{/*
Pod template labels with deploymentTime.
*/}}
{{- define "demo-common.podLabels" -}}
{{- range $key, $value := .Values.labels }}
{{ $key }}: {{ $value }}
{{- end }}
{{/* Forces pod restart on helm upgrade by setting unique timestamp. */}}
deploymentTime: "{{ now | unixEpoch }}"
{{- end }}

{{/*
Container ports from values.container.ports list.
*/}}
{{- define "demo-common.containerPorts" -}}
{{- range .Values.container.ports }}
- containerPort: {{ .port }}
  name: {{ .name }}
  protocol: TCP
{{- end }}
{{- end }}

{{/*
TLS configuration JSON snippet for ConfigMaps.
Outputs the databricks.rpc.cert/key/truststore config keys.
*/}}
{{- define "demo-common.tls.configJson" -}}
"databricks.rpc.cert": "{{ .Values.tls.mountPath }}/{{ .Values.tls.certFile }}",
"databricks.rpc.key": "{{ .Values.tls.mountPath }}/{{ .Values.tls.keyFile }}",
"databricks.rpc.truststore": "{{ .Values.tls.mountPath }}/{{ .Values.tls.caFile }}"
{{- end }}

{{/*
TLS volumes section for pod spec. Includes the 'volumes:' key when TLS is enabled.
*/}}
{{- define "demo-common.tls.volumeIfEnabled" -}}
{{- if .Values.tls.enabled }}
volumes:
- name: tls-certs
  secret:
    secretName: {{ .Values.tls.secretName }}
{{- end }}
{{- end }}

{{/*
TLS volumeMounts section for container spec. Includes the 'volumeMounts:' key when TLS is enabled.
*/}}
{{- define "demo-common.tls.volumeMountIfEnabled" -}}
{{- if .Values.tls.enabled }}
volumeMounts:
- name: tls-certs
  mountPath: {{ .Values.tls.mountPath }}
  readOnly: true
{{- end }}
{{- end }}

{{/*
Expand the name of the chart.
*/}}
{{- define "opentenbase.name" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "opentenbase.extraEnv" -}}
{"DATANODE_COUNT":"{{ len .Values.dn }}", "COORDINATOR_COUNT":"{{ len .Values.cn }}"}
{{- end }}
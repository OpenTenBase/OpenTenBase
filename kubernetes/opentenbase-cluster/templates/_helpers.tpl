{{/*
Expand the name of the chart.
*/}}
{{- define "opentenbase.name" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
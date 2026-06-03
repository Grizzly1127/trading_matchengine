{{- define "trading.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "trading.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "trading.namespace" -}}
{{- default .Values.global.namespace .Release.Namespace }}
{{- end }}

{{- define "trading.image" -}}
{{- $reg := trimSuffix "/" (default "" .Values.global.imageRegistry) -}}
{{- $repo := required "image name required" .repo -}}
{{- $tag := default .Values.global.imageTag .tag -}}
{{- if $reg -}}
{{- printf "%s/%s:%s" $reg $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end }}

{{- define "trading.labels" -}}
app.kubernetes.io/name: {{ include "trading.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "trading.selectorLabels" -}}
app.kubernetes.io/name: {{ include "trading.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "px-pem.name" -}}
px-pem
{{- end -}}

{{- define "px-pem.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "px-pem.name" .) -}}
{{- end -}}

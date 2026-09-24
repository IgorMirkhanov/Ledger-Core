package notifications

import (
	"embed"
	"fmt"
	"text/template"
)

//go:embed templates/*.tmpl
var templateFS embed.FS

func loadTemplates() (*template.Template, error) {
	root := template.New("notifications")
	names := []string{
		TemplateDepositReceived,
		TemplateTransferSent,
		TemplateTransferReceived,
		TemplateTransferFailed,
	}
	for _, name := range names {
		b, err := templateFS.ReadFile("templates/" + name + ".tmpl")
		if err != nil {
			return nil, fmt.Errorf("notifications: read template %s: %w", name, err)
		}
		if _, err := root.New(name).Parse(string(b)); err != nil {
			return nil, fmt.Errorf("notifications: parse template %s: %w", name, err)
		}
	}
	return root, nil
}

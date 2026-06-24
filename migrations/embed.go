// Package migrations embeds the SQL migration files into the binary so the
// `-mode=migrate` step is fully self-contained — no migrations directory needs
// to ship alongside the distributed image.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

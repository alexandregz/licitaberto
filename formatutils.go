package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// ==== utilidades ====
// Crea links dos PDFs fisicos
func createLinkPDF(taboa string) string {
	// hai que eliminar _files e é importante desde o raiz
	return fmt.Sprintf("/pdfs/%s", strings.TrimSuffix(taboa, "_files"))
}

func quoteIdent(id string) string {
	// minimal: wrap with double quotes and escape existing quotes
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

// remove extension
func stripExt(path string) string {
	ext := filepath.Ext(path)            // inclúe o punto: ".txt", ".gz", etc.
	return strings.TrimSuffix(path, ext) // elimina a última extensión
}

// conversores formatos de importes

// Converte "12.345,67" -> 12345.67
func parseEuroNumber(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if !euroNumRe.MatchString(s) {
		return 0, false
	}
	// quitar puntos de milleiro e cambiar coma por punto
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, ",", ".")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// Expr SQL para ordenar numericamente segundo estilo
func numericOrderExpr(col string, style string) string {
	id := quoteIdent(col)
	switch style {
	case "euro":
		// "12.345,67" -> 12345.67
		return fmt.Sprintf("CAST(REPLACE(REPLACE(%s, '.', ''), ',', '.') AS REAL)", id)
	case "dot":
		// "12345.67" ou REAL
		return fmt.Sprintf("CAST(%s AS REAL)", id)
	default:
		return id
	}
}

// Detecta se a columna parece "euro" ou "dot" (ou ningún) segundo mostras da propia táboa/WHERE
func detectNumericStyle(db *sql.DB, table, col, where string, args []any) string {
	// construír WHERE que garanta non nulos
	id := quoteIdent(col)
	where2 := where
	cond := fmt.Sprintf("%s IS NOT NULL AND TRIM(%s) <> ''", id, id)
	if strings.TrimSpace(where2) == "" {
		where2 = "WHERE " + cond
	} else {
		where2 = where2 + " AND " + cond
	}
	q := fmt.Sprintf("SELECT %s FROM %s %s LIMIT 50", id, quoteIdent(table), where2)

	rows, err := db.Query(q, args...)
	if err != nil {
		return ""
	}
	defer rows.Close()

	euro, dot := 0, 0
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			continue
		}
		s := strings.TrimSpace(fmt.Sprint(v))
		if euroNumRe.MatchString(s) {
			euro++
		}
		if dotNumRe.MatchString(s) {
			dot++
		}
	}
	if euro == 0 && dot == 0 {
		return ""
	}
	if euro >= dot {
		return "euro"
	}
	return "dot"
}

// 12.345,67 a partir dun float64
func formatEuroFloat(f float64) string {
	s := fmt.Sprintf("%.2f", f) // "12345.67"
	parts := strings.SplitN(s, ".", 2)
	intp, decp := parts[0], "00"
	if len(parts) > 1 {
		decp = parts[1]
	}
	// milleiros con puntos
	var b strings.Builder
	for i, n := range intp {
		if i > 0 && (len(intp)-i)%3 == 0 {
			b.WriteByte('.')
		}
		b.WriteRune(n)
	}
	return b.String() + "," + decp
}

// ==== conversores formatos de importes

func safeFile(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.Map(func(r rune) rune {
		if r == '_' || r == '-' || r == '.' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			return r
		}
		return -1
	}, s)
	if s == "" {
		s = "export"
	}
	return s
}

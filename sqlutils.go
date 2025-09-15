package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

// ==== Datos e utilidades SQL ====
type Column struct{ Name, Type string }

// --- Números europeos tipo "12.345,67", é como chegan do sqlite ao parsear da páxina ---
var (
	euroNumRe = regexp.MustCompile(`^\s*\d{1,3}(\.\d{3})*(,\d+)?\s*$`)
	dotNumRe  = regexp.MustCompile(`^\s*\d+(\.\d+)?\s*$`)
)

// ColNames devolve só os nomes das columnas.
func ColNames(cols []Column) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, c.Name)
	}
	return out
}

// Converte un campo numérico en formato "12.345,67" a REAL en SQLite
func sqlToRealEuro(expr string) string {
	return fmt.Sprintf("CAST(REPLACE(REPLACE(%s, '.', ''), ',', '.') AS REAL)", expr)
}

// asciiFold elimina diacríticos e pasa a minúsculas.
func asciiFold(s string) string {
	// Normalizamos a NFD e eliminamos marcas (Mn)
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range norm.NFD.String(s) {
		if unicode.Is(unicode.Mn, r) {
			continue // descarta a marca diacrítica
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

// Rexistra unha función SQL chamada unaccent_lower(text) -> text
func registerSQLiteFuncs(db *sql.DB) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Raw(func(dc any) error {
		c, ok := dc.(*sqlite3.SQLiteConn)
		if !ok {
			return nil
		}
		return c.RegisterFunc("unaccent_lower", func(s any) any {
			if s == nil {
				return ""
			}
			switch v := s.(type) {
			case string:
				// log.Printf("unaccent_lower string: [%s] [%s]", v, asciiFold(v))
				return asciiFold(v)
			default:
				// log.Printf("unaccent_lower string: [%s] [%s]", v, asciiFold(fmt.Sprint(v)))
				return asciiFold(fmt.Sprint(v))
			}
		}, true) // pure=true
	})
}

// --- Normalización simple ---

func openSQLite(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	// Read-only reforzado a nivel de sesión
	if _, err := db.Exec("PRAGMA query_only = ON"); err != nil {
		return nil, err
	}
	return db, nil
}

func listTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := []string{}
	for rows.Next() {
		var n string
		rows.Scan(&n)
		tables = append(tables, n)
	}
	return tables, nil
}

func tableColumns(db *sql.DB, table string) ([]Column, error) {
	q := fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(table))
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := []Column{}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt *string
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		res = append(res, Column{Name: name, Type: ctype})
	}
	return res, nil
}

// devolve true se existe a táboa
func tableExists(db *sql.DB, name string) bool {
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&n)
	return n > 0
}

// devolve a táboa de anexos asociada (base+"_files" ou base+"_file"), ou "" se non existe
func findFilesTable(db *sql.DB, base string) string {
	if tableExists(db, base+"_files") {
		return base + "_files"
	}
	if tableExists(db, base+"_file") {
		return base + "_file"
	}
	return ""
}

// escolle a primeira columna dispoñible na táboa (case-insensitive)
func pickFirstColumnName(cols []Column, candidates ...string) string {
	for _, want := range candidates {
		for _, c := range cols {
			if strings.EqualFold(c.Name, want) {
				return c.Name // devolvemos o nome exacto tal e como existe
			}
		}
	}
	return ""
}

// lista de táboas "base" (non *_files/_file)
func listBaseTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		if strings.HasSuffix(n, "_files") || strings.HasSuffix(n, "_file") {
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

// ==== SQL utils ====

// buildWhereLike crea unha WHERE con OR sobre as columnas, aplicando unaccent_lower, substitúe á anterior versión
func buildWhereLike(cols []string, q string) (string, []any) {
	q = strings.TrimSpace(q)
	if q == "" {
		return "", nil
	}
	like := "%" + asciiFold(q) + "%"
	parts := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, fmt.Sprintf("unaccent_lower(CAST(%s AS TEXT)) LIKE ?", quoteIdent(c)))
		// parts = append(parts, fmt.Sprintf("unaccent_lower(%s) LIKE ?", quoteIdent(c)))
		args = append(args, like)
	}
	return "WHERE (" + strings.Join(parts, " OR ") + ")", args
}

func countRows(db *sql.DB, table string, where string, args []any) (int, error) {
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", quoteIdent(table), where)
	row := db.QueryRow(q, args...)
	var n int
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func fetchPage(db *sql.DB, table string, cols []Column, where string, orderBy string, desc bool, page, perPage int, args []any) ([]map[string]any, error) {
	if page < 1 {
		page = 1
	}
	if perPage <= 0 {
		perPage = 50
	}

	ob := ""
	if orderBy != "" {
		// Detectar estilo numérico da columna (se aplica)
		style := detectNumericStyle(db, table, orderBy, where, args)
		if style != "" {
			ob = fmt.Sprintf("ORDER BY %s %s", numericOrderExpr(orderBy, style), map[bool]string{true: "DESC", false: "ASC"}[desc])
		} else {
			ob = fmt.Sprintf("ORDER BY %s %s", quoteIdent(orderBy), map[bool]string{true: "DESC", false: "ASC"}[desc])
		}
	}

	offset := (page - 1) * perPage
	selectCols := make([]string, len(cols))
	for i, c := range cols {
		selectCols[i] = quoteIdent(c.Name)
	}

	q := fmt.Sprintf("SELECT %s FROM %s %s %s LIMIT %d OFFSET %d", strings.Join(selectCols, ","), quoteIdent(table), where, ob, perPage, offset)
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []map[string]any{}
	cNames := make([]string, len(cols))
	for i, c := range cols {
		cNames[i] = c.Name
	}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := map[string]any{}
		for i, n := range cNames {
			m[n] = vals[i]
		}
		out = append(out, m)
	}

	return out, nil
}

// Ordena os datos da gráfica segundo a dirección elixida no formulario.
// Ordena a gráfica:
// - Se a columna é numérica (euro/dot): ordena por valor (k) en ASC/DESC segundo 'desc'.
// - Se é texto: se countDescOnText==true → ordena por COUNT(*) DESC; se non, por clave (k) ASC/DESC.
func histogramCounts(db *sql.DB, table, col, where string, args []any, limit int, desc bool, countDescOnText bool) (labels []string, counts []int, err error) {
	if col == "" {
		return nil, nil, errors.New("no column")
	}

	style := detectNumericStyle(db, table, col, where, args)
	tname := quoteIdent(table)
	orderDir := "ASC"
	if desc {
		orderDir = "DESC"
	}

	var q string
	if style != "" {
		// Numérico → clave REAL, ordenar por valor
		key := numericOrderExpr(col, style)
		q = fmt.Sprintf(`
			WITH vals AS (
				SELECT %s AS k FROM %s %s
			)
			SELECT k, COUNT(*) AS c
			FROM vals
			WHERE k IS NOT NULL
			GROUP BY k
			ORDER BY k %s
			LIMIT %d
		`, key, tname, where, orderDir, limit)
	} else {
		// Texto → ou ben por count DESC (modo web), ou pola clave (modo TUI)
		id := quoteIdent(col)
		if countDescOnText {
			q = fmt.Sprintf(`
				SELECT %s AS k, COUNT(*) AS c
				FROM %s %s
				GROUP BY %s
				ORDER BY c DESC
				LIMIT %d
			`, id, tname, where, id, limit)
		} else {
			q = fmt.Sprintf(`
				SELECT %s AS k, COUNT(*) AS c
				FROM %s %s
				GROUP BY %s
				ORDER BY %s %s
				LIMIT %d
			`, id, tname, where, id, id, orderDir, limit)
		}
	}

	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	if style != "" {
		for rows.Next() {
			var k sql.NullFloat64
			var c int
			if err := rows.Scan(&k, &c); err != nil {
				return nil, nil, err
			}
			if !k.Valid {
				continue
			}
			labels = append(labels, formatEuroFloat(k.Float64)) // exibir “12.345,67”
			counts = append(counts, c)
		}
	} else {
		for rows.Next() {
			var k sql.NullString
			var c int
			if err := rows.Scan(&k, &c); err != nil {
				return nil, nil, err
			}
			labels = append(labels, k.String)
			counts = append(counts, c)
		}
	}
	return labels, counts, nil
}

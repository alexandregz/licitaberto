// main.go
// Build/run:
//
//	go run . --db ./data.sqlite --mode web   # UI web en http://127.0.0.1:8080
//	go run . --db ./data.sqlite --mode tui   # UI TUI (terminal)
//
// Dependencias:
//
//	go get github.com/mattn/go-sqlite3
//	go get github.com/charmbracelet/bubbletea
//	go get github.com/charmbracelet/bubbles@v0.16.2
//	go get github.com/charmbracelet/lipgloss
//	go get github.com/xuri/excelize/v2
//
// Notas:
// - Read-only: activamos PRAGMA query_only=ON. Este programa non fai INSERT/UPDATE/DELETE.
// - Exportación: CSV e XLSX (Excel) da vista filtrada/ordenada.
// - Gráficas: no modo web úsase Chart.js; no modo TUI amósase un histograma ASCII.
// - Portabilidade: para migrar a Postgres/MySQL no futuro, substitúe o driver e os SQLs de detalles.

// by ChatGPT+pequenas correcións minhas
//

package main

import (
	"database/sql"
	"embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"

	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed webstatic/*
var webFS embed.FS

// ==== Datos e utilidades SQL ====

type Column struct{ Name, Type string }

// --- Números europeos tipo "12.345,67", é como chegan do sqlite ao parsear da páxina ---
var (
	euroNumRe = regexp.MustCompile(`^\s*\d{1,3}(\.\d{3})*(,\d+)?\s*$`)
	dotNumRe  = regexp.MustCompile(`^\s*\d+(\.\d+)?\s*$`)
)

var pdfPath string

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

// ==== utilidades ====
func quoteIdent(id string) string {
	// minimal: wrap with double quotes and escape existing quotes
	return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}

// remove extension
func stripExt(path string) string {
	ext := filepath.Ext(path)            // inclúe o punto: ".txt", ".gz", etc.
	return strings.TrimSuffix(path, ext) // elimina a última extensión
}

// Crea links dos PDFs fisicos
func createLinkPDF(taboa string) string {
	// hai que eliminar _files e é importante desde o raiz
	return fmt.Sprintf("/pdfs/%s", strings.TrimSuffix(taboa, "_files"))
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

// ==== SQL utils ====

// Build WHERE with LIKE across all columns (casting to TEXT when needed)
func buildWhereLike(cols []Column, q string) (string, []any) {
	q = strings.TrimSpace(q)
	if q == "" || len(cols) == 0 {
		return "", nil
	}
	parts := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, fmt.Sprintf("CAST(%s AS TEXT) LIKE ?", quoteIdent(c.Name)))
		args = append(args, "%"+q+"%") // ← un valor por cada "?"
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

// ==== Modo WEB ====

type server struct {
	db      *sql.DB
	tpl     *template.Template
	perPage int
}

//go:embed templates/*
var tplFS embed.FS

func newServer(db *sql.DB) (*server, error) {
	tpl := template.Must(
		template.New("").
			Funcs(template.FuncMap{
				"hasSuffix": strings.HasSuffix,  // comprobar sufixo
				"replace":   strings.ReplaceAll, // substituír substring
				"toLower":   strings.ToLower,    // pasar a minúsculas
				"toUpper":   strings.ToUpper,    // pasar a maiúsculas
				"hasPrefix": strings.HasPrefix,  // comprobar prefixo
				"trim":      strings.TrimSpace,  // quitar espazos arredor
			}).
			ParseFS(tplFS, "templates/*.gohtml"),
	)

	return &server{db: db, tpl: tpl, perPage: 25}, nil
}

func (s *server) routes(addr string) error {
	assets, err := fs.Sub(webFS, "webstatic")
	if err != nil {
		return err
	}

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(assets))))
	http.HandleFunc("/", s.handleIndex)
	http.HandleFunc("/table/", s.handleTable)
	http.HandleFunc("/export/csv", s.handleExportCSV)
	http.HandleFunc("/export/xlsx", s.handleExportXLSX)
	http.HandleFunc("/api/table/", s.handleAPITable) // ← API JSON para Instant Search

	http.Handle("/pdfs/", http.StripPrefix("/pdfs/", http.FileServer(http.Dir(pdfPath))))

	log.Printf("Web UI en http://%s", addr)
	log.Printf("PDFs en %s", http.Dir(pdfPath))

	return http.ListenAndServe(addr, nil)
}

// handlers
func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	tables, err := listTables(s.db)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = s.tpl.ExecuteTemplate(w, "index.gohtml", map[string]any{"Tables": tables})
}

func (s *server) handleTable(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/table/")
	if name == "" {
		http.NotFound(w, r)
		return
	}
	cols, err := tableColumns(s.db, name)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	q := r.URL.Query().Get("q")
	order := r.URL.Query().Get("order")
	dir := strings.ToUpper(r.URL.Query().Get("dir")) == "DESC"
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	chartBy := r.URL.Query().Get("chartBy")
	where, args := buildWhereLike(cols, q)
	total, err := countRows(s.db, name, where, args)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	pages := max(1, (total+s.perPage-1)/s.perPage)
	if page < 1 {
		page = 1
	}
	if page > pages {
		page = pages
	}
	rows, err := fetchPage(s.db, name, cols, where, order, dir, page, s.perPage, args)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	labels, counts, _ := histogramCounts(s.db, name, chartBy, where, args, 50, dir, true)
	labelsJSON, _ := json.Marshal(labels)
	countsJSON, _ := json.Marshal(counts)
	prev := 1
	if page > 1 {
		prev = page - 1
	}
	next := pages
	if page < pages {
		next = page + 1
	}
	_ = s.tpl.ExecuteTemplate(w, "table.gohtml", map[string]any{
		"Table":           name,
		"Cols":            cols,
		"Rows":            rows,
		"Q":               q,
		"Order":           order,
		"Desc":            dir,
		"Page":            page,
		"PerPage":         s.perPage,
		"Total":           total,
		"Pages":           pages,
		"HasPrev":         page > 1,
		"HasNext":         page < pages,
		"PrevPage":        prev,
		"NextPage":        next,
		"ChartBy":         chartBy,
		"ChartLabelsJSON": template.JS(labelsJSON),
		"ChartCountsJSON": template.JS(countsJSON),
		"PDFPath":         createLinkPDF(name),
	})
}

func (s *server) handleExportCSV(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("table")
	if name == "" {
		http.Error(w, "missing table", 400)
		return
	}

	cols, err := tableColumns(s.db, name)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	qParam := r.URL.Query().Get("q")
	order := r.URL.Query().Get("order")
	dir := strings.ToUpper(r.URL.Query().Get("dir")) == "DESC"
	where, args := buildWhereLike(cols, qParam)

	// export todo sen páxina
	rows, err := fetchPage(s.db, name, cols, where, order, dir, 1, 1_000_000, args)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=_%s_export.csv", safeFile(name)))
	csvw := csv.NewWriter(w)
	head := make([]string, len(cols))
	for i, c := range cols {
		head[i] = c.Name
	}

	_ = csvw.Write(head)
	for _, r := range rows {
		row := make([]string, len(cols))
		for i, c := range cols {
			s := fmt.Sprint(r[c.Name])
			if f, ok := parseEuroNumber(s); ok {
				// exportar normalizado con punto decimal (2 decimais para cartos)
				s = strconv.FormatFloat(f, 'f', 2, 64)
			}
			row[i] = s
		}
		_ = csvw.Write(row)
	}

	csvw.Flush()
}

func (s *server) handleExportXLSX(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("table")
	if name == "" {
		http.Error(w, "missing table", 400)
		return
	}
	cols, err := tableColumns(s.db, name)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	qParam := r.URL.Query().Get("q")
	order := r.URL.Query().Get("order")
	dir := strings.ToUpper(r.URL.Query().Get("dir")) == "DESC"
	where, args := buildWhereLike(cols, qParam)
	rows, err := fetchPage(s.db, name, cols, where, order, dir, 1, 1_000_000, args)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	f := excelize.NewFile()
	sheet := "Sheet1"
	head := make([]any, len(cols))
	for i, c := range cols {
		head[i] = c.Name
	}

	_ = f.SetSheetRow(sheet, "A1", &head)
	for i, r := range rows {
		row := make([]any, len(cols))
		for j, c := range cols {
			s := fmt.Sprint(r[c.Name])
			if fval, ok := parseEuroNumber(s); ok {
				row[j] = fval // número REAL -> Excel/LibreOffice verano como número
			} else {
				row[j] = s
			}
		}
		_ = f.SetSheetRow(sheet, fmt.Sprintf("A%d", i+2), &row)
	}

	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=_%s_export.xlsx", safeFile(name)))
	_ = f.Write(w)
	_ = f.Close()
}

// ==== API JSON (Instant Search) ====
func (s *server) handleAPITable(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/table/")
	if name == "" {
		http.Error(w, "missing table", 400)
		return
	}

	cols, err := tableColumns(s.db, name)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	q := r.URL.Query().Get("q")
	order := r.URL.Query().Get("order")
	dir := strings.ToUpper(r.URL.Query().Get("dir")) == "DESC"
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	chartBy := r.URL.Query().Get("chartBy")

	where, args := buildWhereLike(cols, q)
	total, err := countRows(s.db, name, where, args)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	pages := max(1, (total+s.perPage-1)/s.perPage)
	if page > pages {
		page = pages
	}

	rows, err := fetchPage(s.db, name, cols, where, order, dir, page, s.perPage, args)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// serializar filas a cadeas para JSON limpo
	srows := make([]map[string]string, len(rows))
	for i, rmap := range rows {
		m := make(map[string]string, len(cols))
		for _, c := range cols {
			m[c.Name] = fmt.Sprint(rmap[c.Name])
		}
		srows[i] = m
	}

	labels, counts, _ := histogramCounts(s.db, name, chartBy, where, args, 50, dir, true)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"table":       name,
		"columns":     colNames,
		"rows":        srows,
		"total":       total,
		"page":        page,
		"pages":       pages,
		"perPage":     s.perPage,
		"order":       order,
		"desc":        dir,
		"chartBy":     chartBy,
		"chartLabels": labels,
		"chartCounts": counts,
	})
}

// --

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

// ==== Modo TUI (Bubble Tea) ====

type tuiModel struct {
	db      *sql.DB
	tables  []string
	list    list.Model
	cols    []Column
	rows    []map[string]any
	q       string
	order   string
	desc    bool
	page    int
	perPage int
	table   string
	chartBy string
	input   textinput.Model
	status  string
	focus   int // 0=list, 1=table
}

func initialTUI(db *sql.DB) tuiModel {
	tabs, _ := listTables(db)
	items := make([]list.Item, len(tabs))
	for i, t := range tabs {
		items[i] = listItem(t)
	}
	l := list.New(items, list.NewDefaultDelegate(), 24, 20)
	l.Title = "Táboas"
	in := textinput.New()
	in.Placeholder = "buscar... (/ para focar)"
	return tuiModel{db: db, tables: tabs, list: l, perPage: 20, page: 1, input: in, focus: 0}
}

type listItem string

func (i listItem) FilterValue() string { return string(i) }
func (i listItem) Title() string       { return string(i) }
func (i listItem) Description() string { return "" }

func (m tuiModel) Init() tea.Cmd { return nil }

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		s := msg.String()
		switch s {
		case "ctrl+c", "Q":
			return m, tea.Quit
		case "/":
			m.focus = 1
			m.input.Focus()
			return m, nil
		case "enter":
			if m.focus == 0 {
				if it, ok := m.list.SelectedItem().(listItem); ok {
					m.table = string(it)
					m.page = 1
					return m.loadTable()
				}
			}
		case "E": // export CSV
			if m.table != "" {
				fn, err := m.exportCSV()
				if err != nil {
					m.status = err.Error()
				} else {
					m.status = "Exportado " + fn
				}
			}
		case "X": // export XLSX
			if m.table != "" {
				fn, err := m.exportXLSX()
				if err != nil {
					m.status = err.Error()
				} else {
					m.status = "Exportado " + fn
				}
			}
		case "C": // cycle chart column
			if len(m.cols) > 0 {
				idx := 0
				for i, c := range m.cols {
					if c.Name == m.chartBy {
						idx = i + 1
						break
					}
				}
				if idx >= len(m.cols) {
					idx = 0
				}
				m.chartBy = m.cols[idx].Name
			}
		case "N":
			m.page++
			return m.loadTable()
		case "P":
			if m.page > 1 {
				m.page--
			}
			return m.loadTable()
		case "O": // cycle order by
			if len(m.cols) > 0 {
				idx := 0
				for i, c := range m.cols {
					if c.Name == m.order {
						idx = i + 1
						break
					}
				}
				if idx >= len(m.cols) {
					idx = 0
				}
				m.order = m.cols[idx].Name
				return m.loadTable()
			}
		case "D":
			m.desc = !m.desc
			return m.loadTable()
		}
	case tea.WindowSizeMsg:
		m.list.SetSize(msg.Width/3, msg.Height-5)
	}
	if m.focus == 0 {
		var cmd tea.Cmd
		m.list, cmd = m.list.Update(msg)
		return m, cmd
	}
	// input focus
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	m.q = m.input.Value()
	m, _ = m.loadTable()
	return m, cmd
}

func (m tuiModel) View() string {
	left := lipgloss.NewStyle().Width(30).Render(m.list.View())
	rightSB := strings.Builder{}
	fmt.Fprintf(&rightSB, "Táboa: %s\n", m.table)
	fmt.Fprintf(&rightSB, "Busca [/]: %s\n", m.input.View())
	fmt.Fprintf(&rightSB, "Orden [O]: %s %s  · Páx [N/P]: %d\n", m.order, map[bool]string{true: "DESC", false: "ASC"}[m.desc], m.page)
	fmt.Fprintf(&rightSB, "%s\n\n", m.renderRows(10))
	if m.chartBy != "" {
		fmt.Fprintf(&rightSB, "Histograma [C] por %s\n%s\n", m.chartBy, m.renderHistogram())
	}
	fmt.Fprintf(&rightSB, "[enter] abrir  [E] CSV  [X] XLSX  [D] asc/desc  [Q] sair\n")
	fmt.Fprintf(&rightSB, "%s", m.status)
	right := lipgloss.NewStyle().Width(80).Render(rightSB.String())
	return lipgloss.JoinHorizontal(lipgloss.Top, left, right)
}

func (m *tuiModel) loadTable() (tuiModel, tea.Cmd) {
	if m.table == "" {
		return *m, nil
	}
	cols, err := tableColumns(m.db, m.table)
	if err != nil {
		m.status = err.Error()
		return *m, nil
	}
	m.cols = cols
	where, args := buildWhereLike(cols, m.q)
	rows, err := fetchPage(m.db, m.table, cols, where, m.order, m.desc, m.page, m.perPage, args)
	if err != nil {
		m.status = err.Error()
	} else {
		m.rows = rows
		m.status = fmt.Sprintf("%d filas (vista)", len(rows))
	}
	if m.chartBy == "" && len(cols) > 0 {
		m.chartBy = cols[0].Name
	}
	return *m, nil
}

func (m *tuiModel) renderRows(maxLines int) string {
	if len(m.cols) == 0 {
		return "(sen columnas)"
	}
	// header
	head := make([]string, len(m.cols))
	for i, c := range m.cols {
		head[i] = c.Name
	}
	lines := []string{strings.Join(head, " | ")}
	lines = append(lines, strings.Repeat("-", len(lines[0])))
	for i, r := range m.rows {
		if i >= maxLines {
			break
		}
		row := make([]string, len(m.cols))
		for j, c := range m.cols {
			v := fmt.Sprint(r[c.Name])
			if len(v) > 30 {
				v = v[:27] + "…"
			}
			row[j] = v
		}
		lines = append(lines, strings.Join(row, " | "))
	}
	return strings.Join(lines, "\n")
}

func (m *tuiModel) renderHistogram() string {
	if m.table == "" || m.chartBy == "" {
		return ""
	}
	where, args := buildWhereLike(m.cols, m.q)
	labels, counts, err := histogramCounts(m.db, m.table, m.chartBy, where, args, 20, m.desc, true)
	if err != nil || len(labels) == 0 {
		return "(sen datos)"
	}
	maxc := 0
	for _, c := range counts {
		if c > maxc {
			maxc = c
		}
	}
	maxBar := 40
	b := strings.Builder{}
	for i := range labels {
		bar := strings.Repeat("█", int(float64(counts[i])/float64(maxc)*float64(maxBar)))
		lab := labels[i]
		if lab == "" {
			lab = "(NULL)"
		}
		if len(lab) > 18 {
			lab = lab[:15] + "…"
		}
		fmt.Fprintf(&b, "%-18s | %-*s %d\n", lab, maxBar, bar, counts[i])
	}
	return b.String()
}

func (m *tuiModel) exportCSV() (string, error) {
	if m.table == "" {
		return "", errors.New("sen táboa")
	}
	cols := m.cols
	where, args := buildWhereLike(cols, m.q)
	rows, err := fetchPage(m.db, m.table, cols, where, m.order, m.desc, 1, 1_000_000, args)
	if err != nil {
		return "", err
	}
	fn := fmt.Sprintf("%s_export_%d.csv", safeFile(m.table), time.Now().Unix())
	f, err := os.Create(fn)
	if err != nil {
		return "", err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	head := make([]string, len(cols))
	for i, c := range cols {
		head[i] = c.Name
	}
	_ = w.Write(head)
	for _, r := range rows {
		row := make([]string, len(cols))
		for j, c := range cols {
			row[j] = fmt.Sprint(r[c.Name])
		}
		_ = w.Write(row)
	}
	w.Flush()
	return fn, w.Error()
}

func (m *tuiModel) exportXLSX() (string, error) {
	if m.table == "" {
		return "", errors.New("sen táboa")
	}
	cols := m.cols
	where, args := buildWhereLike(cols, m.q)
	rows, err := fetchPage(m.db, m.table, cols, where, m.order, m.desc, 1, 1_000_000, args)
	if err != nil {
		return "", err
	}
	fn := fmt.Sprintf("%s_export_%d.xlsx", safeFile(m.table), time.Now().Unix())
	f := excelize.NewFile()
	sheet := "Sheet1"
	head := make([]any, len(cols))
	for i, c := range cols {
		head[i] = c.Name
	}
	_ = f.SetSheetRow(sheet, "A1", &head)
	for i, r := range rows {
		row := make([]any, len(cols))
		for j, c := range cols {
			row[j] = fmt.Sprint(r[c.Name])
		}
		_ = f.SetSheetRow(sheet, fmt.Sprintf("A%d", i+2), &row)
	}
	if err := f.SaveAs(fn); err != nil {
		return "", err
	}
	_ = f.Close()
	return fn, nil
}

// ==== main ====

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func main() {
	// o dbPath tamen indica onde estaran os ficheiros PDF, entendendo que ao utilizar o scrapper
	//  https://github.com/alexandregz/plataforma_contratacion_estado_scrapper van ter esa estructura:
	// 	PDF/CONCELHO/TABOA/EXPEDIENTE/
	dbPath := flag.String("db", "./data.sqlite", "ruta ao ficheiro SQLite")
	mode := flag.String("mode", "web", "web|tui")
	addr := flag.String("addr", "127.0.0.1:8080", "enderezo para o modo web")
	flag.Parse()

	db, err := openSQLite(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// path fisico a PDFs. Hai que reemprazar "TABOA/EXPEDIENTE/" polo que toque "on the fly"
	pdfPath = filepath.Dir(*dbPath) + "/PDF" + stripExt(strings.TrimPrefix(*dbPath, filepath.Dir(*dbPath)))
	// log.Printf("pdfPath: %s", pdfPath)

	switch *mode {
	case "web":
		srv, err := newServer(db)
		if err != nil {
			log.Fatal(err)
		}
		if err := srv.routes(*addr); err != nil {
			log.Fatal(err)
		}
	case "tui":
		p := tea.NewProgram(initialTUI(db))
		if _, err := p.Run(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("modo descoñecido: %s", *mode)
	}
}

// Estrutura suxerida do proxecto:
//   main.go
//   templates/
//     index.gohtml
//     table.gohtml
//   webstatic/
//     pico.min.css

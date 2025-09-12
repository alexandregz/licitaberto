package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

// handlers
func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	tables, err := listTables(s.db)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = s.tpl.ExecuteTemplate(w, "index.gohtml", map[string]any{"Tables": tables, "concello": concello})
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
		"concello":        concello,
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

// /summary: páxina HTML con gráficas (filtrable por q e por table)
func (s *server) handleSummary(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	sel := strings.TrimSpace(r.URL.Query().Get("table"))
	if sel == "" {
		sel = "Alcaldia_contratos_menores"
	}

	bases, err := listBaseTables(s.db)
	if err != nil || len(bases) == 0 {
		http.Error(w, "non hai táboas", 500)
		return
	}
	found := false
	for _, b := range bases {
		if b == sel {
			found = true
			break
		}
	}
	if !found && !tableExists(s.db, sel) {
		sel = bases[0]
	}

	files := findFilesTable(s.db, sel)
	baseQ := quoteIdent(sel)

	// columnas para WHERE e detección de nomes
	cols, err := tableColumns(s.db, sel)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	where, args := buildWhereLike(cols, q)

	// detectar nomes de columnas clave
	tipoCol := pickFirstColumnName(cols, "Tipo", "TipoContrato", "Tipo_licitacion", "Tipo_licitación")
	importeCol := pickFirstColumnName(cols, "Importe", "Importe_con_iva", "Importe_con_IVE", "Importe_sin_iva", "Importe_sen_IVE")
	adxCol := pickFirstColumnName(cols, "Adxudicatario", "Adjudicatario", "Proveedor", "Contratista", "Empresa")

	var (
		tiposLabels   []string
		tiposCounts   []int
		impLabels     []string
		impTotals     []float64
		adxLabels     []string
		adxCounts     []int
		conPDF, total int
	)

	// 1) Número por Tipo (se existe columna)
	if tipoCol != "" {
		q1 := fmt.Sprintf(`
			SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen tipo)') AS k, COUNT(*) AS c
			FROM %s %s
			GROUP BY k
			ORDER BY c DESC
		`, quoteIdent(tipoCol), baseQ, where)
		rows, err := s.db.Query(q1, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for rows.Next() {
			var k string
			var c int
			if err := rows.Scan(&k, &c); err != nil {
				rows.Close()
				http.Error(w, err.Error(), 500)
				return
			}
			tiposLabels = append(tiposLabels, k)
			tiposCounts = append(tiposCounts, c)
		}
		rows.Close()
	}

	// 2) Importe total por Tipo (se existen ambas columnas)
	if tipoCol != "" && importeCol != "" {
		q2 := fmt.Sprintf(`
			SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen tipo)') AS k,
			       SUM(CAST(REPLACE(REPLACE(%s,'.',''),',','.') AS REAL)) AS total
			FROM %s %s
			GROUP BY k
			ORDER BY total DESC
		`, quoteIdent(tipoCol), quoteIdent(importeCol), baseQ, where)
		rows, err := s.db.Query(q2, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for rows.Next() {
			var k string
			var v float64
			if err := rows.Scan(&k, &v); err != nil {
				rows.Close()
				http.Error(w, err.Error(), 500)
				return
			}
			impLabels = append(impLabels, k)
			impTotals = append(impTotals, v)
		}
		rows.Close()
	}

	// 3) Top 10 adxudicatarios (se existe columna)
	if adxCol != "" {
		q3 := fmt.Sprintf(`
			SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen adxudicatario)') AS k, COUNT(*) AS c
			FROM %s %s
			GROUP BY k
			ORDER BY c DESC
			LIMIT 10
		`, quoteIdent(adxCol), baseQ, where)
		rows, err := s.db.Query(q3, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for rows.Next() {
			var k string
			var c int
			if err := rows.Scan(&k, &c); err != nil {
				rows.Close()
				http.Error(w, err.Error(), 500)
				return
			}
			adxLabels = append(adxLabels, k)
			adxCounts = append(adxCounts, c)
		}
		rows.Close()
	}

	// 4) Con anexos vs sen anexos
	if files != "" {
		q4 := fmt.Sprintf(`
			SELECT COUNT(DISTINCT m.Expediente)
			FROM %s m
			JOIN %s f ON m.Expediente = f.Expediente
			%s
		`, baseQ, quoteIdent(files), where)
		if err := s.db.QueryRow(q4, args...).Scan(&conPDF); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	qTotal := fmt.Sprintf(`SELECT COUNT(*) FROM %s %s`, baseQ, where)
	if err := s.db.QueryRow(qTotal, args...).Scan(&total); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	senPDF := total - conPDF

	// serializar para o template
	type js = template.JS
	lblTipos, _ := json.Marshal(tiposLabels)
	cntTipos, _ := json.Marshal(tiposCounts)
	lblImp, _ := json.Marshal(impLabels)
	valImp, _ := json.Marshal(impTotals)
	lblAdx, _ := json.Marshal(adxLabels)
	cntAdx, _ := json.Marshal(adxCounts)
	lblAnx, _ := json.Marshal([]string{"Con PDF", "Sen PDF"})
	cntAnx, _ := json.Marshal([]int{conPDF, senPDF})

	data := map[string]any{
		"Q": q, "Table": sel, "Tables": bases, "FilesTable": files,
		"TiposLabels": js(lblTipos), "TiposCounts": js(cntTipos),
		"ImpLabels": js(lblImp), "ImpTotals": js(valImp),
		"AdxLabels": js(lblAdx), "AdxCounts": js(cntAdx),
		"AnexosLabels": js(lblAnx), "AnexosCounts": js(cntAnx),
		"concello": concello,
	}
	if err := s.tpl.ExecuteTemplate(w, "summary.gohtml", data); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
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

// /api/summary: devolve os mesmos datos ca handleSummary pero en JSON
func (s *server) handleAPISummary(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	sel := strings.TrimSpace(r.URL.Query().Get("table"))
	if sel == "" {
		sel = "Alcaldia_contratos_menores"
	}

	// reutilizamos a lóxica de handleSummary (copiamos o miolo simplificado)
	cols, err := tableColumns(s.db, sel)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	where, args := buildWhereLike(cols, q)

	// detección de columnas
	tipoCol := pickFirstColumnName(cols, "Tipo", "TipoContrato", "Tipo_licitacion", "Tipo_licitación")
	importeCol := pickFirstColumnName(cols, "Importe", "Importe_con_iva", "Importe_con_IVE", "Importe_sin_iva", "Importe_sen_IVE")
	adxCol := pickFirstColumnName(cols, "Adxudicatario", "Adjudicatario", "Proveedor", "Contratista", "Empresa")
	files := findFilesTable(s.db, sel)
	baseQ := quoteIdent(sel)

	type resp struct {
		Table        string    `json:"table"`
		Q            string    `json:"q"`
		TiposLabels  []string  `json:"tiposLabels"`
		TiposCounts  []int     `json:"tiposCounts"`
		ImpLabels    []string  `json:"impLabels"`
		ImpTotals    []float64 `json:"impTotals"`
		AdxLabels    []string  `json:"adxLabels"`
		AdxCounts    []int     `json:"adxCounts"`
		AnexosLabels []string  `json:"anexosLabels"`
		AnexosCounts []int     `json:"anexosCounts"`
	}

	out := resp{Table: sel, Q: q}

	// queries iguais ás de handleSummary...
	if tipoCol != "" {
		q1 := fmt.Sprintf(`SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen tipo)'), COUNT(*) FROM %s %s GROUP BY 1 ORDER BY 2 DESC`,
			quoteIdent(tipoCol), baseQ, where)
		rows, err := s.db.Query(q1, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for rows.Next() {
			var k string
			var c int
			_ = rows.Scan(&k, &c)
			out.TiposLabels = append(out.TiposLabels, k)
			out.TiposCounts = append(out.TiposCounts, c)
		}
		rows.Close()
	}
	if tipoCol != "" && importeCol != "" {
		q2 := fmt.Sprintf(`SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen tipo)'), SUM(CAST(REPLACE(REPLACE(%s,'.',''),',','.') AS REAL))
			FROM %s %s GROUP BY 1 ORDER BY 2 DESC`, quoteIdent(tipoCol), quoteIdent(importeCol), baseQ, where)
		rows, err := s.db.Query(q2, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for rows.Next() {
			var k string
			var v float64
			_ = rows.Scan(&k, &v)
			out.ImpLabels = append(out.ImpLabels, k)
			out.ImpTotals = append(out.ImpTotals, v)
		}
		rows.Close()
	}
	if adxCol != "" {
		q3 := fmt.Sprintf(`SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen adxudicatario)'), COUNT(*) FROM %s %s GROUP BY 1 ORDER BY 2 DESC LIMIT 10`,
			quoteIdent(adxCol), baseQ, where)
		rows, err := s.db.Query(q3, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for rows.Next() {
			var k string
			var c int
			_ = rows.Scan(&k, &c)
			out.AdxLabels = append(out.AdxLabels, k)
			out.AdxCounts = append(out.AdxCounts, c)
		}
		rows.Close()
	}
	var conPDF, total int
	if files != "" {
		q4 := fmt.Sprintf(`SELECT COUNT(DISTINCT m.Expediente) FROM %s m JOIN %s f ON m.Expediente=f.Expediente %s`, baseQ, quoteIdent(files), where)
		_ = s.db.QueryRow(q4, args...).Scan(&conPDF)
	}
	_ = s.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s %s`, baseQ, where), args...).Scan(&total)
	out.AnexosLabels = []string{"Con PDF", "Sen PDF"}
	out.AnexosCounts = []int{conPDF, total - conPDF}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(out)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

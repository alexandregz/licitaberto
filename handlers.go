package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"sort"
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
	where, args := buildWhereLike(ColNames(cols), q)
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
	where, args := buildWhereLike(ColNames(cols), qParam)

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
	where, args := buildWhereLike(ColNames(cols), qParam)
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
	where, args := buildWhereLike(ColNames(cols), q)

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

	// === Top 20 maiores licitacións por importe (para a táboa sel) ===
	var topLicLabels []string
	var topLicAmounts []float64
	var topLicURLs []string
	var topLicObjects []string

	if importeCol != "" {
		// columnas auxiliares
		colOrEmpty := func(name string) string {
			if name == "" {
				return "''"
			}
			return quoteIdent(name)
		}
		expCol := pickFirstColumnName(cols, "Expediente")
		objCol := pickFirstColumnName(
			cols,
			"Objeto_del_contrato", "Objeto_del_Contrato", "ObjetoContrato",
			"Obxecto", "Objeto", "Asunto",
			"Descripcion", "Descripción",
			"Concepto", "Titulo", "Título",
		)

		qTop := fmt.Sprintf(`
		SELECT
			%[1]s AS expediente,
			%[2]s AS obxecto,
			%[3]s AS adx,
			%[4]s AS imp
		FROM %[5]s
		%[6]s
		ORDER BY imp DESC
		LIMIT 20
	`,
			colOrEmpty(expCol),
			colOrEmpty(objCol),
			colOrEmpty(adxCol),
			sqlToRealEuro(quoteIdent(importeCol)),
			baseQ,
			where,
		)

		if rows, err := s.db.Query(qTop, args...); err == nil {
			defer rows.Close()
			for rows.Next() {
				var exp, obj, adj sql.NullString
				var imp float64
				if err := rows.Scan(&exp, &obj, &adj, &imp); err == nil {
					// etiqueta: Obxecto do contrato (fallback a expediente/adxudicatario/táboa)
					label := strings.TrimSpace(obj.String)
					if label == "" && exp.Valid {
						label = strings.TrimSpace(exp.String)
					}
					if label == "" && adj.Valid {
						label = strings.TrimSpace(adj.String)
					}
					if label == "" {
						label = sel
					}
					if len(label) > 50 {
						label = label[:50] + "…"
					}

					// URL: /table/<filesTable OU base>/?q=<expediente>
					var urlStr string
					if exp.Valid && strings.TrimSpace(exp.String) != "" {
						urlStr = "/table/" + sel + "?q=" + url.QueryEscape(strings.TrimSpace(exp.String))
					}

					topLicLabels = append(topLicLabels, label)
					topLicAmounts = append(topLicAmounts, imp)
					topLicURLs = append(topLicURLs, urlStr)
					topLicObjects = append(topLicObjects, strings.TrimSpace(obj.String))
				}
			}
		}
	}

	// === Nº de adxudicacións e importe total por mes (para a táboa sel) ===
	var adxMesLabels []string
	var adxMesCounts []int
	var adxMesImportes []float64

	if importeCol != "" {
		lowSel := strings.ToLower(sel)

		// elixir columna de data segundo a táboa:
		// - *_contratos_menores → Estado
		// - *_licitacions      → Fechas
		var dateCol string
		switch {
		case strings.HasSuffix(lowSel, "_contratos_menores"):
			dateCol = "Estado"
		case strings.HasSuffix(lowSel, "_licitacions"):
			dateCol = "Fechas"
		default:
			dateCol = ""
		}

		if dateCol != "" {
			q5 := fmt.Sprintf(`
					SELECT
						SUBSTR(%[1]s, -7, 2) AS mes_publicacion,
						SUBSTR(%[1]s, -4, 4) AS ano_publicacion,
						COUNT(*) AS total,
						SUM(%[2]s) AS total_importe
					FROM %[3]s
					%[4]s
					GROUP BY ano_publicacion, mes_publicacion
					ORDER BY ano_publicacion, mes_publicacion
				`,
				dateCol,
				sqlToRealEuro(quoteIdent(importeCol)),
				baseQ,
				where,
			)

			if rows, err := s.db.Query(q5, args...); err == nil {
				defer rows.Close()
				type pair struct {
					K string
					C int
					I float64
				}
				tmp := []pair{}
				for rows.Next() {
					var mes, ano string
					var total int
					var totalImp float64
					if err := rows.Scan(&mes, &ano, &total, &totalImp); err == nil {
						tmp = append(tmp, pair{K: ano + "-" + mes, C: total, I: totalImp})
					}
				}
				sort.Slice(tmp, func(i, j int) bool { return tmp[i].K < tmp[j].K })
				for _, p := range tmp {
					adxMesLabels = append(adxMesLabels, p.K)
					adxMesCounts = append(adxMesCounts, p.C)
					adxMesImportes = append(adxMesImportes, p.I)
				}
			}
		}
	}

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

	lblTop, _ := json.Marshal(topLicLabels)
	amtTop, _ := json.Marshal(topLicAmounts)
	urlTop, _ := json.Marshal(topLicURLs)
	objTop, _ := json.Marshal(topLicObjects)

	lblMes, _ := json.Marshal(adxMesLabels)
	cntMes, _ := json.Marshal(adxMesCounts)
	impMes, _ := json.Marshal(adxMesImportes)

	data := map[string]any{
		"Q": q, "Table": sel, "Tables": bases, "FilesTable": files,
		"TiposLabels": js(lblTipos), "TiposCounts": js(cntTipos),
		"ImpLabels": js(lblImp), "ImpTotals": js(valImp),
		"AdxLabels": js(lblAdx), "AdxCounts": js(cntAdx),
		"AnexosLabels": js(lblAnx), "AnexosCounts": js(cntAnx),
		"concello":       concello,
		"TopLicLabels":   template.JS(lblTop),
		"TopLicAmounts":  template.JS(amtTop),
		"TopLicURLs":     template.JS(urlTop),
		"TopLicObjects":  template.JS(objTop),
		"AdxMesLabels":   template.JS(lblMes),
		"AdxMesCounts":   template.JS(cntMes),
		"AdxMesImportes": template.JS(impMes),
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

	where, args := buildWhereLike(ColNames(cols), q)
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// /summary_all: agregados globais sobre todas as táboas base
func (s *server) handleSummaryAll(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))

	// 1) táboas base (non *_files/_file)
	bases, err := listBaseTables(s.db)
	if err != nil || len(bases) == 0 {
		http.Error(w, "non hai táboas", 500)
		return
	}

	// acumuladores
	tiposCount := map[string]int{}
	tiposImporte := map[string]float64{}
	adxCount := map[string]int{}
	adxMensual := map[string]int{}
	adxMensualImporte := map[string]float64{}

	// para barras apiladas: conteos por táboa e por mes
	countsByMonth := map[string]map[string]int{} // table -> month(YYYY-MM) -> count
	var stackTables []string                     // orde estable das táboas

	// ── NOVO: acumuladores por entidade (para barras apiladas)
	tiposCountByTable := map[string]map[string]int{}       // table -> tipo -> count
	tiposImporteByTable := map[string]map[string]float64{} // table -> tipo -> importe total
	adxCountByTable := map[string]map[string]int{}         // table -> adxudicatario -> count

	// manter orde estable de táboas
	var stackTablesAll []string

	type topItem struct {
		Label  string
		Amount float64
		URL    string
		Object string
	}
	var topLic []topItem

	var conPDF, total int

	for _, sel := range bases {
		baseQ := quoteIdent(sel)

		// columnas dispoñibles nesta táboa
		cols, err := tableColumns(s.db, sel)
		if err != nil {
			continue
		}

		where, args := buildWhereLike(ColNames(cols), q)

		// detectar columnas desta táboa
		tipoCol := pickFirstColumnName(cols, "Tipo", "TipoContrato", "Tipo_licitacion", "Tipo_licitación")
		importeCol := pickFirstColumnName(cols, "Importe", "Importe_con_iva", "Importe_con_IVE", "Importe_sin_iva", "Importe_sen_IVE")
		adxCol := pickFirstColumnName(cols, "Adxudicatario", "Adjudicatario", "Proveedor", "Contratista", "Empresa")

		// nº por Tipo
		if tipoCol != "" {
			q1 := fmt.Sprintf(`
				SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen tipo)') AS k, COUNT(*) AS c
				FROM %s %s
				GROUP BY k
			`, quoteIdent(tipoCol), baseQ, where)
			rows, err := s.db.Query(q1, args...)
			if err == nil {
				for rows.Next() {
					var k string
					var c int
					_ = rows.Scan(&k, &c)
					tiposCount[k] += c

					// ── por entidade (para apilar)
					if _, ok := tiposCountByTable[sel]; !ok {
						tiposCountByTable[sel] = map[string]int{}
						stackTablesAll = append(stackTablesAll, sel)
					}
					tiposCountByTable[sel][k] += c
				}
				rows.Close()
			}
		}

		// importe total por Tipo
		if tipoCol != "" && importeCol != "" {
			q2 := fmt.Sprintf(`
				SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen tipo)') AS k,
				       SUM(CAST(REPLACE(REPLACE(%s,'.',''),',','.') AS REAL)) AS total
				FROM %s %s
				GROUP BY k
			`, quoteIdent(tipoCol), quoteIdent(importeCol), baseQ, where)
			rows, err := s.db.Query(q2, args...)
			if err == nil {
				for rows.Next() {
					var k string
					var v float64
					_ = rows.Scan(&k, &v)
					tiposImporte[k] += v

					// ── por entidade (para apilar)
					if _, ok := tiposImporteByTable[sel]; !ok {
						tiposImporteByTable[sel] = map[string]float64{}
						// stackTablesAll xa se encheu antes, non fai falla repetir
					}
					tiposImporteByTable[sel][k] += v

				}
				rows.Close()
			}
		}

		// === Top adxudicatarios por táboa: detecta columna e agrega ===
		if adxCol := pickAdjCol(s.db, sel); adxCol != "" {
			// normalizamos o nome para agrupar (baixa, trim). Pero devolvemos o "display" (primeiro visto).
			q3 := fmt.Sprintf(`
					SELECT
						LOWER(TRIM(CAST(%[1]s AS TEXT))) as keynorm,
						COALESCE(NULLIF(TRIM(CAST(%[1]s AS TEXT)), ''), '(sen nome)') as display,
						COUNT(*) as c
					FROM %[2]s
					%[3]s
					GROUP BY keynorm
					ORDER BY c DESC
					LIMIT 100
				`, quoteIdent(adxCol), baseQ, where)

			if rows, err := s.db.Query(q3, args...); err == nil {
				defer rows.Close()
				// mapa da táboa
				m, ok := adxCountByTable[sel]
				if !ok {
					m = map[string]int{}
					adxCountByTable[sel] = m
				}
				for rows.Next() {
					var keynorm, display string
					var c int
					if err := rows.Scan(&keynorm, &display, &c); err == nil {
						if display == "" {
							display = "(sen nome)"
						}
						// global (para escoller top 10 final)
						adxCount[display] += c
						// por táboa (para pila)
						m[display] += c
					}
				}
			}
		}

		lowSel := strings.ToLower(sel)

		// totais importes e num. licitacións agrupadas por mes
		if !strings.HasSuffix(lowSel, "_files") && importeCol != "" {
			lowSel := strings.ToLower(sel)

			// columna que se emprega para recolher a data: Estado en _contratos_menores e Fechas en _licitacions.
			// Em ámbolos casos os últimos caracteres son "DD/MM/YYYY"
			var dateCol string
			switch {
			case strings.HasSuffix(lowSel, "_contratos_menores"):
				dateCol = "Estado"
			case strings.HasSuffix(lowSel, "_licitacions"):
				dateCol = "Fechas"
			default:
				dateCol = "" // non se aplica
			}

			q5 := fmt.Sprintf(`
					SELECT
						SUBSTR(%[1]s, -7, 2) AS mes_publicacion,
						SUBSTR(%[1]s, -4, 4) AS ano_publicacion,
						COUNT(*) AS total,
						SUM(%[2]s) AS total_importe
					FROM %[3]s
					%[4]s
					GROUP BY ano_publicacion, mes_publicacion
					ORDER BY ano_publicacion, mes_publicacion
				`,
				dateCol,
				sqlToRealEuro(quoteIdent(importeCol)),
				sel,
				where,
			)
			// if sel == "Alcaldia_contratos_menores" {
			// 	log.Printf("[DEBUG q5 SQL] %s ARGS=%v", q5, args)
			// }

			if rows, err := s.db.Query(q5, args...); err == nil {
				defer rows.Close()
				for rows.Next() {
					var mes, ano string
					var total int
					var totalImporte float64
					if err := rows.Scan(&mes, &ano, &total, &totalImporte); err == nil {
						key := ano + "-" + mes // YYYY-MM
						adxMensual[key] += total
						adxMensualImporte[key] += totalImporte

						// apilar por táboa
						m, ok := countsByMonth[sel]
						if !ok {
							m = map[string]int{}
							countsByMonth[sel] = m
							stackTables = append(stackTables, sel) // gardar orde de aparición
						}
						m[key] += total
					}
				}
			}
		}

		// con/total PDF para esta táboa
		files := findFilesTable(s.db, sel)
		if files != "" {
			q4 := fmt.Sprintf(`
				SELECT COUNT(DISTINCT m.Expediente)
				FROM %s m
				JOIN %s f ON m.Expediente = f.Expediente
				%s
			`, baseQ, quoteIdent(files), where)
			var part int
			_ = s.db.QueryRow(q4, args...).Scan(&part)
			conPDF += part
		}
		var partTot int
		_ = s.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s %s`, baseQ, where), args...).Scan(&partTot)
		total += partTot

		// query para top20 importes
		if importeCol != "" {
			// columnas auxiliares (se non existen, devolvemos cadea baleira para non romper Scan)
			colOrEmpty := func(name string) string {
				if name == "" {
					return "''"
				}
				return quoteIdent(name)
			}
			expCol := pickFirstColumnName(cols, "Expediente")
			objCol := pickFirstColumnName(cols, "Objeto_del_contrato", "Objeto_del_Contrato", "ObjetoContrato", "Obxecto", "Objeto", "Asunto",
				"Descripcion", "Descripción",
				"Concepto", "Titulo", "Título",
			)

			qTop := fmt.Sprintf(`
					SELECT
						%s AS expediente,
						%s AS obxecto,
						%s AS adx,
						%s AS imp
					FROM %s
					%s
					ORDER BY imp DESC
					LIMIT 20
    `,
				colOrEmpty(expCol),
				colOrEmpty(objCol),
				colOrEmpty(adxCol),
				sqlToRealEuro(quoteIdent(importeCol)),
				quoteIdent(sel),
				where,
			)

			if rows, err := s.db.Query(qTop, args...); err == nil {
				for rows.Next() {
					var exp, obj, adj sql.NullString
					var imp float64
					if err := rows.Scan(&exp, &obj, &adj, &imp); err == nil {
						// etiqueta: damos prioridade ao Objeto del contrato
						label := ""
						if obj.Valid && strings.TrimSpace(obj.String) != "" {
							label = strings.TrimSpace(obj.String)
						}
						if label == "" && exp.Valid && strings.TrimSpace(exp.String) != "" {
							label = strings.TrimSpace(exp.String)
						}
						if label == "" && adj.Valid && strings.TrimSpace(adj.String) != "" {
							label = strings.TrimSpace(adj.String)
						}
						if label == "" {
							label = sel
						}
						if len(label) > 50 {
							label = label[:50] + "…"
						}

						//
						var urlStr string
						if exp.Valid && strings.TrimSpace(exp.String) != "" {
							urlStr = "/table/" + sel + "?q=" + url.QueryEscape(strings.TrimSpace(exp.String))
						}

						topLic = append(topLic, topItem{Label: label, Amount: imp, URL: urlStr, Object: strings.TrimSpace(obj.String)})
					}
				}
				rows.Close()
			}
		}
	}

	// pasar mapas a arrays ordenados
	type kvI struct {
		K string
		V int
	}
	type kvF struct {
		K string
		V float64
	}

	// tiposCount -> desc por V
	tiposArr := make([]kvI, 0, len(tiposCount))
	for k, v := range tiposCount {
		tiposArr = append(tiposArr, kvI{k, v})
	}
	sort.Slice(tiposArr, func(i, j int) bool { return tiposArr[i].V > tiposArr[j].V })
	var tiposLabels []string
	var tiposCounts []int
	for _, p := range tiposArr {
		tiposLabels = append(tiposLabels, p.K)
		tiposCounts = append(tiposCounts, p.V)
	}
	// ── matriz apilada por entidade para "Número por Tipo"
	var tiposSeries []string     // nomes de táboas
	var tiposCountsStack [][]int // [serie][labelIndex]
	tiposSeries = append(tiposSeries, stackTablesAll...)
	for _, t := range tiposSeries {
		row := make([]int, len(tiposLabels))
		m := tiposCountByTable[t]
		for i, lab := range tiposLabels {
			row[i] = m[lab]
		}
		tiposCountsStack = append(tiposCountsStack, row)
	}
	// ---

	// tiposImporte -> desc por V
	impArr := make([]kvF, 0, len(tiposImporte))
	for k, v := range tiposImporte {
		impArr = append(impArr, kvF{k, v})
	}
	sort.Slice(impArr, func(i, j int) bool { return impArr[i].V > impArr[j].V })
	var impLabels []string
	var impTotals []float64
	for _, p := range impArr {
		impLabels = append(impLabels, p.K)
		impTotals = append(impTotals, p.V)
	}
	// ── matriz apilada por entidade para "Importe total por Tipo"
	var impSeries []string
	var impTotalsStack [][]float64 // [serie][labelIndex]
	impSeries = append(impSeries, stackTablesAll...)
	for _, t := range impSeries {
		row := make([]float64, len(impLabels))
		m := tiposImporteByTable[t]
		for i, lab := range impLabels {
			row[i] = m[lab]
		}
		impTotalsStack = append(impTotalsStack, row)
	}
	// ---

	// adxCount -> top 10 desc
	adxArr := make([]kvI, 0, len(adxCount))
	for k, v := range adxCount {
		adxArr = append(adxArr, kvI{k, v})
	}
	sort.Slice(adxArr, func(i, j int) bool { return adxArr[i].V > adxArr[j].V })
	if len(adxArr) > 10 {
		adxArr = adxArr[:10]
	}
	var adxLabels []string
	var adxCounts []int
	for _, p := range adxArr {
		adxLabels = append(adxLabels, p.K)
		adxCounts = append(adxCounts, p.V)
	}
	// ── matriz apilada por entidade para "Top 10 adxudicatarios"
	var adxSeries []string
	var adxCountsStack [][]int // [serie][labelIndex]
	adxSeries = append(adxSeries, stackTablesAll...)
	for _, t := range adxSeries {
		row := make([]int, len(adxLabels))
		m := adxCountByTable[t]
		for i, lab := range adxLabels {
			row[i] = m[lab]
		}
		adxCountsStack = append(adxCountsStack, row)
	}
	// ---

	// JSON seguro para template
	type js = template.JS
	lblTipos, _ := json.Marshal(tiposLabels)
	cntTipos, _ := json.Marshal(tiposCounts)
	lblImp, _ := json.Marshal(impLabels)
	valImp, _ := json.Marshal(impTotals)
	lblAdx, _ := json.Marshal(adxLabels)
	cntAdx, _ := json.Marshal(adxCounts)
	lblAnx, _ := json.Marshal([]string{"Con PDF", "Sen PDF"})
	cntAnx, _ := json.Marshal([]int{conPDF, total - conPDF})
	// ── NOVO: serialización para apiladas
	seriesTipos, _ := json.Marshal(tiposSeries)
	stackTipos, _ := json.Marshal(tiposCountsStack)
	seriesImp, _ := json.Marshal(impSeries)
	stackImp, _ := json.Marshal(impTotalsStack)
	seriesAdx, _ := json.Marshal(adxSeries)
	stackAdx, _ := json.Marshal(adxCountsStack)

	// meses ordenados
	mArr := make([]struct {
		K string
		V int
	}, 0, len(adxMensual))
	for k, v := range adxMensual {
		mArr = append(mArr, struct {
			K string
			V int
		}{k, v})
	}
	sort.Slice(mArr, func(i, j int) bool { return mArr[i].K < mArr[j].K })

	var adxMesLabels []string
	var adxMesCounts []int
	var adxMesImportes []float64
	for _, p := range mArr {
		adxMesLabels = append(adxMesLabels, p.K)
		adxMesCounts = append(adxMesCounts, p.V)
		adxMesImportes = append(adxMesImportes, adxMensualImporte[p.K])
	}

	// series apiladas por táboa (misma orde que stackTables)
	var adxMesSeries []string     // nomes das táboas
	var adxMesCountsStack [][]int // matriz [serie][mesIndex]
	for _, t := range stackTables {
		adxMesSeries = append(adxMesSeries, t)
		row := make([]int, len(adxMesLabels))
		byMonth := countsByMonth[t]
		for i, mm := range adxMesLabels {
			if byMonth[mm] > 0 {
				row[i] = byMonth[mm]
			}
		}
		adxMesCountsStack = append(adxMesCountsStack, row)
	}

	// serializar a JSON para o template
	lblMes, _ := json.Marshal(adxMesLabels)
	cntMes, _ := json.Marshal(adxMesCounts)
	impMes, _ := json.Marshal(adxMesImportes)
	// barras apiladas
	seriesMes, _ := json.Marshal(adxMesSeries)
	stackMes, _ := json.Marshal(adxMesCountsStack)

	sort.Slice(topLic, func(i, j int) bool { return topLic[i].Amount > topLic[j].Amount })
	if len(topLic) > 20 {
		topLic = topLic[:20]
	}

	var topLicLabels []string
	var topLicAmounts []float64
	var topLicURLs []string
	var topLicObjects []string
	for _, it := range topLic {
		topLicLabels = append(topLicLabels, it.Label)
		topLicAmounts = append(topLicAmounts, it.Amount)
		topLicURLs = append(topLicURLs, it.URL)
		topLicObjects = append(topLicObjects, it.Object)
	}

	lblTop, _ := json.Marshal(topLicLabels)
	amtTop, _ := json.Marshal(topLicAmounts)
	urlTop, _ := json.Marshal(topLicURLs)
	objTop, _ := json.Marshal(topLicObjects)

	data := map[string]any{
		"Q": q, "Tables": bases,
		"TiposLabels": js(lblTipos), "TiposCounts": js(cntTipos),
		"ImpLabels": js(lblImp), "ImpTotals": js(valImp),
		"AdxLabels": js(lblAdx), "AdxCounts": js(cntAdx),
		"AnexosLabels": js(lblAnx), "AnexosCounts": js(cntAnx),
		"concello":          concello,
		"AdxMesLabels":      template.JS(lblMes),
		"AdxMesCounts":      template.JS(cntMes),
		"AdxMesImportes":    template.JS(impMes),
		"TopLicLabels":      template.JS(lblTop),
		"TopLicAmounts":     template.JS(amtTop),
		"TopLicURLs":        template.JS(urlTop),
		"TopLicObjects":     template.JS(objTop),
		"AdxMesSeries":      template.JS(seriesMes),
		"AdxMesCountsStack": template.JS(stackMes),
		"TiposSeries":       template.JS(seriesTipos),
		"TiposCountsStack":  template.JS(stackTipos),
		"ImpSeries":         template.JS(seriesImp),
		"ImpTotalsStack":    template.JS(stackImp),
		"AdxSeries":         template.JS(seriesAdx),
		"AdxCountsStack":    template.JS(stackAdx),
	}
	if err := s.tpl.ExecuteTemplate(w, "summary_all.gohtml", data); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

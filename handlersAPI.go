package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

func (s *server) handleAPISummaryAll(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))

	bases, err := listBaseTables(s.db)
	if err != nil || len(bases) == 0 {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tiposLabels": []string{}, "tiposCounts": []int{},
			"impLabels": []string{}, "impTotals": []float64{},
			"adxLabels": []string{}, "adxCounts": []int{},
			"anexosLabels": []string{"Con PDF", "Sen PDF"}, "anexosCounts": []int{0, 0},
		})
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

	// para apilar o resto de barras tamén
	tiposCountByTable := map[string]map[string]int{}
	tiposImporteByTable := map[string]map[string]float64{}
	adxCountByTable := map[string]map[string]int{}
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
		cols, err := tableColumns(s.db, sel)
		if err != nil {
			continue
		}
		where, args := buildWhereLike(ColNames(cols), q)

		// detectar columnas desta táboa
		tipoCol := pickFirstColumnName(cols, "Tipo", "TipoContrato", "Tipo_licitacion", "Tipo_licitación")
		importeCol := pickFirstColumnName(cols, "Importe", "Importe_con_iva", "Importe_con_IVE", "Importe_sin_iva", "Importe_sen_IVE")
		adxCol := pickFirstColumnName(cols, "Adxudicatario", "Adjudicatario", "Proveedor", "Contratista", "Empresa")

		if tipoCol != "" {
			q1 := fmt.Sprintf(`SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen tipo)'), COUNT(*) FROM %s %s GROUP BY 1`,
				quoteIdent(tipoCol), baseQ, where)
			if rows, err := s.db.Query(q1, args...); err == nil {
				for rows.Next() {
					var k string
					var c int
					_ = rows.Scan(&k, &c)
					tiposCount[k] += c

					// -- para barras apiladas
					if _, ok := tiposCountByTable[sel]; !ok {
						tiposCountByTable[sel] = map[string]int{}
						stackTablesAll = append(stackTablesAll, sel)
					}
					tiposCountByTable[sel][k] += c
				}
				rows.Close()
			}
		}
		if tipoCol != "" && importeCol != "" {
			q2 := fmt.Sprintf(`SELECT COALESCE(NULLIF(TRIM(%s),''),'(Sen tipo)'),
			                   SUM(CAST(REPLACE(REPLACE(%s,'.',''),',','.') AS REAL))
			                   FROM %s %s GROUP BY 1`,
				quoteIdent(tipoCol), quoteIdent(importeCol), baseQ, where)
			if rows, err := s.db.Query(q2, args...); err == nil {
				for rows.Next() {
					var k string
					var v float64
					_ = rows.Scan(&k, &v)
					tiposImporte[k] += v

					// -- para barras apiladas
					if _, ok := tiposImporteByTable[sel]; !ok {
						tiposImporteByTable[sel] = map[string]float64{}
					}
					tiposImporteByTable[sel][k] += v
				}
				rows.Close()
			}
		}

		// === Top adxudicatarios por táboa: detecta columna e agrega ===
		if adjCol := pickAdjCol(s.db, sel); adjCol != "" {
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
				`, quoteIdent(adjCol), sel, where)

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
						// global
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

						// log.Printf("[DEBUG q5] %s-%s → total=%d importe=%.2f", ano, mes, total, totalImporte)

						m, ok := countsByMonth[sel]
						if !ok {
							m = map[string]int{}
							countsByMonth[sel] = m
							stackTables = append(stackTables, sel)
						}
						m[key] += total
					}
				}
			}
		}

		files := findFilesTable(s.db, sel)
		if files != "" {
			q4 := fmt.Sprintf(`SELECT COUNT(DISTINCT m.Expediente) FROM %s m JOIN %s f ON m.Expediente=f.Expediente %s`,
				baseQ, quoteIdent(files), where)
			var part int
			_ = s.db.QueryRow(q4, args...).Scan(&part)
			conPDF += part
		}
		var partTot int
		_ = s.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s %s`, baseQ, where), args...).Scan(&partTot)
		total += partTot

		// query para top10 importes
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

						// URL: /table/<filesTable OU base>/?q=<expediente>
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

	type kvI struct {
		K string
		V int
	}
	type kvF struct {
		K string
		V float64
	}

	tArr := make([]kvI, 0, len(tiposCount))
	for k, v := range tiposCount {
		tArr = append(tArr, kvI{k, v})
	}
	sort.Slice(tArr, func(i, j int) bool { return tArr[i].V > tArr[j].V })
	var tiposLabels []string
	var tiposCounts []int
	for _, p := range tArr {
		tiposLabels = append(tiposLabels, p.K)
		tiposCounts = append(tiposCounts, p.V)
	}

	iArr := make([]kvF, 0, len(tiposImporte))
	for k, v := range tiposImporte {
		iArr = append(iArr, kvF{k, v})
	}
	sort.Slice(iArr, func(i, j int) bool { return iArr[i].V > iArr[j].V })
	var impLabels []string
	var impTotals []float64
	for _, p := range iArr {
		impLabels = append(impLabels, p.K)
		impTotals = append(impTotals, p.V)
	}

	aArr := make([]kvI, 0, len(adxCount))
	for k, v := range adxCount {
		aArr = append(aArr, kvI{k, v})
	}
	sort.Slice(aArr, func(i, j int) bool { return aArr[i].V > aArr[j].V })
	if len(aArr) > 10 {
		aArr = aArr[:10]
	}
	var adxLabels []string
	var adxCounts []int
	for _, p := range aArr {
		adxLabels = append(adxLabels, p.K)
		adxCounts = append(adxCounts, p.V)
	}

	// Tipos (count)
	var tiposSeries []string
	var tiposCountsStack [][]int
	tiposSeries = append(tiposSeries, stackTablesAll...)
	for _, t := range tiposSeries {
		row := make([]int, len(tiposLabels))
		m := tiposCountByTable[t]
		for i, lab := range tiposLabels {
			row[i] = m[lab]
		}
		tiposCountsStack = append(tiposCountsStack, row)
	}

	// Tipos (importe)
	var impSeries []string
	var impTotalsStack [][]float64
	impSeries = append(impSeries, stackTablesAll...)
	for _, t := range impSeries {
		row := make([]float64, len(impLabels))
		m := tiposImporteByTable[t]
		for i, lab := range impLabels {
			row[i] = m[lab]
		}
		impTotalsStack = append(impTotalsStack, row)
	}

	// Adxudicatarios (Top 10)
	var adxSeries []string
	var adxCountsStack [][]int
	adxSeries = append(adxSeries, stackTablesAll...)
	for _, t := range adxSeries {
		row := make([]int, len(adxLabels))
		m := adxCountByTable[t]
		for i, lab := range adxLabels {
			row[i] = m[lab]
		}
		adxCountsStack = append(adxCountsStack, row)
	}

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
	var adxMesSeries []string
	var adxMesCountsStack [][]int
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

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"q":           q,
		"tiposLabels": tiposLabels, "tiposCounts": tiposCounts,
		"impLabels": impLabels, "impTotals": impTotals,
		"adxLabels": adxLabels, "adxCounts": adxCounts,
		"anexosLabels":      []string{"Con PDF", "Sen PDF"},
		"anexosCounts":      []int{conPDF, total - conPDF},
		"adxMesLabels":      adxMesLabels,
		"adxMesCounts":      adxMesCounts,
		"adxMesImportes":    adxMesImportes,
		"topLicLabels":      topLicLabels,
		"topLicAmounts":     topLicAmounts,
		"topLicUrls":        topLicURLs,
		"topLicObjects":     topLicObjects,
		"adxMesSeries":      adxMesSeries,
		"adxMesCountsStack": adxMesCountsStack,
		"tiposSeries":       tiposSeries,
		"tiposCountsStack":  tiposCountsStack,
		"impSeries":         impSeries,
		"impTotalsStack":    impTotalsStack,
		"adxSeries":         adxSeries,
		"adxCountsStack":    adxCountsStack,
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
	where, args := buildWhereLike(ColNames(cols), q)

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

		// Top 20 por importe
		TopLicLabels  []string  `json:"topLicLabels"`
		TopLicAmounts []float64 `json:"topLicAmounts"`
		TopLicUrls    []string  `json:"topLicUrls"`
		TopLicObjects []string  `json:"topLicObjects"`

		// Mes a mes: conta e importes
		AdxMesLabels   []string  `json:"adxMesLabels"`
		AdxMesCounts   []int     `json:"adxMesCounts"`
		AdxMesImportes []float64 `json:"adxMesImportes"`
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

	// Nº de adxudicacións e importes por mes (API)
	if importeCol != "" {
		lowSel := strings.ToLower(sel)
		var dateCol string
		switch {
		case strings.HasSuffix(lowSel, "_contratos_menores"):
			dateCol = "Estado"
		case strings.HasSuffix(lowSel, "_licitacions"):
			dateCol = "Fechas"
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
					var c int
					var imp float64
					if err := rows.Scan(&mes, &ano, &c, &imp); err == nil {
						tmp = append(tmp, pair{K: ano + "-" + mes, C: c, I: imp})
					}
				}
				sort.Slice(tmp, func(i, j int) bool { return tmp[i].K < tmp[j].K })
				for _, p := range tmp {
					out.AdxMesLabels = append(out.AdxMesLabels, p.K)
					out.AdxMesCounts = append(out.AdxMesCounts, p.C)
					out.AdxMesImportes = append(out.AdxMesImportes, p.I)
				}
			}
		}
	}

	// Top 20 por importe (para API)
	var topLicLabels []string
	var topLicAmounts []float64
	var topLicUrls []string
	var topLicObjects []string

	if importeCol != "" {
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

					//
					var urlStr string
					if exp.Valid && strings.TrimSpace(exp.String) != "" {
						urlStr = "/table/" + sel + "?q=" + url.QueryEscape(strings.TrimSpace(exp.String))
					}

					topLicLabels = append(topLicLabels, label)
					topLicAmounts = append(topLicAmounts, imp)
					topLicUrls = append(topLicUrls, urlStr)
					topLicObjects = append(topLicObjects, strings.TrimSpace(obj.String))
				}
			}
		}
	}

	_ = s.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s %s`, baseQ, where), args...).Scan(&total)
	out.AnexosLabels = []string{"Con PDF", "Sen PDF"}
	out.AnexosCounts = []int{conPDF, total - conPDF}

	out.TopLicLabels = topLicLabels
	out.TopLicAmounts = topLicAmounts
	out.TopLicUrls = topLicUrls
	out.TopLicObjects = topLicObjects

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(out)
}

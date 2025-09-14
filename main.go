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
	"time"

	"flag"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

//go:embed webstatic/*
var webFS embed.FS

var pdfPath, concello string

type server struct {
	db      *sql.DB
	tpl     *template.Template
	perPage int
}

//go:embed templates/* templates/partials/*
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
			ParseFS(tplFS,
				"templates/*.gohtml",
				"templates/partials/*.gohtml",
			),
	)
	// log.Printf("templates: %s", tpl.DefinedTemplates())

	return &server{db: db, tpl: tpl, perPage: 25}, nil
}

func (s *server) routes(addr string, debug bool) error {
	assets, err := fs.Sub(webFS, "webstatic")
	if err != nil {
		return err
	}

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(assets))))
	http.HandleFunc("/", withLogging(debug, s.handleIndex))
	http.HandleFunc("/table/", withLogging(debug, s.handleTable))
	http.HandleFunc("/export/csv", withLogging(debug, s.handleExportCSV))
	http.HandleFunc("/export/xlsx", withLogging(debug, s.handleExportXLSX))
	http.HandleFunc("/api/table/", withLogging(debug, s.handleAPITable)) // ← API JSON para Instant Search

	http.Handle("/pdfs/", http.StripPrefix("/pdfs/", http.FileServer(http.Dir(pdfPath))))

	http.HandleFunc("/summary", withLogging(debug, s.handleSummary))
	http.HandleFunc("/api/summary", withLogging(debug, s.handleAPISummary))

	http.HandleFunc("/summary_all", withLogging(debug, s.handleSummaryAll))
	http.HandleFunc("/api/summary_all", withLogging(debug, s.handleAPISummaryAll))

	log.Printf("Web UI en http://%s", addr)
	log.Printf("PDFs en %s", http.Dir(pdfPath))

	return http.ListenAndServe(addr, nil)
}

// middleware para empregar de debug nos handlers
func withLogging(debug bool, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if debug {
			start := time.Now()
			log.Printf("→ %s %s %s", r.Method, r.URL.Path, r.URL.RawQuery)
			defer log.Printf("← %s %s (%s)", r.Method, r.URL.Path, time.Since(start))
		}
		h(w, r)
	}
}

// --

// ==== main ====
func main() {
	// o dbPath tamen indica onde estaran os ficheiros PDF, entendendo que ao utilizar o scrapper
	//  https://github.com/alexandregz/plataforma_contratacion_estado_scrapper van ter esa estructura:
	// 	PDF/CONCELHO/TABOA/EXPEDIENTE/
	dbPath := flag.String("db", "", "ruta ao ficheiro SQLite")
	mode := flag.String("mode", "web", "web|tui")
	addr := flag.String("addr", "127.0.0.1:8080", "enderezo para o modo web")

	debug := flag.Bool("debug", false, "enable debug logging")

	flag.Parse()

	if *dbPath == "" {
		log.Fatal("Debe especificar a ruta ao ficheiro SQLite con --db")
	}

	db, err := openSQLite(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// path fisico a PDFs. Hai que reemprazar "TABOA/EXPEDIENTE/" polo que toque "on the fly"
	pdfPath = filepath.Dir(*dbPath) + "/PDF" + stripExt(strings.TrimPrefix(*dbPath, filepath.Dir(*dbPath)))
	concello = strings.Replace(stripExt(strings.TrimPrefix(*dbPath, filepath.Dir(*dbPath))), "/", "", 1)

	caser := cases.Title(language.EuropeanSpanish) // nh...
	concello = caser.String(concello)

	log.Printf("concello: %s", concello)
	// log.Printf("pdfPath: %s", pdfPath)

	switch *mode {
	case "web":
		srv, err := newServer(db)
		if err != nil {
			log.Fatal(err)
		}
		if err := srv.routes(*addr, *debug); err != nil {
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

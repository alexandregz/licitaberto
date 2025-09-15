package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/xuri/excelize/v2"
)

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
	where, args := buildWhereLike(ColNames(cols), m.q)
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
	where, args := buildWhereLike(ColNames(m.cols), m.q)
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
	where, args := buildWhereLike(ColNames(cols), m.q)
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
	where, args := buildWhereLike(ColNames(cols), m.q)
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

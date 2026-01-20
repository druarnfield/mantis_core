#set page(
  paper: "a4",
  flipped: true,
  margin: (x: 1cm, y: 1.2cm),
  header: context {
    if counter(page).get().first() > 1 [
      #grid(
        columns: (1fr, auto),
        align: (left, right),
        [#text(size: 10pt, weight: "bold", fill: rgb("#2c3e50"))[Rolling 12-Month Performance Dashboard]],
        [#text(size: 8pt, fill: rgb("#666"))[Page #counter(page).display() of #counter(page).final().first()]]
      )
      #v(-4pt)
      #line(length: 100%, stroke: 0.5pt + rgb("#dee2e6"))
    ]
  },
  footer: context {
    align(center)[
      #text(size: 6pt, fill: rgb("#666"))[
        *Legend:* #h(4pt)
        #text(fill: rgb("#27ae60"))[▲] Favourable #h(8pt)
        #text(fill: rgb("#e74c3c"))[▼] Unfavourable #h(8pt)
        #text(fill: rgb("#95a5a6"))[●] On Target
        #h(20pt) | #h(20pt)
        YTD = Year to Date (Avg) #h(6pt) | #h(6pt) YOY = Year on Year #h(6pt) | #h(6pt) FY: Jul–Jun
      ]
    ]
  }
)

#set text(font: "Arial", size: 7pt)

// === CONFIGURATION ===
#let fy_start_month = 7

// === COLOR DEFINITIONS ===
#let header_bg = rgb("#1e293b")
#let header_text = white
#let subheader_bg = rgb("#334155")
#let row_alt = rgb("#f8f9fa")
#let positive = rgb("#27ae60")
#let negative = rgb("#e74c3c")
#let neutral = rgb("#95a5a6")

// === LOAD DATA ===
#let raw_data = csv("measures_data.csv")
#let headers = raw_data.at(0)
#let data_rows = raw_data.slice(1)

// Month labels for display
#let month_labels = ("Feb 24", "Mar 24", "Apr 24", "May 24", "Jun 24", "Jul 24", "Aug 24", "Sep 24", "Oct 24", "Nov 24", "Dec 24", "Jan 25")

// Column indices
#let col_target = 2
#let col_direction = 1
#let cols_display = range(15, 27)
#let col_current = 26
#let col_yoy_prior = 14
#let cols_ytd_current = range(20, 27)
#let cols_ytd_prior = range(8, 15)

// === HELPER FUNCTIONS ===
#let parse_num(s) = {
  let cleaned = str(s).trim()
  if cleaned == "" or cleaned == "-" { 0.0 }
  else { float(cleaned) }
}

#let format_num(n, decimals: 1) = {
  let rounded = calc.round(n, digits: decimals)
  if decimals == 0 { str(int(rounded)) }
  else { str(rounded) }
}

#let format_large(n) = {
  let val = int(calc.round(n, digits: 0))
  if val >= 1000000 { str(calc.round(val / 1000000, digits: 1)) + "M" }
  else if val >= 10000 { str(calc.round(val / 1000, digits: 0)) + "k" }
  else if val >= 1000 { str(calc.round(val / 1000, digits: 1)) + "k" }
  else { str(val) }
}

#let smart_format(n, target) = {
  if calc.abs(n) >= 10000 or calc.abs(target) >= 10000 { format_large(n) }
  else if calc.abs(n) < 10 { format_num(n, decimals: 1) }
  else { format_num(n, decimals: 0) }
}

#let calc_avg(row, cols) = {
  let sum = cols.map(c => parse_num(row.at(c))).sum()
  sum / cols.len()
}

#let var_indicator(actual, target, direction) = {
  let dir = parse_num(direction)
  let diff = actual - target
  let is_good = (dir > 0 and diff > 0) or (dir < 0 and diff < 0)
  let is_bad = (dir > 0 and diff < 0) or (dir < 0 and diff > 0)

  if calc.abs(diff) < 0.01 { text(fill: neutral, weight: "bold")[●] }
  else if is_good { text(fill: positive, weight: "bold")[▲] }
  else { text(fill: negative, weight: "bold")[▼] }
}

#let pct_change(current, prior) = {
  if prior == 0 { 0 }
  else { ((current - prior) / calc.abs(prior)) * 100 }
}

// === TITLE BLOCK WITH LOGO ===
#grid(
  columns: (1fr, auto),
  align: (left + horizon, right + horizon),
  gutter: 12pt,
  [
    #text(size: 16pt, weight: "bold", fill: rgb("#2c3e50"))[Rolling 12-Month Performance Dashboard]
    #v(2pt)
    #text(size: 9pt, fill: rgb("#666"))[
      Reporting Period: February 2024 – January 2025
      #h(1em) | #h(1em)
      FY YTD: July 2024 – January 2025
      #h(1em) | #h(1em)
      Generated: #datetime.today().display()
    ]
  ],
  [
    #image("hospital_logo.svg", width: 140pt)
  ]
)

#v(8pt)

// === BUILD TABLE ===
#table(
  columns: (
    130pt,
    ..range(12).map(_ => 1fr),
    30pt, 30pt, 18pt,
    30pt, 30pt, 18pt,
    30pt, 18pt,
  ),
  stroke: 0.5pt + rgb("#dee2e6"),
  align: center + horizon,
  inset: 4pt,

  // Repeating header
  table.header(
    // Row 1
    table.cell(rowspan: 2, fill: header_bg)[#text(fill: header_text, weight: "bold")[Measure]],
    table.cell(colspan: 12, fill: header_bg)[#text(fill: header_text, weight: "bold")[Monthly Performance]],
    table.cell(colspan: 3, fill: header_bg)[#text(fill: header_text, weight: "bold")[Current]],
    table.cell(colspan: 3, fill: header_bg)[#text(fill: header_text, weight: "bold")[Year to Date]],
    table.cell(colspan: 2, fill: header_bg)[#text(fill: header_text, weight: "bold")[YOY]],

    // Row 2
    ..month_labels.map(m => table.cell(fill: subheader_bg)[#text(fill: header_text, size: 6pt)[#m]]),
    table.cell(fill: subheader_bg)[#text(fill: header_text, size: 6pt)[Result]],
    table.cell(fill: subheader_bg)[#text(fill: header_text, size: 6pt)[Target]],
    table.cell(fill: subheader_bg)[#text(fill: header_text, size: 6pt)[Var]],
    table.cell(fill: subheader_bg)[#text(fill: header_text, size: 6pt)[Avg]],
    table.cell(fill: subheader_bg)[#text(fill: header_text, size: 6pt)[Target]],
    table.cell(fill: subheader_bg)[#text(fill: header_text, size: 6pt)[Var]],
    table.cell(fill: subheader_bg)[#text(fill: header_text, size: 6pt)[Chg%]],
    table.cell(fill: subheader_bg)[#text(fill: header_text, size: 6pt)[Var]],
  ),

  // Data rows
  ..data_rows.enumerate().map(((i, row)) => {
    let bg = if calc.rem(i, 2) == 0 { white } else { row_alt }
    let measure = row.at(0)
    let direction = row.at(col_direction)
    let target = parse_num(row.at(col_target))
    let current = parse_num(row.at(col_current))
    let prior_month = parse_num(row.at(col_yoy_prior))
    let ytd_avg = calc_avg(row, cols_ytd_current)
    let ytd_prior_avg = calc_avg(row, cols_ytd_prior)
    let yoy_pct = pct_change(current, prior_month)

    (
      table.cell(fill: bg, align: left)[#text(weight: "medium")[#measure]],

      ..cols_display.map(c => {
        let val = parse_num(row.at(c))
        table.cell(fill: bg)[#smart_format(val, target)]
      }),

      table.cell(fill: bg)[#text(weight: "bold")[#smart_format(current, target)]],
      table.cell(fill: bg)[#smart_format(target, target)],
      table.cell(fill: bg)[#var_indicator(current, target, direction)],

      table.cell(fill: bg)[#smart_format(ytd_avg, target)],
      table.cell(fill: bg)[#smart_format(target, target)],
      table.cell(fill: bg)[#var_indicator(ytd_avg, target, direction)],

      table.cell(fill: bg)[
        #if yoy_pct >= 0 [+#format_num(yoy_pct, decimals: 1)%]
        #if yoy_pct < 0 [#format_num(yoy_pct, decimals: 1)%]
      ],
      table.cell(fill: bg)[#var_indicator(current, prior_month, direction)],
    )
  }).flatten(),
)

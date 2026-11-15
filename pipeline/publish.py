"""SERVE: exporta los marts a un sitio estático (dashboard + datos + docs de dbt).

Genera:
    site/index.html              dashboard interactivo (Plotly.js)
    site/data/*.csv | *.parquet  data products descargables
    site/docs/index.html         documentación y linaje de dbt (si existe target/static_index.html)
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(os.getenv("DUCKDB_PATH", ROOT / "warehouse" / "weather.duckdb"))
SITE = ROOT / "site"
REPO_URL = os.getenv("REPO_URL", "https://github.com/VictorPlatero/Data-Enginnering")

EXPORTS = ["mart_city_kpis", "mart_city_monthly", "mart_extreme_events", "fct_weather_daily", "dim_city"]


def q(con, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, (v.isoformat() if hasattr(v, "isoformat") else v for v in row))) for row in cur.fetchall()]


def build(db_path: Path = DB_PATH, site: Path = SITE) -> dict:
    if site.exists():
        shutil.rmtree(site)
    (site / "data").mkdir(parents=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        for t in EXPORTS:
            con.execute(f"COPY marts.{t} TO '{(site / 'data' / f'{t}.csv').as_posix()}' (HEADER)").fetchall()
            con.execute(f"COPY marts.{t} TO '{(site / 'data' / f'{t}.parquet').as_posix()}' (FORMAT parquet)").fetchall()

        data = {
            "kpis": q(con, "SELECT * FROM marts.mart_city_kpis ORDER BY city"),
            "monthly": q(con, """SELECT city, natural_region, year_month, avg_temp_c, total_precip_mm,
                                        rainy_days, hot_days, frost_days
                                 FROM marts.mart_city_monthly ORDER BY city, year_month"""),
            "events": q(con, """SELECT event_type, natural_region, city, count(*) AS n
                                FROM marts.mart_extreme_events GROUP BY ALL ORDER BY n DESC"""),
            "recent_events": q(con, """SELECT * FROM marts.mart_extreme_events
                                       ORDER BY weather_date DESC LIMIT 25"""),
            "runs": q(con, """SELECT run_id, min(started_at) AS started_at, sum(rows_loaded) AS rows_loaded,
                                     count(*) FILTER (WHERE status='success') AS ok,
                                     count(*) FILTER (WHERE status='failed') AS failed
                              FROM raw.ingestion_log GROUP BY run_id ORDER BY started_at DESC LIMIT 10"""),
            "stats": q(con, """SELECT (SELECT count(*) FROM raw.weather_daily) AS raw_rows,
                                      (SELECT count(*) FROM marts.fct_weather_daily) AS fact_rows,
                                      (SELECT count(*) FROM marts.dim_city) AS cities,
                                      (SELECT min(weather_date) FROM marts.fct_weather_daily) AS first_date,
                                      (SELECT max(weather_date) FROM marts.fct_weather_daily) AS last_date""")[0],
        }
    finally:
        con.close()

    data["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = TEMPLATE.replace("__DATA__", json.dumps(data, ensure_ascii=False, default=str)).replace("__REPO__", REPO_URL)
    (site / "index.html").write_text(html, encoding="utf-8")

    docs = ROOT / "target" / "static_index.html"
    if docs.exists():
        (site / "docs").mkdir()
        shutil.copy2(docs, site / "docs" / "index.html")
    (site / ".nojekyll").write_text("")
    return data["stats"]


TEMPLATE = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Clima Perú · Data Engineering con dbt + DuckDB</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  :root { --bg:#0f172a; --card:#1e293b; --txt:#e2e8f0; --mut:#94a3b8; --acc:#f59e0b; }
  * { box-sizing:border-box } body { margin:0; font-family:system-ui,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--txt) }
  header { padding:24px 32px; border-bottom:1px solid #334155 } h1 { margin:0 0 6px; font-size:26px }
  header p { margin:0; color:var(--mut) } a { color:var(--acc) }
  main { padding:24px 32px; display:grid; gap:20px }
  .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:14px }
  .kpi, .card { background:var(--card); border-radius:12px; padding:16px }
  .kpi b { display:block; font-size:24px; color:var(--acc) } .kpi span { color:var(--mut); font-size:13px }
  .grid2 { display:grid; grid-template-columns:repeat(auto-fit,minmax(460px,1fr)); gap:20px }
  h2 { margin:0 0 10px; font-size:17px } select { background:#0b1220; color:var(--txt); border:1px solid #334155; padding:6px; border-radius:6px }
  table { width:100%; border-collapse:collapse; font-size:13px } th, td { padding:6px 8px; border-bottom:1px solid #334155; text-align:left }
  th { color:var(--mut) } .pipe { display:flex; flex-wrap:wrap; gap:8px; align-items:center; font-size:13px }
  .pipe span { background:#0b1220; border:1px solid #334155; padding:6px 10px; border-radius:20px }
  footer { padding:20px 32px; color:var(--mut); font-size:13px }
</style>
</head>
<body>
<header>
  <h1>🌦️ Clima del Perú · Pipeline de Data Engineering</h1>
  <p>Open-Meteo API → Python (extract/load incremental) → DuckDB → dbt (staging · marts · tests) → GitHub Pages ·
     actualizado automáticamente por GitHub Actions · <span id="gen"></span></p>
  <p style="margin-top:8px"><a href="docs/">📚 Documentación y linaje dbt</a> · <a href="data/">⬇️ Datos</a>
     (<a href="data/fct_weather_daily.csv">CSV</a> / <a href="data/fct_weather_daily.parquet">Parquet</a>) ·
     <a href="__REPO__">💻 Repositorio</a></p>
</header>
<main>
  <div class="kpis" id="kpis"></div>
  <div class="card"><h2>Pipeline</h2><div class="pipe">
    <span>⏰ cron diario (GitHub Actions)</span>→<span>🐍 extract.py · Open-Meteo</span>→<span>🦆 raw.weather_daily</span>→
    <span>🔧 dbt staging</span>→<span>⭐ dbt marts (incremental)</span>→<span>✅ dbt tests</span>→<span>🌐 GitHub Pages</span></div></div>
  <div class="card"><h2>Temperatura media mensual por ciudad (°C)</h2><div id="temp" style="height:420px"></div></div>
  <div class="grid2">
    <div class="card"><h2>Precipitación mensual (mm) · <select id="city"></select></h2><div id="precip" style="height:360px"></div></div>
    <div class="card"><h2>Anomalía térmica últimos 30 días vs. histórico (°C)</h2><div id="anom" style="height:360px"></div></div>
  </div>
  <div class="grid2">
    <div class="card"><h2>Eventos extremos por tipo y región</h2><div id="events" style="height:380px"></div></div>
    <div class="card"><h2>Mapa · temperatura media últimos 30 días</h2><div id="map" style="height:380px"></div></div>
  </div>
  <div class="grid2">
    <div class="card"><h2>Eventos extremos recientes</h2><table id="recent"></table></div>
    <div class="card"><h2>Últimas ejecuciones de la ingesta</h2><table id="runs"></table></div>
  </div>
</main>
<footer>Actividad Grupal 01-U2 · Data Engineering · Datos: Open-Meteo (CC BY 4.0)</footer>
<script>
const D = __DATA__;
const L = { paper_bgcolor:'rgba(0,0,0,0)', plot_bgcolor:'rgba(0,0,0,0)', font:{color:'#e2e8f0'}, margin:{t:10,r:10,b:40,l:50},
            xaxis:{gridcolor:'#334155'}, yaxis:{gridcolor:'#334155'}, legend:{orientation:'h'} };
const C = { responsive:true, displaylogo:false };
document.getElementById('gen').textContent = 'generado ' + D.generated_at;
const s = D.stats;
const kp = [['Registros raw', s.raw_rows.toLocaleString()], ['Filas en fct_weather_daily', s.fact_rows.toLocaleString()],
            ['Ciudades', s.cities], ['Desde', s.first_date], ['Hasta', s.last_date],
            ['Eventos extremos', D.events.reduce((a,e)=>a+e.n,0).toLocaleString()]];
document.getElementById('kpis').innerHTML = kp.map(([k,v])=>`<div class="kpi"><b>${v}</b><span>${k}</span></div>`).join('');

const cities = [...new Set(D.monthly.map(r=>r.city))];
Plotly.newPlot('temp', cities.map(c=>{ const r=D.monthly.filter(x=>x.city===c);
  return {x:r.map(x=>x.year_month), y:r.map(x=>x.avg_temp_c), name:c, mode:'lines'}; }), L, C);

const sel = document.getElementById('city');
sel.innerHTML = cities.map(c=>`<option>${c}</option>`).join('');
function precip(){ const r=D.monthly.filter(x=>x.city===sel.value);
  Plotly.react('precip', [{x:r.map(x=>x.year_month), y:r.map(x=>x.total_precip_mm), type:'bar', marker:{color:'#38bdf8'}, name:'mm'},
                          {x:r.map(x=>x.year_month), y:r.map(x=>x.rainy_days), name:'días de lluvia', yaxis:'y2', mode:'lines+markers', line:{color:'#f59e0b'}}],
    {...L, yaxis2:{overlaying:'y', side:'right', showgrid:false}}, C); }
sel.onchange = precip; precip();

const k = [...D.kpis].sort((a,b)=>(a.temp_anomaly_c??0)-(b.temp_anomaly_c??0));
Plotly.newPlot('anom', [{x:k.map(r=>r.temp_anomaly_c), y:k.map(r=>r.city), type:'bar', orientation:'h',
  marker:{color:k.map(r=>(r.temp_anomaly_c??0)>=0?'#ef4444':'#3b82f6')}}], {...L, margin:{t:10,r:10,b:40,l:90}}, C);

const types = [...new Set(D.events.map(e=>e.event_type))], regions = ['Costa','Sierra','Selva'];
Plotly.newPlot('events', regions.map(rg=>({x:types, name:rg, type:'bar',
  y:types.map(t=>D.events.filter(e=>e.event_type===t && e.natural_region===rg).reduce((a,e)=>a+e.n,0))})), {...L, barmode:'stack'}, C);

Plotly.newPlot('map', [{type:'scattergeo', lat:D.kpis.map(r=>r.latitude), lon:D.kpis.map(r=>r.longitude),
  text:D.kpis.map(r=>`${r.city}: ${r.avg_temp_30d} °C · ${r.precip_30d} mm`), mode:'markers+text', textposition:'top center',
  marker:{size:14, color:D.kpis.map(r=>r.avg_temp_30d), colorscale:'RdYlBu', reversescale:true, showscale:true}}],
  {...L, margin:{t:0,r:0,b:0,l:0}, geo:{scope:'south america', center:{lat:-9.5,lon:-75}, projection:{scale:3.2},
   bgcolor:'rgba(0,0,0,0)', showcountries:true, countrycolor:'#64748b', showland:true, landcolor:'#1e293b'}}, C);

const table = (id, rows) => { if(!rows.length) return; const cols=Object.keys(rows[0]);
  document.getElementById(id).innerHTML = '<tr>'+cols.map(c=>`<th>${c}</th>`).join('')+'</tr>' +
    rows.map(r=>'<tr>'+cols.map(c=>`<td>${r[c]??''}</td>`).join('')+'</tr>').join(''); };
table('recent', D.recent_events); table('runs', D.runs);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    print(build())

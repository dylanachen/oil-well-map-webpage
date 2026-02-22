const API_BASE = "http://127.0.0.1:5050";

const container = document.getElementById("popup");
const content = document.getElementById("popup-content");
const closer = document.getElementById("popup-closer");

const overlay = new ol.Overlay({
  element: container,
  autoPan: { animation: { duration: 250 } },
});
closer.onclick = function () {
  overlay.setPosition(undefined);
  closer.blur();
  return false;
};

const map = new ol.Map({
  target: "map",
  layers: [new ol.layer.Tile({ source: new ol.source.OSM() })],
  overlays: [overlay],
  view: new ol.View({
    center: ol.proj.fromLonLat([-100.5, 47.5]),
    zoom: 6,
  }),
});

const markerStyle = new ol.style.Style({
  image: new ol.style.Circle({
    radius: 9,
    fill: new ol.style.Fill({ color: "rgba(255, 0, 0, 0.9)" }),
    stroke: new ol.style.Stroke({ color: "#000", width: 2 }),
  }),
});

const source = new ol.source.Vector();
const layer = new ol.layer.Vector({ source });
map.addLayer(layer);

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[c]));
}

function isNA(v) {
  if (v === null || v === undefined) return true;
  const s = String(v).trim();
  if (s.length === 0) return true;
  const u = s.toUpperCase();
  return u === "N/A" || u === "NA" || u === "NULL" || u === "NONE" || u === "--";
}


function kvRow(k, v) {
  if (isNA(v)) return "";
  return `<tr><td class="k">${esc(k)}</td><td>${esc(v)}</td></tr>`;
}


function line(label, v) {
  if (isNA(v)) return "";
  return `<div><b>${esc(label)}:</b> ${esc(v)}</div>`;
}


function topBottomLine(top, bottom) {
  if (isNA(top) && isNA(bottom)) return "";
  return `<div><b>Top-Bottom (ft):</b> ${esc(isNA(top) ? "N/A" : top)} - ${esc(isNA(bottom) ? "N/A" : bottom)}</div>`;
}

function volumeLine(vol, units) {
  if (isNA(vol)) return "";
  const u = isNA(units) ? "" : ` ${esc(units)}`;
  return `<div><b>Volume:</b> ${esc(vol)}${u}</div>`;
}

async function loadWells() {
  const res = await fetch(`${API_BASE}/api/wells`);
  const wells = await res.json();

  for (const w of wells) {
    const lon = Number(w.longitude);
    const lat = Number(w.latitude);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;

    const feat = new ol.Feature({
      geometry: new ol.geom.Point(ol.proj.fromLonLat([lon, lat])),
      well_id: w.id,
      well_name: w.well_name || "",
    });
    feat.setStyle(markerStyle);
    source.addFeature(feat);
  }

  const n = source.getFeatures().length;
  console.log(`Loaded ${n} wells with coordinates.`);


  if (n > 0) {
    map.getView().fit(source.getExtent(), {
      padding: [50, 50, 50, 50],
      maxZoom: 12,
    });
  }
}

map.on("singleclick", async (evt) => {
  const feature = map.forEachFeatureAtPixel(evt.pixel, (f) => f);
  if (!feature) {
    overlay.setPosition(undefined);
    return;
  }

  const wellId = feature.get("well_id");
  const coordinate = evt.coordinate;

  content.innerHTML = `<b>Loading well ${esc(wellId)}...</b>`;
  overlay.setPosition(coordinate);

  const res = await fetch(`${API_BASE}/api/wells/${wellId}`);
  const data = await res.json();

  if (data.error) {
    content.innerHTML = `<b>Error:</b> ${esc(data.error)}`;
    return;
  }

  const well = data.well;
  const stim = data.stimulation || [];

  let tableRows = "";
  tableRows += kvRow("API", well.api_number);
  tableRows += kvRow("Well File No", well.well_file_no);
  tableRows += kvRow("Well Name", well.well_name);
  tableRows += kvRow("County", well.county);
  tableRows += kvRow("State", well.state);
  tableRows += kvRow("Address", well.address);
  tableRows += kvRow("Operator", well.operator);
  tableRows += kvRow("Field", well.field);
  tableRows += kvRow("Permit #", well.permit_number);
  tableRows += kvRow("Permit Date", well.permit_date);
  tableRows += kvRow("Formation", well.formation);
  tableRows += kvRow("Total Depth", well.total_depth);
  tableRows += kvRow("Stimulation Notes", well.stimulation_notes);
  tableRows += kvRow("Ensco Job No", well.enseco_job_no);
  tableRows += kvRow("Job Type", well.job_type);
  tableRows += kvRow("Surface Hole Location", well.surface_hole_location);
  tableRows += kvRow("Datum", well.datum);
  tableRows += kvRow("PDF Source", well.pdf_source);
  tableRows += kvRow("Created At", well.created_at);

  let html = `<div><b>${esc(well.well_name || "Well")}</b> (ID ${esc(well.id)})</div>`;
  html += `<table class="kv">${tableRows}</table>`;

  html += `<div class="section">Stimulation records (${stim.length})</div>`;

  if (stim.length === 0) {
    html += `<div>N/A</div>`;
  } else {
    for (const s of stim) {
      const block =
        line("Date", s.date_stimulated) +
        line("Formation", s.stimulated_formation) +
        topBottomLine(s.top_ft, s.bottom_ft) +
        line("Stages", s.stimulation_stages) +
        volumeLine(s.volume, s.volume_units) +
        line("Treatment Type", s.type_treatment) +
        line("Acid %", s.acid_pct) +
        line("Proppant (lbs)", s.lbs_proppant) +
        line("Max Pressure (psi)", s.max_treatment_pressure_psi) +
        line("Max Rate", s.max_treatment_rate) +
        line("Details", s.details);

      if (block.trim().length === 0) continue;

      html += `<div class="stim">${block}</div>`;
    }
  }

  content.innerHTML = html;
});

loadWells().catch(console.error);
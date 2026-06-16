const themeToggleEl = document.getElementById('themeToggle');
const loadErrorEl = document.getElementById('loadError');
const contentEl = document.getElementById('content');

const THEME_KEY = 'websql_theme';

function isDark() {
  return document.documentElement.getAttribute('data-theme') === 'dark';
}
function applyTheme(dark) {
  if (dark) document.documentElement.setAttribute('data-theme', 'dark');
  else document.documentElement.removeAttribute('data-theme');
  themeToggleEl.textContent = dark ? '☀️' : '🌙';
}
themeToggleEl.addEventListener('click', () => {
  const dark = !isDark();
  try { localStorage.setItem(THEME_KEY, dark ? 'dark' : 'light'); } catch (_e) {}
  applyTheme(dark);
});
applyTheme(isDark());

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function rateClass(rate) {
  if (rate < 40) return 'low';
  if (rate < 75) return 'mid';
  return 'high';
}

function fmtDateTime(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return escapeHtml(ts);
  return d.toLocaleString('es-AR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function fmtDate(ts) {
  if (!ts) return '—';
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? ts : d.toLocaleDateString('es-AR', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

function renderCards(t) {
  const cards = [
    { num: t.queries, lbl: 'Consultas ejecutadas' },
    { num: t.exerciseChecks, lbl: 'Verificaciones de ejercicios' },
    { num: t.uniqueUsers, lbl: 'Usuarios (por IP)' },
    { num: t.errors, lbl: 'Consultas con error' },
    { num: `${fmtDate(t.firstTs)} → ${fmtDate(t.lastTs)}`, lbl: 'Período', small: true }
  ];
  document.getElementById('cards').innerHTML = cards
    .map((c) => `<div class="stat-card"><div class="num"${c.small ? ' style="font-size:1rem;"' : ''}>${escapeHtml(c.num)}</div><div class="lbl">${c.lbl}</div></div>`)
    .join('');
}

function renderExercises(list) {
  const body = document.querySelector('#exTable tbody');
  if (!list.length) {
    body.innerHTML = '<tr><td colspan="5" class="muted">Sin verificaciones registradas.</td></tr>';
    return;
  }
  body.innerHTML = list
    .map((e) => `
      <tr>
        <td>Ejercicio ${escapeHtml(e.exerciseId)}</td>
        <td class="num">${e.attempts}</td>
        <td class="num">${e.correct}</td>
        <td class="num">${e.students}</td>
        <td><span class="rate ${rateClass(e.successRate)}">${e.successRate}%</span></td>
      </tr>`)
    .join('');
}

function renderDays(list) {
  const body = document.querySelector('#dayTable tbody');
  if (!list.length) {
    body.innerHTML = '<tr><td colspan="4" class="muted">Sin datos.</td></tr>';
    return;
  }
  body.innerHTML = list
    .slice(-14)
    .map((d) => `<tr><td>${escapeHtml(d.day)}</td><td class="num">${d.queries}</td><td class="num">${d.checks}</td><td class="num">${d.correct}</td></tr>`)
    .join('');
}

function renderCounts(tableId, obj) {
  const body = document.querySelector(`#${tableId} tbody`);
  const entries = Object.entries(obj).sort((a, b) => b[1] - a[1]);
  const max = entries.reduce((m, [, n]) => Math.max(m, n), 0) || 1;
  if (!entries.length) {
    body.innerHTML = '<tr><td class="muted">Sin datos.</td></tr>';
    return;
  }
  body.innerHTML = entries
    .map(([k, n]) => `
      <tr>
        <td style="width:35%;">${escapeHtml(k)}</td>
        <td><div class="bar"><span style="width:${Math.round((n / max) * 100)}%;"></span></div></td>
        <td class="num" style="width:48px;">${n}</td>
      </tr>`)
    .join('');
}

function renderErrors(list) {
  const body = document.querySelector('#errTable tbody');
  if (!list.length) {
    body.innerHTML = '<tr><td colspan="2" class="muted">Sin errores registrados.</td></tr>';
    return;
  }
  body.innerHTML = list
    .map((e) => `<tr><td class="num">${e.count}</td><td class="err-msg">${escapeHtml(e.message)}</td></tr>`)
    .join('');
}

function renderRecent(list) {
  const body = document.querySelector('#recentTable tbody');
  if (!list.length) {
    body.innerHTML = '<tr><td colspan="6" class="muted">Sin actividad.</td></tr>';
    return;
  }
  body.innerHTML = list
    .map((r) => {
      let resultado = '';
      if (r.exerciseId !== undefined && r.exerciseId !== null) {
        resultado = r.correcto ? '<span class="pill-ok">✔ correcto</span>' : '<span class="pill-fail">✘ incorrecto</span>';
      } else if (!r.success) {
        resultado = '<span class="pill-fail">error</span>';
      } else {
        resultado = '<span class="muted">—</span>';
      }
      const ej = (r.exerciseId !== undefined && r.exerciseId !== null) ? `Ej ${escapeHtml(r.exerciseId)}` : '<span class="muted">—</span>';
      return `<tr>
        <td>${fmtDateTime(r.timestamp)}</td>
        <td>${escapeHtml(r.ip)}</td>
        <td>${escapeHtml(r.platform)}</td>
        <td>${escapeHtml(r.database)}</td>
        <td>${ej}</td>
        <td>${resultado}</td>
      </tr>`;
    })
    .join('');
}

async function load() {
  const key = new URLSearchParams(location.search).get('key');
  const url = key ? `/api/stats?key=${encodeURIComponent(key)}` : '/api/stats';
  try {
    const res = await fetch(url);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'No se pudieron cargar las estadísticas.');

    renderCards(data.totals);
    renderExercises(data.exercises);
    renderDays(data.byDay);
    renderCounts('platTable', data.byPlatform);
    renderCounts('dbTable', data.byDatabase);
    renderErrors(data.topErrors);
    renderRecent(data.recent);

    contentEl.classList.remove('hidden');
  } catch (error) {
    loadErrorEl.textContent = error.message
      + (error.message.includes('clave') || error.message.includes('Clave') ? ' Agregá ?key=... en la URL.' : '');
    loadErrorEl.classList.remove('hidden');
  }
}

load();

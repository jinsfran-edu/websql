'use strict';

// Agrega las líneas JSONL de query-stats en las métricas del panel docente.
function buildStats(lines) {
  const totals = { queries: 0, exerciseChecks: 0, errors: 0 };
  const users = new Set();
  const byPlatform = {};
  const byDatabase = {};
  const byDay = new Map();
  const exercises = new Map(); // id -> { attempts, correct, students:Set }
  const errors = new Map(); // message -> count
  const recent = [];
  let firstTs = null;
  let lastTs = null;

  for (const line of lines) {
    let entry;
    try {
      entry = JSON.parse(line);
    } catch (_e) {
      continue;
    }

    totals.queries += 1;
    if (entry.ip) users.add(entry.ip);
    if (entry.platform) byPlatform[entry.platform] = (byPlatform[entry.platform] || 0) + 1;
    if (entry.database) byDatabase[entry.database] = (byDatabase[entry.database] || 0) + 1;
    if (!entry.success) totals.errors += 1;

    const ts = entry.timestamp || '';
    if (ts) {
      if (!firstTs || ts < firstTs) firstTs = ts;
      if (!lastTs || ts > lastTs) lastTs = ts;
      const day = ts.slice(0, 10);
      if (!byDay.has(day)) byDay.set(day, { day, queries: 0, checks: 0, correct: 0 });
      byDay.get(day).queries += 1;
    }

    if (entry.exerciseId !== undefined && entry.exerciseId !== null) {
      totals.exerciseChecks += 1;
      const id = String(entry.exerciseId);
      if (!exercises.has(id)) exercises.set(id, { exerciseId: entry.exerciseId, attempts: 0, correct: 0, students: new Set() });
      const ex = exercises.get(id);
      ex.attempts += 1;
      if (entry.correcto) ex.correct += 1;
      if (entry.ip) ex.students.add(entry.ip);
      if (ts) {
        const d = byDay.get(ts.slice(0, 10));
        d.checks += 1;
        if (entry.correcto) d.correct += 1;
      }
    }

    if (!entry.success && entry.error) {
      const msg = String(entry.error).slice(0, 160);
      errors.set(msg, (errors.get(msg) || 0) + 1);
    }

    recent.push({
      timestamp: ts,
      ip: entry.ip || '',
      platform: entry.platform || '',
      database: entry.database || '',
      exerciseId: entry.exerciseId,
      correcto: entry.correcto,
      success: entry.success !== false
    });
  }

  const exerciseList = Array.from(exercises.values())
    .map((e) => ({
      exerciseId: e.exerciseId,
      attempts: e.attempts,
      correct: e.correct,
      students: e.students.size,
      successRate: e.attempts ? Math.round((e.correct / e.attempts) * 100) : 0
    }))
    .sort((a, b) => a.successRate - b.successRate || b.attempts - a.attempts);

  const topErrors = Array.from(errors.entries())
    .map(([message, count]) => ({ message, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  return {
    totals: { ...totals, uniqueUsers: users.size, firstTs, lastTs },
    byPlatform,
    byDatabase,
    byDay: Array.from(byDay.values()).sort((a, b) => a.day.localeCompare(b.day)),
    exercises: exerciseList,
    topErrors,
    recent: recent.slice(-25).reverse()
  };
}

module.exports = { buildStats };

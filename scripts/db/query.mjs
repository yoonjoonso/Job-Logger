/**
 * query.mjs — Query interface for the job-log SQLite DB
 *
 * Usage:
 *   node query.mjs gaps       [--archetype <name>] [--type hard|closeable|soft|strength]
 *   node query.mjs training   [--limit 10]
 *   node query.mjs project    [--limit 10]
 *   node query.mjs stories    [--archetype <name>] [--limit 5]
 *   node query.mjs bullets    [--archetype <name>] [--limit 8]
 *   node query.mjs tracker    [--status <status>] [--company <name>] [--limit 20]
 *   node query.mjs profile
 *   node query.mjs archetypes
 *
 * Add --human for readable table output instead of JSON.
 */

import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join, resolve } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname  = dirname(__filename);
const ROOT = resolve(__dirname, '..', '..');
const DB_PATH    = join(ROOT, 'data', 'job-log.db');

// ──────────────────────────────────────────────────────────
// Arg parsing
// ──────────────────────────────────────────────────────────
const rawArgs = process.argv.slice(2);
const command = rawArgs[0];

if (!command || command === '--help' || command === '-h') {
  console.log(`Usage: node query.mjs <command> [options]

Commands:
  gaps       [--archetype <name>] [--type hard|closeable|soft|strength]
  training   [--limit N]
  project    [--limit N]
  stories    [--archetype <name>] [--limit N]
  bullets    [--archetype <name>] [--limit N]
  tracker    [--status <status>] [--company <name>] [--limit N]
  requirements [--company <name>] [--role <name>] [--name <term>] [--kind skill|capability|unknown] [--priority required|preferred|bonus|unknown]
  profile
  archetypes

Options:
  --human    Human-readable table output instead of JSON`);
  process.exit(0);
}

const flags = {};
for (let i = 1; i < rawArgs.length; i++) {
  const arg = rawArgs[i];
  if (arg.startsWith('--')) {
    const key = arg.slice(2);
    const next = rawArgs[i + 1];
    if (next && !next.startsWith('--')) {
      flags[key] = next;
      i++;
    } else {
      flags[key] = true;
    }
  }
}

const humanMode = flags['human'] === true;
const limit     = parseInt(flags['limit'] || '0', 10) || null;

// ──────────────────────────────────────────────────────────
// Open DB
// ──────────────────────────────────────────────────────────
const db = new Database(DB_PATH, { readonly: true });

// ──────────────────────────────────────────────────────────
// Output helpers
// ──────────────────────────────────────────────────────────
function output(rows) {
  if (humanMode) {
    if (!rows || rows.length === 0) {
      console.log('(no results)');
    } else {
      console.table(rows);
    }
  } else {
    console.log(JSON.stringify(rows, null, 2));
  }
}

// ──────────────────────────────────────────────────────────
// Type alias normalization for --type flag
// ──────────────────────────────────────────────────────────
const TYPE_MAP = {
  hard:      'hard_gap',
  closeable: 'closeable',
  soft:      'soft_gap',
  strength:  'strength',
  hard_gap:  'hard_gap',
  soft_gap:  'soft_gap',
};

// ──────────────────────────────────────────────────────────
// Commands
// ──────────────────────────────────────────────────────────

function cmdGaps() {
  let sql = 'SELECT * FROM gap_analysis WHERE 1=1';
  const params = [];

  if (flags['archetype']) {
    sql += ' AND archetype LIKE ?';
    params.push(`%${flags['archetype']}%`);
  }
  if (flags['type']) {
    const mapped = TYPE_MAP[flags['type'].toLowerCase()];
    if (mapped) {
      sql += ' AND signal = ?';
      params.push(mapped);
    } else {
      console.error(`Unknown --type "${flags['type']}". Use: hard | closeable | soft | strength`);
      process.exit(1);
    }
  }
  if (limit) {
    sql += ` LIMIT ${limit}`;
  }

  const rows = db.prepare(sql).all(...params);
  output(rows);
}

function cmdTraining() {
  const lim = limit || 10;
  const sql = `
    SELECT
      skill_name,
      skill_normalized,
      SUM(count) AS demand,
      my_level
    FROM gap_analysis
    WHERE signal IN ('hard_gap','closeable')
    GROUP BY skill_normalized
    ORDER BY demand DESC
    LIMIT ?
  `;
  output(db.prepare(sql).all(lim));
}

function cmdProject() {
  const lim = limit || 10;
  const sql = `
    SELECT
      skill_name,
      skill_normalized,
      SUM(count) AS demand,
      my_level
    FROM gap_analysis
    WHERE signal = 'closeable'
    GROUP BY skill_normalized
    ORDER BY demand DESC
    LIMIT ?
  `;
  output(db.prepare(sql).all(lim));
}

function cmdStories() {
  const lim = limit || 5;
  let sql;
  const params = [];

  if (flags['archetype']) {
    sql = `
      SELECT s.*, GROUP_CONCAT(a.name) AS archetypes
      FROM stories s
      JOIN story_archetypes sa ON sa.story_id = s.id
      JOIN archetypes a        ON a.id = sa.archetype_id
      WHERE a.name LIKE ?
      GROUP BY s.id
      ORDER BY MAX(sa.relevance) DESC
      LIMIT ?
    `;
    params.push(`%${flags['archetype']}%`, lim);
  } else {
    sql = `
      SELECT s.*, GROUP_CONCAT(a.name) AS archetypes
      FROM stories s
      LEFT JOIN story_archetypes sa ON sa.story_id = s.id
      LEFT JOIN archetypes a        ON a.id = sa.archetype_id
      GROUP BY s.id
      LIMIT ?
    `;
    params.push(lim);
  }

  output(db.prepare(sql).all(...params));
}

function cmdBullets() {
  const lim = limit || 8;
  let sql;
  const params = [];

  if (flags['archetype']) {
    sql = `
      SELECT b.*, e.company, e.title AS job_title
      FROM bullets b
      JOIN bullet_archetypes ba ON ba.bullet_id = b.id
      JOIN archetypes a         ON a.id = ba.archetype_id
      JOIN experience e         ON e.id = b.exp_id
      WHERE a.name LIKE ?
      ORDER BY ba.relevance DESC, b.strength DESC
      LIMIT ?
    `;
    params.push(`%${flags['archetype']}%`, lim);
  } else {
    sql = `
      SELECT b.*, e.company, e.title AS job_title
      FROM bullets b
      JOIN experience e ON e.id = b.exp_id
      ORDER BY b.strength DESC
      LIMIT ?
    `;
    params.push(lim);
  }

  output(db.prepare(sql).all(...params));
}

function cmdTracker() {
  const lim = limit || 20;
  let sql = 'SELECT * FROM roles WHERE 1=1';
  const params = [];

  if (flags['status']) {
    sql += ' AND status LIKE ?';
    params.push(`%${flags['status']}%`);
  }
  if (flags['company']) {
    sql += ' AND company LIKE ?';
    params.push(`%${flags['company']}%`);
  }
  sql += ' ORDER BY num DESC LIMIT ?';
  params.push(lim);

  const rows = db.prepare(sql).all(...params);

  if (!humanMode) {
    output(rows);
    return;
  }

  // Human-readable: one line per role + score breakdown if available
  const WEIGHTS = { cv_match: 0.50, role_fit: 0.28, comp: 0.13, work_pref: 0.09 };
  for (const r of rows) {
    const num = String(r.num).padStart(3, '0');
    const score = r.score != null ? `${r.score}/10` : '—';
    console.log(`#${num} ${r.company} — ${r.role} [${r.status}] ${score}`);
    if (r.first_seen_date || r.last_updated_date) {
      console.log(`  first-seen: ${r.first_seen_date || '—'}  |  last-updated: ${r.last_updated_date || '—'}`);
    }

    const hasDims = [r.cv_match, r.role_fit, r.comp, r.work_pref].some(v => v != null);
    if (hasDims) {
      const parts = Object.entries(WEIGHTS)
        .filter(([k]) => r[k] != null)
        .map(([k, w]) => `${k.replace('_', '-')}: ${Number(r[k]).toFixed(1)} ×${Math.round(w * 100)}%`);
      const excluded = Object.keys(WEIGHTS).filter(k => r[k] == null);
      let line = `  [${parts.join('  |  ')}`;
      if (r.red_flag_penalty > 0) line += `  |  penalty: -${r.red_flag_penalty}`;
      line += ']';
      if (excluded.length) line += `  (excluded: ${excluded.join(', ')})`;
      console.log(line);
    }

    if (r.notes) console.log(`  ${r.notes}`);
    console.log();
  }
}

function cmdRequirements() {
  const lim = limit || 50;
  let sql = `
    SELECT
      rr.id,
      rr.role_id,
      r.company,
      r.role,
      rr.kind,
      rr.priority,
      rr.requirement_name,
      rr.requirement_normalized,
      rr.matched_entity_type,
      rr.matched_normalized,
      rr.match_method,
      rr.confidence,
      rr.source,
      rr.notes
    FROM role_requirements rr
    JOIN roles r ON r.num = rr.role_id
    WHERE 1=1
  `;
  const params = [];

  if (flags['company']) {
    sql += ' AND r.company LIKE ?';
    params.push(`%${flags['company']}%`);
  }
  if (flags['role']) {
    sql += ' AND r.role LIKE ?';
    params.push(`%${flags['role']}%`);
  }
  if (flags['name']) {
    sql += ' AND (rr.requirement_name LIKE ? OR rr.requirement_normalized LIKE ? OR rr.matched_normalized LIKE ?)';
    params.push(`%${flags['name']}%`, `%${flags['name']}%`, `%${flags['name']}%`);
  }
  if (flags['kind']) {
    sql += ' AND rr.kind = ?';
    params.push(flags['kind']);
  }
  if (flags['priority']) {
    sql += ' AND rr.priority = ?';
    params.push(flags['priority']);
  }

  sql += ' ORDER BY r.num DESC, rr.priority ASC, rr.requirement_name ASC LIMIT ?';
  params.push(lim);
  output(db.prepare(sql).all(...params));
}

function cmdProfile() {
  const rows = db.prepare('SELECT * FROM profile').all();
  if (humanMode) {
    // Print as key: value pairs for readability
    for (const { key, value } of rows) {
      console.log(`${key}: ${value}`);
    }
  } else {
    // Convert to object for cleaner JSON
    const obj = {};
    for (const { key, value } of rows) obj[key] = value;
    console.log(JSON.stringify(obj, null, 2));
  }
}

function cmdArchetypes() {
  const rows = db.prepare('SELECT name, evals FROM archetypes ORDER BY evals DESC').all();
  output(rows);
}

// ──────────────────────────────────────────────────────────
// Dispatch
// ──────────────────────────────────────────────────────────
switch (command) {
  case 'gaps':       cmdGaps();       break;
  case 'training':   cmdTraining();   break;
  case 'project':    cmdProject();    break;
  case 'stories':    cmdStories();    break;
  case 'bullets':    cmdBullets();    break;
  case 'tracker':    cmdTracker();    break;
  case 'requirements': cmdRequirements(); break;
  case 'profile':    cmdProfile();    break;
  case 'archetypes': cmdArchetypes(); break;
  default:
    console.error(`Unknown command: "${command}". Run node query.mjs --help`);
    process.exit(1);
}

db.close();

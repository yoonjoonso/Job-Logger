/**
 * db-init.mjs — Career-Ops SQLite schema init + full seed
 *
 * Usage:
 *   node db-init.mjs           — full init: schema + all seeds
 *   node db-init.mjs roles     — re-parse and upsert roles only
 *   node db-init.mjs signals   — re-parse and upsert skill signals only
 *   node db-init.mjs resume    — create + seed resume-oriented tables only
 *   node db-init.mjs resume-config — seed generic resume config tables only (profile-specific rows come from local sync)
 *   node db-init.mjs resume-layouts — reseed resume layout policy tables only
 *   node db-init.mjs job-config — create/migrate job intake config tables only
 *   node db-init.mjs skills    — migrate + bootstrap skills/capabilities only
 *   node db-init.mjs setup — copy template DB if missing, then apply migrations
 */

import Database from 'better-sqlite3';
import { copyFileSync, existsSync, mkdirSync, readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join, resolve } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT = resolve(__dirname, '..', '..');

const DB_PATH = resolve(process.env.JOB_LOG_DB_PATH || join(ROOT, 'data', 'job-log.db'));
const DB_TEMPLATE_PATH = resolve(process.env.JOB_LOG_TEMPLATE_DB_PATH || join(ROOT, 'data', 'job-log.template.db'));
const SKILL_GAP_PATH = join(ROOT, 'data', 'skill-gap-cache.md');
const APPLICATIONS_PATH = join(ROOT, 'data', 'applications.md');
const EVALUATED_PATH = join(ROOT, 'data', 'evaluated.md');

// ──────────────────────────────────────────────────────────
// Schema
// ──────────────────────────────────────────────────────────
const SCHEMA = `
CREATE TABLE IF NOT EXISTS profile (
  key   TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS experience (
  id         INTEGER PRIMARY KEY,
  company    TEXT,
  title      TEXT,
  start_date TEXT,
  end_date   TEXT,
  location   TEXT,
  UNIQUE(company, title)
);

CREATE TABLE IF NOT EXISTS projects (
  id            INTEGER PRIMARY KEY,
  experience_id INTEGER NOT NULL REFERENCES experience(id) ON DELETE CASCADE,
  slug          TEXT UNIQUE,
  name          TEXT NOT NULL,
  stack         TEXT,
  sort_priority INTEGER DEFAULT 0,
  approved      INTEGER DEFAULT 1,
  UNIQUE(experience_id, name)
);

CREATE TABLE IF NOT EXISTS resume_points (
  id            INTEGER PRIMARY KEY,
  experience_id INTEGER NOT NULL REFERENCES experience(id) ON DELETE CASCADE,
  project_id    INTEGER REFERENCES projects(id) ON DELETE SET NULL,
  bullet_id     INTEGER REFERENCES bullets(id) ON DELETE SET NULL,
  source_key    TEXT UNIQUE,
  fact_key      TEXT UNIQUE,
  text          TEXT NOT NULL,
  canonical_text TEXT,
  dedupe_key    TEXT,
  importance    INTEGER DEFAULT 5,
  sort_priority INTEGER DEFAULT 0,
  approved      INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS resume_point_variants (
  id              INTEGER PRIMARY KEY,
  resume_point_id INTEGER NOT NULL REFERENCES resume_points(id) ON DELETE CASCADE,
  variant_key     TEXT UNIQUE,
  render_text     TEXT NOT NULL,
  variant_type    TEXT NOT NULL DEFAULT 'resume' CHECK (variant_type IN ('resume', 'short', 'long', 'jd-targeted')),
  profile_key     TEXT REFERENCES resume_profiles(profile_key) ON DELETE CASCADE,
  archetype_id    INTEGER REFERENCES archetypes(id) ON DELETE CASCADE,
  is_default      INTEGER DEFAULT 0,
  sort_priority   INTEGER DEFAULT 0,
  approved        INTEGER DEFAULT 1,
  notes           TEXT
);

CREATE TABLE IF NOT EXISTS resume_point_archetypes (
  resume_point_id INTEGER REFERENCES resume_points(id) ON DELETE CASCADE,
  archetype_id    INTEGER REFERENCES archetypes(id) ON DELETE CASCADE,
  relevance       INTEGER DEFAULT 5,
  inclusion_mode  TEXT CHECK(inclusion_mode IN ('always','prefer','context','hide')) DEFAULT 'context',
  PRIMARY KEY (resume_point_id, archetype_id)
);

CREATE TABLE IF NOT EXISTS project_skills (
  project_id        INTEGER REFERENCES projects(id) ON DELETE CASCADE,
  skill_normalized  TEXT REFERENCES skills_mine(skill_normalized) ON DELETE CASCADE,
  sort_priority     INTEGER DEFAULT 0,
  approved          INTEGER DEFAULT 1,
  PRIMARY KEY (project_id, skill_normalized)
);

CREATE TABLE IF NOT EXISTS resume_point_skills (
  resume_point_id   INTEGER REFERENCES resume_points(id) ON DELETE CASCADE,
  skill_normalized  TEXT REFERENCES skills_mine(skill_normalized) ON DELETE CASCADE,
  relevance         INTEGER DEFAULT 5,
  PRIMARY KEY (resume_point_id, skill_normalized)
);

CREATE TABLE IF NOT EXISTS resume_profiles (
  id         INTEGER PRIMARY KEY,
  profile_key TEXT UNIQUE,
  subtitle   TEXT,
  summary    TEXT,
  approved   INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS resume_profile_signal_rules (
  id                 INTEGER PRIMARY KEY,
  profile_key        TEXT NOT NULL REFERENCES resume_profiles(profile_key) ON DELETE CASCADE,
  signal_key         TEXT NOT NULL,
  operator           TEXT NOT NULL CHECK(operator IN ('gte','lte','eq','present','absent')),
  threshold_numeric  REAL,
  threshold_text     TEXT,
  weight             INTEGER DEFAULT 0,
  action             TEXT NOT NULL DEFAULT 'score' CHECK(action IN ('score','gate','penalty')),
  approved           INTEGER DEFAULT 1,
  notes              TEXT
);

CREATE TABLE IF NOT EXISTS skill_resume_rules (
  skill_normalized  TEXT REFERENCES skills_mine(skill_normalized) ON DELETE CASCADE,
  profile_key       TEXT,
  render_group      TEXT,
  group_rank        INTEGER DEFAULT 0,
  item_rank         INTEGER DEFAULT 0,
  visibility        TEXT DEFAULT 'show',
  emphasis          TEXT DEFAULT 'plain',
  direct_match_boost INTEGER DEFAULT 0,
  singleton_penalty INTEGER DEFAULT 0,
  trigger_condition TEXT,
  notes             TEXT,
  approved          INTEGER DEFAULT 1,
  PRIMARY KEY (skill_normalized, profile_key)
);

CREATE TABLE IF NOT EXISTS resume_group_rules (
  profile_key             TEXT,
  group_id                TEXT,
  label                   TEXT,
  group_rank              INTEGER DEFAULT 0,
  min_items_standalone    INTEGER DEFAULT 1,
  singleton_merge_target  TEXT,
  max_items               INTEGER DEFAULT 8,
  approved                INTEGER DEFAULT 1,
  PRIMARY KEY (profile_key, group_id)
);

CREATE TABLE IF NOT EXISTS resume_layouts (
  id                           INTEGER PRIMARY KEY,
  layout_key                   TEXT UNIQUE NOT NULL,
  layout_family                TEXT NOT NULL DEFAULT 'grouped_standard',
  label                        TEXT NOT NULL,
  page_count                   INTEGER NOT NULL DEFAULT 1,
  max_roles                    INTEGER NOT NULL DEFAULT 4,
  max_projects                 INTEGER NOT NULL DEFAULT 7,
  max_bullets_total            INTEGER NOT NULL DEFAULT 8,
  max_bullets_per_role         INTEGER,
  max_bullets_per_project      INTEGER,
  max_direct_bullets_per_role  INTEGER DEFAULT 0,
  skill_group_char_limit       INTEGER,
  max_education_items          INTEGER DEFAULT 2,
  max_certification_items      INTEGER DEFAULT 4,
  certification_policy         TEXT NOT NULL DEFAULT 'contextual',
  section_order                TEXT,
  min_role_score               INTEGER DEFAULT 0,
  min_project_score            INTEGER DEFAULT 0,
  min_bullet_score             INTEGER DEFAULT 0,
  allow_low_match_context      INTEGER DEFAULT 0,
  approved                     INTEGER DEFAULT 1,
  notes                        TEXT
);

CREATE TABLE IF NOT EXISTS resume_layout_profile_rules (
  profile_key  TEXT NOT NULL REFERENCES resume_profiles(profile_key) ON DELETE CASCADE,
  layout_key   TEXT NOT NULL REFERENCES resume_layouts(layout_key) ON DELETE CASCADE,
  priority     INTEGER DEFAULT 0,
  is_default   INTEGER DEFAULT 0,
  approved     INTEGER DEFAULT 1,
  notes        TEXT,
  PRIMARY KEY (profile_key, layout_key)
);

CREATE TABLE IF NOT EXISTS resume_layout_signal_rules (
  id                 INTEGER PRIMARY KEY,
  layout_key         TEXT NOT NULL REFERENCES resume_layouts(layout_key) ON DELETE CASCADE,
  signal_key         TEXT NOT NULL,
  operator           TEXT NOT NULL CHECK(operator IN ('gte','lte','eq','present','absent')),
  threshold_numeric  REAL,
  threshold_text     TEXT,
  weight             INTEGER DEFAULT 0,
  action             TEXT NOT NULL DEFAULT 'score' CHECK(action IN ('score','gate','penalty')),
  approved           INTEGER DEFAULT 1,
  notes              TEXT
);

CREATE TABLE IF NOT EXISTS resume_layout_group_overrides (
  layout_key               TEXT NOT NULL REFERENCES resume_layouts(layout_key) ON DELETE CASCADE,
  group_id                 TEXT NOT NULL,
  max_items                INTEGER,
  min_items_standalone     INTEGER,
  singleton_merge_target   TEXT,
  approved                 INTEGER DEFAULT 1,
  notes                    TEXT,
  PRIMARY KEY (layout_key, group_id)
);

CREATE TABLE IF NOT EXISTS job_archetype_rules (
  archetype_key       TEXT,
  keyword_text        TEXT,
  keyword_normalized  TEXT,
  weight              INTEGER DEFAULT 1,
  sort_priority       INTEGER DEFAULT 0,
  approved            INTEGER DEFAULT 1,
  notes               TEXT,
  PRIMARY KEY (archetype_key, keyword_normalized)
);

CREATE TABLE IF NOT EXISTS job_keyword_candidates (
  keyword_text        TEXT,
  keyword_normalized  TEXT PRIMARY KEY,
  sort_priority       INTEGER DEFAULT 0,
  approved            INTEGER DEFAULT 1,
  notes               TEXT
);

CREATE TABLE IF NOT EXISTS job_keyword_stopwords (
  term_text          TEXT,
  term_normalized    TEXT PRIMARY KEY,
  kind               TEXT CHECK(kind IN ('stopword','banned')) DEFAULT 'stopword',
  approved           INTEGER DEFAULT 1,
  notes              TEXT
);

CREATE TABLE IF NOT EXISTS bullets (
  id       INTEGER PRIMARY KEY,
  exp_id   INTEGER REFERENCES experience(id),
  text     TEXT,
  metrics  TEXT,
  tags     TEXT,
  strength INTEGER DEFAULT 5,
  UNIQUE(exp_id, text)
);

CREATE TABLE IF NOT EXISTS bullet_archetypes (
  bullet_id    INTEGER REFERENCES bullets(id),
  archetype_id INTEGER REFERENCES archetypes(id),
  relevance    INTEGER DEFAULT 5,
  PRIMARY KEY (bullet_id, archetype_id)
);

CREATE TABLE IF NOT EXISTS skills_mine (
  id               INTEGER PRIMARY KEY,
  skill_name       TEXT,
  skill_normalized TEXT UNIQUE,
  category         TEXT,
  display_category TEXT,
  secondary_categories TEXT,
  resume_priority  INTEGER DEFAULT 5,
  include_default  INTEGER DEFAULT 1,
  require_direct_match INTEGER DEFAULT 0,
  profile_bias     TEXT,
  resume_visibility TEXT DEFAULT 'show',
  resume_display   TEXT,
  resume_emphasis  TEXT DEFAULT 'plain',
  resume_group_rank INTEGER DEFAULT 0,
  level            TEXT CHECK(level IN ('none','exposure','basic','intermediate','advanced','expert')),
  evidence         TEXT
);

CREATE TABLE IF NOT EXISTS skill_aliases (
  id               INTEGER PRIMARY KEY,
  skill_normalized TEXT REFERENCES skills_mine(skill_normalized),
  alias_name       TEXT,
  alias_normalized TEXT NOT NULL,
  archetype_key    TEXT NOT NULL DEFAULT '',
  notes            TEXT,
  UNIQUE(alias_normalized, archetype_key)
);

CREATE TABLE IF NOT EXISTS capabilities_mine (
  id                    INTEGER PRIMARY KEY,
  capability_name       TEXT,
  capability_normalized TEXT UNIQUE,
  category              TEXT,
  level                 TEXT CHECK(level IN ('none','exposure','basic','intermediate','advanced','expert')),
  resume_priority       INTEGER DEFAULT 5,
  evidence              TEXT,
  notes                 TEXT
);

CREATE TABLE IF NOT EXISTS capability_aliases (
  id                    INTEGER PRIMARY KEY,
  capability_normalized TEXT REFERENCES capabilities_mine(capability_normalized),
  alias_name            TEXT,
  alias_normalized      TEXT NOT NULL,
  archetype_key         TEXT NOT NULL DEFAULT '',
  notes                 TEXT,
  UNIQUE(alias_normalized, archetype_key)
);

CREATE TABLE IF NOT EXISTS certifications (
  id     INTEGER PRIMARY KEY,
  name   TEXT,
  issuer TEXT,
  date   TEXT,
  status TEXT DEFAULT 'earned'
);

CREATE TABLE IF NOT EXISTS education (
  id        INTEGER PRIMARY KEY,
  school    TEXT,
  degree    TEXT,
  field     TEXT,
  status    TEXT,
  grad_year TEXT
);

CREATE TABLE IF NOT EXISTS archetypes (
  id    INTEGER PRIMARY KEY,
  name  TEXT UNIQUE,
  evals INTEGER DEFAULT 0,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS skill_signals (
  id               INTEGER PRIMARY KEY,
  archetype_id     INTEGER REFERENCES archetypes(id),
  type             TEXT CHECK(type IN ('matched','missing')),
  skill_name       TEXT,
  skill_normalized TEXT,
  count            INTEGER DEFAULT 1,
  notes            TEXT,
  UNIQUE(archetype_id, type, skill_normalized)
);

CREATE TABLE IF NOT EXISTS roles (
  num               INTEGER PRIMARY KEY,
  date              TEXT,
  first_seen_date   TEXT,
  last_updated_date TEXT,
  company           TEXT,
  role              TEXT,
  location_text     TEXT,
  work_model        TEXT,
  compensation_text TEXT,
  salary_min        REAL,
  salary_max        REAL,
  salary_currency   TEXT,
  salary_period     TEXT,
  score             REAL,
  score_pinned      INTEGER DEFAULT 0,
  cv_match          REAL,
  role_fit          REAL,
  comp              REAL,
  work_pref         REAL,
  red_flag_penalty  REAL DEFAULT 0,
  status            TEXT,
  pdf               INTEGER DEFAULT 0,
  report            TEXT,
  source            TEXT,
  url               TEXT,
  found_via         TEXT,
  apply_method      TEXT,
  notes             TEXT,
  UNIQUE(company, role)
);

CREATE TABLE IF NOT EXISTS found_via_sources (
  slug          TEXT PRIMARY KEY,
  label         TEXT NOT NULL,
  sort_priority INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS qualifications_mine (
  id                       INTEGER PRIMARY KEY,
  qualification_name       TEXT NOT NULL,
  qualification_normalized TEXT NOT NULL UNIQUE,
  category                 TEXT,
  met                      TEXT NOT NULL CHECK(met IN ('yes','partial','no')) DEFAULT 'no',
  notes                    TEXT
);

CREATE TABLE IF NOT EXISTS qualification_aliases (
  id                       INTEGER PRIMARY KEY,
  qualification_normalized TEXT NOT NULL REFERENCES qualifications_mine(qualification_normalized),
  alias_name               TEXT NOT NULL,
  alias_normalized         TEXT NOT NULL,
  archetype_key            TEXT NOT NULL DEFAULT '',
  notes                    TEXT,
  UNIQUE(alias_normalized, archetype_key)
);

CREATE TABLE IF NOT EXISTS role_requirements (
  id                    INTEGER PRIMARY KEY,
  role_id               INTEGER REFERENCES roles(num),
  raw_text              TEXT,
  requirement_name      TEXT,
  requirement_normalized TEXT,
  kind                  TEXT CHECK(kind IN ('skill','capability','qualification','unknown')),
  priority              TEXT CHECK(priority IN ('required','preferred','bonus','unknown')) DEFAULT 'unknown',
  matched_entity_type   TEXT CHECK(matched_entity_type IN ('skill','capability','qualification','none')) DEFAULT 'none',
  matched_normalized    TEXT,
  match_method          TEXT CHECK(match_method IN ('exact','alias','similar','new_candidate','unmatched')) DEFAULT 'unmatched',
  confidence            REAL,
  source                TEXT,
  notes                 TEXT,
  UNIQUE(role_id, requirement_normalized, kind, priority)
);

CREATE TABLE IF NOT EXISTS role_archetype_scores (
  role_id            INTEGER NOT NULL REFERENCES roles(num) ON DELETE CASCADE,
  archetype_key      TEXT    NOT NULL,
  score              REAL    NOT NULL,
  rank               INTEGER NOT NULL,
  evidence_json      TEXT,
  requirement_score  REAL    NOT NULL DEFAULT 0,
  keyword_score      REAL    NOT NULL DEFAULT 0,
  title_score        REAL    NOT NULL DEFAULT 0,
  approved           INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (role_id, archetype_key)
);

CREATE INDEX IF NOT EXISTS idx_role_archetype_scores_role_rank
  ON role_archetype_scores(role_id, rank);

CREATE INDEX IF NOT EXISTS idx_role_archetype_scores_archetype
  ON role_archetype_scores(archetype_key, score DESC);

CREATE TABLE IF NOT EXISTS stories (
  id          INTEGER PRIMARY KEY,
  title       TEXT,
  situation   TEXT,
  task        TEXT,
  action      TEXT,
  result      TEXT,
  reflection  TEXT,
  metrics     TEXT,
  competencies TEXT
);

CREATE TABLE IF NOT EXISTS story_archetypes (
  story_id     INTEGER REFERENCES stories(id),
  archetype_id INTEGER REFERENCES archetypes(id),
  relevance    INTEGER DEFAULT 5,
  PRIMARY KEY (story_id, archetype_id)
);

CREATE TABLE IF NOT EXISTS market_cache (
  company   TEXT,
  role_slug TEXT,
  date      TEXT,
  content   TEXT,
  PRIMARY KEY (company, role_slug)
);

CREATE TABLE IF NOT EXISTS cover_letter_profiles (
  archetype      TEXT PRIMARY KEY,
  base_archetype TEXT,   -- if set, inherits content from this archetype's row
  opening        TEXT,   -- para 1: credential hook ("I'm applying for...")
  body_p1        TEXT,   -- para 2: main experience evidence
  body_p2        TEXT,   -- para 3: secondary angle (null = omit)
  closing        TEXT    -- final line before sign-off
);

CREATE TABLE IF NOT EXISTS cover_letter_modules (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  key       TEXT    NOT NULL,
  archetype TEXT    NOT NULL DEFAULT '*',  -- '*' = any archetype
  position  INTEGER NOT NULL DEFAULT 99,  -- lower = inserted earlier
  text      TEXT    NOT NULL
);

DROP VIEW IF EXISTS gap_analysis;
CREATE VIEW IF NOT EXISTS gap_analysis AS
SELECT
  a.name  AS archetype,
  a.evals,
  ss.type,
  ss.skill_name,
  ss.skill_normalized,
  ss.count,
  ROUND(ss.count * 1.0 / MAX(a.evals, 1), 2) AS frequency,
  sm.level AS my_level,
  CASE
    WHEN ss.type = 'missing' AND (sm.level IS NULL OR sm.level = 'none')  THEN 'hard_gap'
    WHEN ss.type = 'missing' AND sm.level IN ('exposure','basic')          THEN 'closeable'
    WHEN ss.type = 'missing' AND sm.level IN ('intermediate','advanced')   THEN 'soft_gap'
    WHEN ss.type = 'matched'                                               THEN 'strength'
  END AS signal
FROM skill_signals ss
JOIN archetypes a  ON ss.archetype_id = a.id
LEFT JOIN skills_mine sm ON ss.skill_normalized = sm.skill_normalized
ORDER BY a.evals DESC, ss.count DESC;

CREATE TABLE IF NOT EXISTS question_bank (
  id       INTEGER PRIMARY KEY,
  slug     TEXT UNIQUE NOT NULL,
  prompt   TEXT NOT NULL,
  answer   TEXT NOT NULL,
  tags     TEXT,
  adapt    INTEGER DEFAULT 0,
  approved INTEGER DEFAULT 1,
  notes    TEXT
);
`;

// ──────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────

// ──────────────────────────────────────────────────────────
// Migration: add score dimension columns to existing DBs
// ──────────────────────────────────────────────────────────
function migrateRolesTable(db) {
  // Rename cultural -> work_pref before adding columns (must run before newCols adds work_pref)
  try {
    const existingCols = db.prepare("PRAGMA table_info(roles)").all().map(r => r.name);
    if (existingCols.includes('cultural') && !existingCols.includes('work_pref')) {
      db.exec(`ALTER TABLE roles RENAME COLUMN cultural TO work_pref`);
    } else if (existingCols.includes('cultural') && existingCols.includes('work_pref')) {
      db.exec(`ALTER TABLE roles DROP COLUMN cultural`);
    }
  } catch {
    // ignore if not supported
  }

  const newCols = [
    'cv_match REAL',
    'role_fit REAL',
    'comp REAL',
    'work_pref REAL',
    'red_flag_penalty REAL DEFAULT 0',
    'first_seen_date TEXT',
    'last_updated_date TEXT',
    'location_text TEXT',
    'work_model TEXT',
    'compensation_text TEXT',
    'salary_min REAL',
    'salary_max REAL',
    'salary_currency TEXT',
    'salary_period TEXT',
    'found_via TEXT',
    'apply_method TEXT',
    'score_pinned INTEGER DEFAULT 0',
  ];
  for (const col of newCols) {
    try {
      db.exec(`ALTER TABLE roles ADD COLUMN ${col}`);
    } catch {
      // column already exists — ignore
    }
  }

  // Rename via -> url if needed (SQLite 3.25+ supports RENAME COLUMN)
  try {
    const cols = db.prepare("PRAGMA table_info(roles)").all().map(r => r.name);
    if (cols.includes('via') && !cols.includes('url')) {
      db.exec(`ALTER TABLE roles RENAME COLUMN via TO url`);
    } else if (!cols.includes('via') && !cols.includes('url')) {
      db.exec(`ALTER TABLE roles ADD COLUMN url TEXT`);
    }
  } catch {
    // ignore if rename not supported
  }

  try {
    db.exec(`
      UPDATE roles
      SET first_seen_date = COALESCE(first_seen_date, date),
          last_updated_date = COALESCE(last_updated_date, date)
      WHERE first_seen_date IS NULL OR last_updated_date IS NULL
    `);
  } catch {
    // ignore if roles table is not ready yet
  }

  // Seed found_via_sources if empty
  try {
    db.exec(`
      CREATE TABLE IF NOT EXISTS found_via_sources (
        slug          TEXT PRIMARY KEY,
        label         TEXT NOT NULL,
        sort_priority INTEGER DEFAULT 0
      )
    `);
    const count = db.prepare("SELECT COUNT(*) AS n FROM found_via_sources").get().n;
    if (count === 0) {
      const insert = db.prepare(
        "INSERT OR IGNORE INTO found_via_sources (slug, label, sort_priority) VALUES (?, ?, ?)"
      );
      const defaults = [
        ['linkedin',       'LinkedIn',         10],
        ['indeed',         'Indeed',           20],
        ['greenhouse',     'Greenhouse',       30],
        ['glassdoor',      'Glassdoor',        40],
        ['hitmarker',      'Hitmarker',        50],
        ['workwithindies', 'Work With Indies', 60],
        ['company',        'Company site',     70],
        ['referral',       'Referral',         80],
        ['other',          'Other',            99],
      ];
      for (const [slug, label, sort_priority] of defaults) {
        insert.run(slug, label, sort_priority);
      }
    }
  } catch {
    // ignore seed errors
  }
}

function migrateSkillsTable(db) {
  const newCols = [
    'display_category TEXT',
    'secondary_categories TEXT',
    'resume_priority INTEGER DEFAULT 5',
    'include_default INTEGER DEFAULT 1',
    'require_direct_match INTEGER DEFAULT 0',
    'profile_bias TEXT',
    "resume_visibility TEXT DEFAULT 'show'",
    'resume_display TEXT',
    "resume_emphasis TEXT DEFAULT 'plain'",
    'resume_group_rank INTEGER DEFAULT 0',
  ];

  for (const col of newCols) {
    try {
      db.exec(`ALTER TABLE skills_mine ADD COLUMN ${col}`);
    } catch {
      // column already exists — ignore
    }
  }
}

function rebuildAliasTable(db, tableName, fkColumn) {
  const tempName = `${tableName}_v2`;
  db.exec(`DROP TABLE IF EXISTS ${tempName}`);
  db.exec(`
    CREATE TABLE ${tempName} (
      id INTEGER PRIMARY KEY,
      ${fkColumn} TEXT NOT NULL,
      alias_name TEXT NOT NULL,
      alias_normalized TEXT NOT NULL,
      archetype_key TEXT NOT NULL DEFAULT '',
      notes TEXT,
      UNIQUE(alias_normalized, archetype_key)
    )
  `);

  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all().map((row) => row.name);
  if (columns.length) {
    const hasArchetypeKey = columns.includes('archetype_key');
    const selectArchetype = hasArchetypeKey ? "COALESCE(archetype_key, '')" : "''";
    db.exec(`
      INSERT OR IGNORE INTO ${tempName} (${fkColumn}, alias_name, alias_normalized, archetype_key, notes)
      SELECT ${fkColumn}, alias_name, alias_normalized, ${selectArchetype}, notes
      FROM ${tableName}
      WHERE ${fkColumn} IS NOT NULL
        AND alias_name IS NOT NULL
        AND alias_normalized IS NOT NULL
    `);
    db.exec(`DROP TABLE ${tableName}`);
  }

  db.exec(`ALTER TABLE ${tempName} RENAME TO ${tableName}`);
}

function migrateAliasTables(db) {
  rebuildAliasTable(db, 'skill_aliases', 'skill_normalized');
  rebuildAliasTable(db, 'capability_aliases', 'capability_normalized');
  rebuildAliasTable(db, 'qualification_aliases', 'qualification_normalized');
}

function migrateSkillResumeRulesTable(db) {
  const newCols = [
    'trigger_condition TEXT',
    'notes TEXT',
  ];

  for (const col of newCols) {
    try {
      db.exec(`ALTER TABLE skill_resume_rules ADD COLUMN ${col}`);
    } catch {
      // column already exists — ignore
    }
  }
}

function migrateResumePointsTable(db) {
  const newCols = [
    'fact_key TEXT',
    'canonical_text TEXT',
  ];

  for (const col of newCols) {
    try {
      db.exec(`ALTER TABLE resume_points ADD COLUMN ${col}`);
    } catch {
      // column already exists — ignore
    }
  }

  try {
    db.exec(`
      UPDATE resume_points
      SET fact_key = COALESCE(NULLIF(TRIM(fact_key), ''), source_key, 'point-' || id)
      WHERE fact_key IS NULL OR TRIM(fact_key) = ''
    `);
  } catch {
    // ignore if table is not ready yet
  }

  try {
    db.exec(`
      UPDATE resume_points
      SET canonical_text = COALESCE(NULLIF(TRIM(canonical_text), ''), text)
      WHERE canonical_text IS NULL OR TRIM(canonical_text) = ''
    `);
  } catch {
    // ignore if table is not ready yet
  }
}

function migrateResumeLayoutsTable(db) {
  const newCols = [
    "layout_family TEXT NOT NULL DEFAULT 'grouped_standard'",
    'max_education_items INTEGER DEFAULT 2',
    'max_certification_items INTEGER DEFAULT 4',
    "certification_policy TEXT NOT NULL DEFAULT 'contextual'",
    'section_order TEXT',
  ];

  for (const col of newCols) {
    try {
      db.exec(`ALTER TABLE resume_layouts ADD COLUMN ${col}`);
    } catch {
      // column already exists — ignore
    }
  }

  try {
    db.exec(`
      UPDATE resume_layouts
      SET layout_family = COALESCE(NULLIF(TRIM(layout_family), ''), 'grouped_standard'),
          max_education_items = COALESCE(max_education_items, 2),
          max_certification_items = COALESCE(max_certification_items, 4),
          certification_policy = COALESCE(NULLIF(TRIM(certification_policy), ''), 'contextual'),
          section_order = COALESCE(
            NULLIF(TRIM(section_order), ''),
            CASE
              WHEN layout_key LIKE '%top-certs%' THEN 'summary,skills,certifications,experience,education'
              ELSE 'summary,skills,experience,education,certifications'
            END
          )
      WHERE 1 = 1
    `);
  } catch {
    // ignore if table is not ready yet
  }
}

function normalizeSkill(name) {
  // Strip count annotations and normalize punctuation-heavy JD fragments into stable lookup keys.
  return String(name ?? '')
    .replace(/\s*\(\d+x[^)]*\)/g, '')
    .replace(/\s*--.*$/, '')
    .toLowerCase()
    .replace(/[()]+/g, ' ')
    .replace(/[^a-z0-9+#/.\-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeText(value) {
  return String(value ?? '')
    .toLowerCase()
    .replace(/[^a-z0-9+#/.\-]+/g, ' ')
    .trim();
}

function escapeRegExp(value) {
  return String(value ?? '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function parseCompensation(text) {
  const source = String(text ?? '');
  if (!source || /\bno salary\b/i.test(source)) {
    return { compensation_text: /\bno salary\b/i.test(source) ? 'no salary' : null, salary_min: null, salary_max: null, salary_currency: null, salary_period: null };
  }

  const rangeMatch = source.match(/\$([\d.,]+)\s*[Kk]?\s*[–-]\s*\$?([\d.,]+)\s*[Kk]?\s*(USD|CAD)?/);
  const singleMatch = source.match(/\$([\d.,]+)\s*[Kk]?\s*(USD|CAD)?/);
  const monthlyMatch = source.match(/\$([\d.,]+)\s*\/\s*mo/i);

  function parseMoney(raw) {
    if (!raw) return null;
    const hasK = /k$/i.test(raw.trim());
    const numeric = parseFloat(raw.replace(/[^0-9.]/g, ''));
    if (Number.isNaN(numeric)) return null;
    return hasK ? numeric * 1000 : numeric;
  }

  if (rangeMatch) {
    return {
      compensation_text: rangeMatch[0],
      salary_min: parseMoney(rangeMatch[1]),
      salary_max: parseMoney(rangeMatch[2]),
      salary_currency: rangeMatch[3] || 'USD',
      salary_period: /\/\s*mo/i.test(source) ? 'monthly' : 'yearly',
    };
  }

  if (monthlyMatch) {
    const value = parseMoney(monthlyMatch[1]);
    return {
      compensation_text: monthlyMatch[0],
      salary_min: value,
      salary_max: value,
      salary_currency: 'USD',
      salary_period: 'monthly',
    };
  }

  if (singleMatch) {
    const value = parseMoney(singleMatch[1]);
    return {
      compensation_text: singleMatch[0],
      salary_min: value,
      salary_max: value,
      salary_currency: singleMatch[2] || 'USD',
      salary_period: 'yearly',
    };
  }

  return { compensation_text: null, salary_min: null, salary_max: null, salary_currency: null, salary_period: null };
}

function parseWorkModel(text) {
  const source = String(text ?? '');
  if (/\bremote\b/i.test(source)) return 'remote';
  if (/\bhybrid\b/i.test(source)) return 'hybrid';
  if (/\bon[- ]site\b/i.test(source)) return 'onsite';
  return null;
}

function parseRoleMetadata(notes) {
  const text = String(notes ?? '').trim();
  const segments = text.split(';').map((s) => s.trim()).filter(Boolean);
  let locationText = null;
  let compensationText = null;
  let workModel = parseWorkModel(text);
  const requirementSegments = [];
  const nonRequirementSegments = [];

  for (const segment of segments) {
    const lower = segment.toLowerCase();
    const comp = parseCompensation(segment);
    const looksLikeLocation =
      /\b(remote|hybrid|on[- ]site|onsite)\b/i.test(segment) ||
      /\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)*\s(?:CA|TX|WA|FL|AL|UK|USD|CAD|Romania|Germany|Canada|Spain|Sweden|China|India|Australia|Scotland|Finland|Czech Republic|Poland|Colombia|Ukraine)\b/.test(segment) ||
      /\b(?:Bucharest|Vancouver|Montreal|Austin|Orlando|Seville|Barcelona|Dallas|Kyiv|Sofia|Dundee|Prague|Warsaw|Helsinki|Irvine|Redmond|San Francisco|Seattle|Los Angeles|Melbourne|Stockholm|Bogota|Hyderabad)\b/i.test(segment);

    if (!locationText && looksLikeLocation) {
      locationText = segment;
      if (!workModel) workModel = parseWorkModel(segment);
      nonRequirementSegments.push(segment);
      continue;
    }

    if (!compensationText && (comp.compensation_text || /\bno salary\b/i.test(segment))) {
      compensationText = comp.compensation_text || 'no salary';
      nonRequirementSegments.push(segment);
      continue;
    }

    if (/\b(resume:|applied by decision|waiting to hear back|phone screen|last contact|direct email|recruiter outreach|low priority|pivot role|overqualified|position filled|unknown role|resume submitted|pre[- ]?score|score:|contractor via staffing co|call mon|ai-written apps voided|technical take-home|weak fit|strong match|strong fit|medium fit|direct match|full match|zero hard gaps)\b/i.test(segment)) {
      nonRequirementSegments.push(segment);
      continue;
    }

    requirementSegments.push(segment);
  }

  return {
    location_text: locationText,
    work_model: workModel,
    ...parseCompensation(compensationText || text),
    requirement_segments: requirementSegments,
    non_requirement_segments: nonRequirementSegments,
  };
}

function loadRequirementMatcher(db) {
  const skills = db.prepare('SELECT skill_name, skill_normalized FROM skills_mine').all();
  const skillAliases = db.prepare("SELECT skill_normalized, alias_name, alias_normalized, COALESCE(archetype_key, '') AS archetype_key FROM skill_aliases").all();
  const capabilities = db.prepare('SELECT capability_name, capability_normalized FROM capabilities_mine').all();
  const capabilityAliases = db.prepare("SELECT capability_normalized, alias_name, alias_normalized, COALESCE(archetype_key, '') AS archetype_key FROM capability_aliases").all();

  const entries = [];
  for (const row of skills) {
    entries.push({ kind: 'skill', canonical_name: row.skill_name, canonical_normalized: row.skill_normalized, term: row.skill_name, term_normalized: row.skill_normalized, match_method: 'exact', archetype_key: '' });
  }
  for (const row of skillAliases) {
    entries.push({ kind: 'skill', canonical_name: row.alias_name, canonical_normalized: row.skill_normalized, term: row.alias_name, term_normalized: row.alias_normalized, match_method: 'alias', archetype_key: row.archetype_key || '' });
  }
  for (const row of capabilities) {
    entries.push({ kind: 'capability', canonical_name: row.capability_name, canonical_normalized: row.capability_normalized, term: row.capability_name, term_normalized: row.capability_normalized, match_method: 'exact', archetype_key: '' });
  }
  for (const row of capabilityAliases) {
    entries.push({ kind: 'capability', canonical_name: row.alias_name, canonical_normalized: row.capability_normalized, term: row.alias_name, term_normalized: row.alias_normalized, match_method: 'alias', archetype_key: row.archetype_key || '' });
  }

  entries.sort((a, b) => (
    b.term_normalized.length - a.term_normalized.length ||
    (b.archetype_key ? 1 : 0) - (a.archetype_key ? 1 : 0) ||
    a.kind.localeCompare(b.kind)
  ));
  return entries;
}

function inferPriority(segment) {
  const lower = String(segment ?? '').toLowerCase();
  if (/\bbonus\b/.test(lower)) return 'bonus';
  if (/\b(preferred|desired)\b/.test(lower)) return 'preferred';
  if (/\b(required|req|must)\b/.test(lower)) return 'required';
  return 'unknown';
}

function deriveRoleScore(roleRecord, matchedEntries, notes) {
  const explicit = String(notes ?? '').match(/\b(?:pre[- ]score|score)\s*[:=]?\s*(\d+(?:\.\d+)?)\b/i);
  if (explicit) {
    return Math.max(0, Math.min(10, Math.round(parseFloat(explicit[1]) * 2) / 2));
  }

  let score = 5.0;
  const roleText = `${roleRecord.role || ''} ${notes || ''}`;
  const lower = roleText.toLowerCase();
  const gapCount = (notes.match(/\bgap[s]?\b/gi) || []).length;
  const matchCount = matchedEntries.length;

  if (/\b(strong match|direct match|full match|zero hard gaps)\b/i.test(lower)) score += 1.5;
  if (/\b(medium fit|relevant|match)\b/i.test(lower)) score += 0.5;
  if (/\b(weak fit|below floor|no production frontend experience)\b/i.test(lower)) score -= 1.5;
  if (/\b(overqualified|low priority|pivot role)\b/i.test(lower)) score -= 0.5;
  if (/\b(lead|principal|staff|manager|director)\b/i.test(roleRecord.role || '')) score -= 0.5;
  if (roleRecord.salary_max != null && roleRecord.salary_max < 80000) score -= 2.0;
  else if (roleRecord.salary_min != null && roleRecord.salary_min >= 100000) score += 0.5;
  if (roleRecord.work_model === 'remote') score += 0.4;
  if (roleRecord.work_model === 'hybrid') score += 0.1;
  if (roleRecord.work_model === 'onsite') score -= 0.3;

  score += Math.min(2.0, matchCount * 0.15);
  score -= Math.min(2.0, gapCount * 0.5);

  return Math.max(0, Math.min(10, Math.round(score * 2) / 2));
}

function inferPrimaryArchetype(db, roleText, notes) {
  const ruleRows = db.prepare(`
    SELECT archetype_key, keyword_text, keyword_normalized, weight
    FROM job_archetype_rules
    WHERE approved = 1
  `).all();
  if (!ruleRows.length) return '';

  const titleText = normalizeText(roleText || '');
  const keywordText = normalizeText(`${roleText || ''} ${notes || ''}`);
  const scores = new Map();

  for (const row of ruleRows) {
    const term = row.keyword_normalized || normalizeText(row.keyword_text);
    const weight = Number(row.weight || 0);
    if (!term || !weight) continue;

    let score = scores.get(row.archetype_key) || 0;
    if (normalizedContains(titleText, term)) score += weight * 1.2;
    if (normalizedContains(keywordText, term)) score += weight;
    scores.set(row.archetype_key, score);
  }

  const ranked = [...scores.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
  return ranked.length && ranked[0][1] > 0 ? ranked[0][0] : '';
}

function buildRoleRequirements(db, roleId, roleText, notes, matcherEntries, primaryArchetype = '') {
  const basis = `${roleText || ''}; ${notes || ''}`;
  const normalizedBasis = normalizeText(basis);
  const matched = new Map();
  const requirementRows = [];

  for (const entry of matcherEntries) {
    if (!entry.term_normalized) continue;
    if (entry.match_method === 'alias' && entry.archetype_key && entry.archetype_key !== primaryArchetype) continue;
    const pattern = new RegExp(`(^| )${escapeRegExp(entry.term_normalized)}( |$)`);
    if (!pattern.test(normalizedBasis)) continue;
    const key = `${entry.kind}:${entry.canonical_normalized}:${entry.term_normalized}`;
    if (matched.has(key)) continue;
    matched.set(key, true);
    requirementRows.push({
      role_id: roleId,
      raw_text: entry.term,
      requirement_name: entry.term,
      requirement_normalized: entry.term_normalized,
      kind: entry.kind,
      priority: 'unknown',
      matched_entity_type: entry.kind,
      matched_normalized: entry.canonical_normalized,
      match_method: entry.match_method,
      confidence: entry.match_method === 'exact' ? 1 : 0.9,
      source: 'tracker_notes',
      notes: entry.match_method === 'alias' && entry.archetype_key ? `archetype=${entry.archetype_key}` : null,
    });
  }

  const metadata = parseRoleMetadata(notes);
  for (const segment of metadata.requirement_segments) {
    const normalizedSegment = normalizeText(segment);
    if (!normalizedSegment) continue;
    const alreadyRepresented = requirementRows.some((row) => normalizedSegment.includes(row.requirement_normalized));
    if (alreadyRepresented) continue;
    requirementRows.push({
      role_id: roleId,
      raw_text: segment,
      requirement_name: segment,
      requirement_normalized: normalizedSegment,
      kind: 'unknown',
      priority: inferPriority(segment),
      matched_entity_type: 'none',
      matched_normalized: null,
      match_method: 'new_candidate',
      confidence: 0.4,
      source: 'tracker_notes',
      notes: 'Unmatched tracker note segment; review for alias or new inventory entry.',
    });
  }

  db.prepare('DELETE FROM role_requirements WHERE role_id = ?').run(roleId);
  const insert = db.prepare(`
    INSERT INTO role_requirements (
      role_id, raw_text, requirement_name, requirement_normalized, kind, priority,
      matched_entity_type, matched_normalized, match_method, confidence, source, notes
    ) VALUES (
      @role_id, @raw_text, @requirement_name, @requirement_normalized, @kind, @priority,
      @matched_entity_type, @matched_normalized, @match_method, @confidence, @source, @notes
    )
    ON CONFLICT(role_id, requirement_normalized, kind, priority) DO UPDATE SET
      raw_text = excluded.raw_text,
      requirement_name = excluded.requirement_name,
      matched_entity_type = excluded.matched_entity_type,
      matched_normalized = excluded.matched_normalized,
      match_method = excluded.match_method,
      confidence = excluded.confidence,
      source = excluded.source,
      notes = excluded.notes
  `);

  for (const row of requirementRows) insert.run(row);
  return requirementRows;
}

function normalizedContains(text, term) {
  const haystack = normalizeText(text);
  const needle = normalizeText(term);
  if (!haystack || !needle) return false;
  return (` ${haystack} `).includes(` ${needle} `);
}

function scoreRoleArchetypes(db, roleId, roleRecord, requirementRows = []) {
  const ruleRows = db.prepare(`
    SELECT archetype_key, keyword_text, keyword_normalized, weight, sort_priority
    FROM job_archetype_rules
    WHERE approved = 1
    ORDER BY archetype_key ASC, sort_priority ASC
  `).all();
  if (!ruleRows.length) {
    db.prepare('DELETE FROM role_archetype_scores WHERE role_id = ?').run(roleId);
    return [];
  }

  const titleText = normalizeText(roleRecord.role || '');
  const keywordText = normalizeText([
    roleRecord.role,
    roleRecord.notes,
    roleRecord.location_text,
    roleRecord.work_model,
    roleRecord.compensation_text,
  ].filter(Boolean).join(' '));

  const requirementTexts = requirementRows.map((row) => ({
    text: normalizeText([
      row.raw_text,
      row.requirement_name,
      row.requirement_normalized,
      row.matched_normalized,
    ].filter(Boolean).join(' ')),
    priority: row.priority || 'unknown',
  }));

  const priorityFactor = {
    required: 1.15,
    preferred: 1.0,
    bonus: 0.85,
    unknown: 1.0,
  };

  const byArchetype = new Map();
  for (const row of ruleRows) {
    const entry = byArchetype.get(row.archetype_key) || {
      archetype_key: row.archetype_key,
      requirement_score: 0,
      keyword_score: 0,
      title_score: 0,
      evidence: {
        title_terms: [],
        keyword_terms: [],
        requirement_terms: [],
      },
    };

    const term = row.keyword_normalized || normalizeText(row.keyword_text);
    const weight = Number(row.weight || 0);
    if (!term || !weight) {
      byArchetype.set(row.archetype_key, entry);
      continue;
    }

    if (normalizedContains(titleText, term)) {
      entry.title_score += weight;
      entry.evidence.title_terms.push(row.keyword_text);
    }

    if (normalizedContains(keywordText, term)) {
      entry.keyword_score += weight;
      entry.evidence.keyword_terms.push(row.keyword_text);
    }

    let requirementContribution = 0;
    for (const requirement of requirementTexts) {
      if (!normalizedContains(requirement.text, term)) continue;
      const weighted = weight * (priorityFactor[requirement.priority] || 1.0);
      if (weighted > requirementContribution) {
        requirementContribution = weighted;
      }
    }
    if (requirementContribution > 0) {
      entry.requirement_score += requirementContribution;
      entry.evidence.requirement_terms.push(row.keyword_text);
    }

    byArchetype.set(row.archetype_key, entry);
  }

  const scored = [...byArchetype.values()].map((entry) => {
    const raw_score =
      (entry.requirement_score * 0.6) +
      (entry.keyword_score * 0.3) +
      (entry.title_score * 0.1);
    return {
      ...entry,
      raw_score,
    };
  });

  const maxRaw = Math.max(...scored.map((entry) => entry.raw_score), 0);
  const ranked = scored
    .map((entry) => ({
      ...entry,
      score: maxRaw > 0 ? Number((entry.raw_score / maxRaw).toFixed(4)) : 0,
    }))
    .sort((a, b) => (
      b.raw_score - a.raw_score ||
      b.requirement_score - a.requirement_score ||
      b.keyword_score - a.keyword_score ||
      a.archetype_key.localeCompare(b.archetype_key)
    ))
    .map((entry, index) => ({
      role_id: roleId,
      archetype_key: entry.archetype_key,
      score: entry.score,
      rank: index + 1,
      evidence_json: JSON.stringify({
        raw_score: Number(entry.raw_score.toFixed(4)),
        title_terms: [...new Set(entry.evidence.title_terms)],
        keyword_terms: [...new Set(entry.evidence.keyword_terms)],
        requirement_terms: [...new Set(entry.evidence.requirement_terms)],
      }),
      requirement_score: Number(entry.requirement_score.toFixed(4)),
      keyword_score: Number(entry.keyword_score.toFixed(4)),
      title_score: Number(entry.title_score.toFixed(4)),
      approved: 1,
    }));

  db.prepare('DELETE FROM role_archetype_scores WHERE role_id = ?').run(roleId);
  const insert = db.prepare(`
    INSERT INTO role_archetype_scores (
      role_id, archetype_key, score, rank, evidence_json,
      requirement_score, keyword_score, title_score, approved
    ) VALUES (
      @role_id, @archetype_key, @score, @rank, @evidence_json,
      @requirement_score, @keyword_score, @title_score, @approved
    )
  `);
  for (const row of ranked) {
    insert.run(row);
  }
  return ranked;
}

function getOrCreateArchetype(db, name) {
  const existing = db.prepare('SELECT id FROM archetypes WHERE name = ?').get(name);
  if (existing) return existing.id;
  const info = db.prepare('INSERT OR IGNORE INTO archetypes (name) VALUES (?)').run(name);
  return info.lastInsertRowid || db.prepare('SELECT id FROM archetypes WHERE name = ?').get(name).id;
}

// ──────────────────────────────────────────────────────────
// Seed: profile
// ──────────────────────────────────────────────────────────
function seedProfile(db) {
  // Personal contact and profile data is populated by scripts/local/personal-seeds.mjs
  // Run: node scripts/local/personal-seeds.mjs
}

// ──────────────────────────────────────────────────────────
// Seed: experience
// ──────────────────────────────────────────────────────────
function seedExperience(db) {
  // Experience records are populated by scripts/local/personal-seeds.mjs
}

// ──────────────────────────────────────────────────────────
// Seed: bullets + bullet_archetypes
// ──────────────────────────────────────────────────────────
function seedBullets(db) {
  // Resume bullets are populated by scripts/local/personal-seeds.mjs
}

// ──────────────────────────────────────────────────────────
// Seed: resume schema (projects, resume_points, profiles)
// ──────────────────────────────────────────────────────────
function seedResumeSchema(db) {
  // Projects, resume points, and variants are populated by scripts/local/personal-seeds.mjs
  // This function intentionally left empty for public distribution.
  // Clear tables so they don't contain stale data from a previous personal seed run
  // that was then removed.
}

function getResumeProfiles() {
  return [];
}

function getResumeProfileSignalRules() {
  return [];
}

// ──────────────────────────────────────────────────────────
// Cover letter seed data
// ──────────────────────────────────────────────────────────

function getCoverLetterProfiles() {
  // Cover letter content is populated by scripts/local/personal-seeds.mjs
  return [];
}

function getCoverLetterModules() {
  // Cover letter modules are populated by scripts/local/personal-seeds.mjs
  return [];
}

function seedCoverLetterProfiles(db) {
  const insert = db.prepare(`
    INSERT OR IGNORE INTO cover_letter_profiles (archetype, base_archetype, opening, body_p1, body_p2, closing)
    VALUES (@archetype, @base_archetype, @opening, @body_p1, @body_p2, @closing)
  `);
  for (const row of getCoverLetterProfiles()) insert.run(row);
}

function seedCoverLetterModules(db) {
  const insert = db.prepare(`
    INSERT INTO cover_letter_modules (key, archetype, position, text)
    VALUES (@key, @archetype, @position, @text)
  `);
  for (const row of getCoverLetterModules()) insert.run(row);
}

function seedResumeProfiles(db, resumeProfiles = getResumeProfiles()) {
  const insertProfile = db.prepare(`
    INSERT OR IGNORE INTO resume_profiles (profile_key, subtitle, summary, approved)
    VALUES (@profile_key, @subtitle, @summary, @approved)
  `);
  for (const profile of resumeProfiles) {
    insertProfile.run(profile);
  }
}

function seedResumeProfileSignalRules(db, rules = getResumeProfileSignalRules()) {
  const insertRule = db.prepare(`
    INSERT OR IGNORE INTO resume_profile_signal_rules (
      profile_key, signal_key, operator, threshold_numeric, threshold_text, weight, action, approved, notes
    )
    VALUES (
      @profile_key, @signal_key, @operator, @threshold_numeric, @threshold_text, @weight, @action, @approved, @notes
    )
  `);
  for (const rule of rules) {
    insertRule.run(rule);
  }
}

function seedJobIntakeConfig(_db) {
  // Intake rules are DB-owned, mutable user policy. This initializer only
  // creates the schema; it must not reseed or overwrite live policy rows.
}

const DEFAULT_SOURCES = [
  { slug: 'linkedin',       label: 'LinkedIn',         sort_priority: 10 },
  { slug: 'indeed',         label: 'Indeed',           sort_priority: 20 },
  { slug: 'greenhouse',     label: 'Greenhouse',       sort_priority: 30 },
  { slug: 'glassdoor',      label: 'Glassdoor',        sort_priority: 40 },
  { slug: 'hitmarker',      label: 'Hitmarker',        sort_priority: 50 },
  { slug: 'workwithindies', label: 'Work With Indies', sort_priority: 60 },
  { slug: 'company',        label: 'Company site',     sort_priority: 70 },
  { slug: 'referral',       label: 'Referral',         sort_priority: 80 },
  { slug: 'other',          label: 'Other',            sort_priority: 99 },
];

function seedFoundViaSources(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS found_via_sources (
      slug          TEXT PRIMARY KEY,
      label         TEXT NOT NULL,
      sort_priority INTEGER DEFAULT 0
    )
  `);
  const insert = db.prepare(
    "INSERT OR IGNORE INTO found_via_sources (slug, label, sort_priority) VALUES (@slug, @label, @sort_priority)"
  );
  for (const row of DEFAULT_SOURCES) {
    insert.run(row);
  }
}

function seedResumeLayouts(db) {
  const layouts = [
    {
      layout_key: 'standard-1p',
      layout_family: 'grouped_standard',
      label: 'Standard One Page',
      page_count: 1,
      max_roles: 3,
      max_projects: 5,
      max_bullets_total: 8,
      max_bullets_per_role: 4,
      max_bullets_per_project: 3,
      max_direct_bullets_per_role: 1,
      skill_group_char_limit: 78,
      max_education_items: 2,
      max_certification_items: 2,
      certification_policy: 'contextual',
      section_order: 'summary,skills,experience,education,certifications',
      min_role_score: 0,
      min_project_score: 0,
      min_bullet_score: 0,
      allow_low_match_context: 0,
      approved: 1,
      notes: 'Default compact one-page layout budget.',
    },
    {
      layout_key: 'standard-2p',
      layout_family: 'grouped_standard',
      label: 'Standard Two Page',
      page_count: 2,
      max_roles: 4,
      max_projects: 8,
      max_bullets_total: 13,
      max_bullets_per_role: 5,
      max_bullets_per_project: 3,
      max_direct_bullets_per_role: 2,
      skill_group_char_limit: 90,
      max_education_items: 2,
      max_certification_items: 3,
      certification_policy: 'contextual',
      section_order: 'summary,skills,experience,education,certifications',
      min_role_score: 0,
      min_project_score: 0,
      min_bullet_score: 0,
      allow_low_match_context: 0,
      approved: 1,
      notes: 'Expanded layout for stronger multi-project fits.',
    },
    {
      layout_key: 'dense-1p',
      layout_family: 'grouped_standard',
      label: 'Dense One Page',
      page_count: 1,
      max_roles: 2,
      max_projects: 4,
      max_bullets_total: 7,
      max_bullets_per_role: 4,
      max_bullets_per_project: 2,
      max_direct_bullets_per_role: 1,
      skill_group_char_limit: 66,
      max_education_items: 1,
      max_certification_items: 2,
      certification_policy: 'contextual',
      section_order: 'summary,skills,experience,education,certifications',
      min_role_score: 8,
      min_project_score: 6,
      min_bullet_score: 4,
      allow_low_match_context: 0,
      approved: 1,
      notes: 'Aggressive one-page compression for crowded profiles.',
    },
    {
      layout_key: 'adjacent-1p',
      layout_family: 'grouped_standard',
      label: 'Adjacent One Page',
      page_count: 1,
      max_roles: 2,
      max_projects: 4,
      max_bullets_total: 6,
      max_bullets_per_role: 3,
      max_bullets_per_project: 2,
      max_direct_bullets_per_role: 1,
      skill_group_char_limit: 72,
      max_education_items: 2,
      max_certification_items: 2,
      certification_policy: 'contextual',
      section_order: 'summary,skills,experience,education,certifications',
      min_role_score: 10,
      min_project_score: 8,
      min_bullet_score: 6,
      allow_low_match_context: 0,
      approved: 1,
      notes: 'For adjacent/low-match roles where weaker context-only bullets should be suppressed.',
    },
    {
      layout_key: 'top-certs-1p',
      layout_family: 'grouped_top_certs',
      label: 'Top Certs One Page',
      page_count: 1,
      max_roles: 2,
      max_projects: 4,
      max_bullets_total: 7,
      max_bullets_per_role: 3,
      max_bullets_per_project: 2,
      max_direct_bullets_per_role: 1,
      skill_group_char_limit: 72,
      max_education_items: 1,
      max_certification_items: 2,
      certification_policy: 'top_relevant',
      section_order: 'summary,skills,certifications,experience,education',
      min_role_score: 8,
      min_project_score: 6,
      min_bullet_score: 4,
      allow_low_match_context: 0,
      approved: 1,
      notes: 'Grouped layout that surfaces top relevant certifications earlier.',
    },
    {
      layout_key: 'top-certs-2p',
      layout_family: 'grouped_top_certs',
      label: 'Top Certs Two Page',
      page_count: 2,
      max_roles: 3,
      max_projects: 6,
      max_bullets_total: 10,
      max_bullets_per_role: 4,
      max_bullets_per_project: 2,
      max_direct_bullets_per_role: 1,
      skill_group_char_limit: 84,
      max_education_items: 2,
      max_certification_items: 3,
      certification_policy: 'top_relevant',
      section_order: 'summary,skills,certifications,experience,education',
      min_role_score: 6,
      min_project_score: 4,
      min_bullet_score: 3,
      allow_low_match_context: 0,
      approved: 1,
      notes: 'Expanded grouped layout that surfaces top relevant certifications earlier.',
    },
  ];

  const profileRules = [];
  const existingProfiles = db.prepare('SELECT profile_key FROM resume_profiles WHERE approved = 1').all().map((row) => row.profile_key);
  for (const profileKey of existingProfiles) {
    profileRules.push(
      { profile_key: profileKey, layout_key: 'standard-1p', priority: 100, is_default: 1, approved: 1, notes: 'Primary default layout.' },
      { profile_key: profileKey, layout_key: 'standard-2p', priority: 80, is_default: 0, approved: 1, notes: 'Allowed expanded layout.' },
      { profile_key: profileKey, layout_key: 'dense-1p', priority: 70, is_default: 0, approved: 1, notes: 'Compact layout option.' },
      { profile_key: profileKey, layout_key: 'adjacent-1p', priority: 60, is_default: 0, approved: 1, notes: 'Use when fit is adjacent rather than direct.' },
      { profile_key: profileKey, layout_key: 'top-certs-1p', priority: 65, is_default: 0, approved: 1, notes: 'Cert-forward layout when requirements justify it.' },
      { profile_key: profileKey, layout_key: 'top-certs-2p', priority: 55, is_default: 0, approved: 1, notes: 'Expanded cert-forward layout when requirements justify it.' },
    );
  }

  const signalRules = [
    { layout_key: 'standard-1p', signal_key: 'direct_keyword_match_density', operator: 'gte', threshold_numeric: 0.35, threshold_text: null, weight: 20, action: 'score', approved: 1, notes: 'Compact one-page works best when direct match density is strong.' },
    { layout_key: 'standard-1p', signal_key: 'selected_project_count_candidate', operator: 'gte', threshold_numeric: 5, threshold_text: null, weight: 10, action: 'penalty', approved: 1, notes: 'Crowded project sets strain one-page layouts.' },
    { layout_key: 'standard-2p', signal_key: 'strong_project_count_candidate', operator: 'gte', threshold_numeric: 4, threshold_text: null, weight: 20, action: 'score', approved: 1, notes: 'Expanded layouts pay off with several strong projects.' },
    { layout_key: 'standard-2p', signal_key: 'requirement_count_matched', operator: 'gte', threshold_numeric: 6, threshold_text: null, weight: 15, action: 'score', approved: 1, notes: 'Good direct fits can support fuller two-page evidence.' },
    { layout_key: 'dense-1p', signal_key: 'selected_project_count_candidate', operator: 'gte', threshold_numeric: 4, threshold_text: null, weight: 15, action: 'score', approved: 1, notes: 'Use dense layout when content is broad but must stay compact.' },
    { layout_key: 'dense-1p', signal_key: 'weak_context_bullet_count_candidate', operator: 'gte', threshold_numeric: 2, threshold_text: null, weight: 10, action: 'penalty', approved: 1, notes: 'Dense one-page should avoid weak context-only filler.' },
    { layout_key: 'adjacent-1p', signal_key: 'requirement_count_unmatched', operator: 'gte', threshold_numeric: 3, threshold_text: null, weight: 25, action: 'score', approved: 1, notes: 'Adjacent layout is useful when direct requirement coverage is sparse.' },
    { layout_key: 'adjacent-1p', signal_key: 'weak_context_bullet_count_candidate', operator: 'gte', threshold_numeric: 3, threshold_text: null, weight: 15, action: 'score', approved: 1, notes: 'Adjacent layout should trigger extra suppression pressure.' },
    { layout_key: 'adjacent-1p', signal_key: 'direct_keyword_match_density', operator: 'lte', threshold_numeric: 0.2, threshold_text: null, weight: 10, action: 'score', approved: 1, notes: 'Prefer adjacent layout when direct matches are weak.' },
    { layout_key: 'top-certs-1p', signal_key: 'matched_cert_requirement_count', operator: 'gte', threshold_numeric: 1, threshold_text: null, weight: 30, action: 'score', approved: 1, notes: 'Prefer cert-forward layout when the role asks for certs and they are matched.' },
    { layout_key: 'top-certs-1p', signal_key: 'cert_sensitivity', operator: 'gte', threshold_numeric: 10, threshold_text: null, weight: 20, action: 'score', approved: 1, notes: 'Prefer cert-forward layout on strong cert-sensitive roles.' },
    { layout_key: 'top-certs-1p', signal_key: 'weak_context_bullet_count_candidate', operator: 'gte', threshold_numeric: 2, threshold_text: null, weight: 8, action: 'penalty', approved: 1, notes: 'Do not use cert-forward layout to preserve weak filler bullets.' },
    { layout_key: 'top-certs-2p', signal_key: 'matched_cert_requirement_count', operator: 'gte', threshold_numeric: 1, threshold_text: null, weight: 25, action: 'score', approved: 1, notes: 'Prefer expanded cert-forward layout when matched cert asks exist.' },
    { layout_key: 'top-certs-2p', signal_key: 'cert_sensitivity', operator: 'gte', threshold_numeric: 8, threshold_text: null, weight: 18, action: 'score', approved: 1, notes: 'Prefer expanded cert-forward layout on broader cert-sensitive roles.' },
  ];

  const insertLayout = db.prepare(`
    INSERT OR IGNORE INTO resume_layouts (
      layout_key, layout_family, label, page_count, max_roles, max_projects, max_bullets_total,
      max_bullets_per_role, max_bullets_per_project, max_direct_bullets_per_role,
      skill_group_char_limit, max_education_items, max_certification_items, certification_policy, section_order,
      min_role_score, min_project_score, min_bullet_score,
      allow_low_match_context, approved, notes
    )
    VALUES (
      @layout_key, @layout_family, @label, @page_count, @max_roles, @max_projects, @max_bullets_total,
      @max_bullets_per_role, @max_bullets_per_project, @max_direct_bullets_per_role,
      @skill_group_char_limit, @max_education_items, @max_certification_items, @certification_policy, @section_order,
      @min_role_score, @min_project_score, @min_bullet_score,
      @allow_low_match_context, @approved, @notes
    )
  `);
  const insertProfileRule = db.prepare(`
    INSERT OR IGNORE INTO resume_layout_profile_rules (profile_key, layout_key, priority, is_default, approved, notes)
    VALUES (@profile_key, @layout_key, @priority, @is_default, @approved, @notes)
  `);
  const insertSignalRule = db.prepare(`
    INSERT OR IGNORE INTO resume_layout_signal_rules (
      layout_key, signal_key, operator, threshold_numeric, threshold_text, weight, action, approved, notes
    )
    VALUES (
      @layout_key, @signal_key, @operator, @threshold_numeric, @threshold_text, @weight, @action, @approved, @notes
    )
  `);

  for (const layout of layouts) {
    insertLayout.run(layout);
  }
  for (const rule of profileRules) {
    insertProfileRule.run(rule);
  }
  for (const rule of signalRules) {
    insertSignalRule.run(rule);
  }
}

function seedResumeSkillRules(db) {
  const groupDefinitions = {
    'programming': { label: 'Programming', group_rank: 10 },
    'engines': { label: 'Engines', group_rank: 20 },
    'backend-cloud': { label: 'Backend & Cloud', group_rank: 30 },
    'networking': { label: 'Networking', group_rank: 40 },
    'security': { label: 'Security', group_rank: 50 },
    'frontend': { label: 'Frontend', group_rank: 60 },
    'data': { label: 'Data', group_rank: 65 },
    'xr-platforms': { label: 'Platforms', group_rank: 70 },
    'tools': { label: 'Tools', group_rank: 80 },
    'spoken_languages': { label: 'Languages', group_rank: 90 },
  };

  const defaultRenderGroup = (category) => {
    const normalized = String(category || '').trim().toLowerCase();
    if (normalized === 'language') return 'programming';
    if (normalized === 'engine') return 'engines';
    if (normalized === 'backend' || normalized === 'cloud') return 'backend-cloud';
    if (normalized === 'networking') return 'networking';
    if (normalized === 'platform') return 'xr-platforms';
    if (normalized === 'security') return 'security';
    if (normalized === 'frontend') return 'frontend';
    if (normalized === 'data') return 'data';
    if (normalized === 'spoken-language') return 'spoken_languages';
    return 'tools';
  };

  const profileKeys = db.prepare('SELECT profile_key FROM resume_profiles WHERE approved = 1').all().map((row) => row.profile_key);
  const skills = db.prepare(`
    SELECT skill_name, skill_normalized, category, include_default, require_direct_match, resume_visibility, resume_emphasis, resume_priority, resume_group_rank
    FROM skills_mine
    ORDER BY skill_name
  `).all();

  const insertGroupRule = db.prepare(`
    INSERT OR IGNORE INTO resume_group_rules (profile_key, group_id, label, group_rank, min_items_standalone, singleton_merge_target, max_items, approved)
    VALUES (@profile_key, @group_id, @label, @group_rank, 1, NULL, 8, 1)
  `);
  for (const profileKey of profileKeys) {
    for (const [groupId, group] of Object.entries(groupDefinitions)) {
      insertGroupRule.run({
        profile_key: profileKey,
        group_id: groupId,
        label: group.label,
        group_rank: group.group_rank,
      });
    }
  }

  const insertSkillRule = db.prepare(`
    INSERT OR IGNORE INTO skill_resume_rules (skill_normalized, profile_key, render_group, group_rank, item_rank, visibility, emphasis, direct_match_boost, singleton_penalty, trigger_condition, notes, approved)
    VALUES (@skill_normalized, @profile_key, @render_group, @group_rank, @item_rank, @visibility, @emphasis, @direct_match_boost, @singleton_penalty, NULL, @notes, 1)
  `);
  for (const profileKey of profileKeys) {
    for (const skill of skills) {
      const renderGroup = defaultRenderGroup(skill.category);
      const visibility = skill.resume_visibility || (skill.require_direct_match ? 'context' : 'show');
      const singletonPenalty = visibility === 'show' ? 0 : (visibility === 'context' ? 3 : 8);
      insertSkillRule.run({
        skill_normalized: skill.skill_normalized || normalizeSkill(skill.skill_name),
        profile_key: profileKey,
        render_group: renderGroup,
        group_rank: skill.resume_group_rank || (groupDefinitions[renderGroup]?.group_rank || 80),
        item_rank: skill.resume_priority || 0,
        visibility,
        emphasis: skill.resume_emphasis || 'plain',
        direct_match_boost: skill.require_direct_match ? 6 : 0,
        singleton_penalty: singletonPenalty,
        notes: 'DB-generated generic resume display rule.',
      });
    }
  }
}

// ──────────────────────────────────────────────────────────
// Seed: skills_mine
// ──────────────────────────────────────────────────────────
function seedSkills(db) {
  const skills = [
    { skill_name: 'C#',               category: 'language',    display_category: 'Languages',             secondary_categories: null,                               resume_priority: 10, include_default: 1, require_direct_match: 0, profile_bias: null, resume_visibility: 'show', resume_emphasis: 'primary', resume_group_rank: 100, level: 'expert',        evidence: '' },
    { skill_name: 'C',                category: 'language',    display_category: 'Languages',             secondary_categories: null,                               resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, resume_visibility: 'hidden',  resume_emphasis: 'plain', resume_group_rank: 60, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Unity',            category: 'engine',      display_category: 'Engines',               secondary_categories: 'Platforms & Engines',              resume_priority: 10, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'expert',        evidence: '' },
    { skill_name: 'Azure Functions',  category: 'cloud',       display_category: 'Cloud',                 secondary_categories: 'Cloud / Backend',                  resume_priority: 9, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'advanced',      evidence: '' },
    { skill_name: 'PlayFab',          category: 'cloud',       display_category: 'Cloud',                 secondary_categories: 'Cloud / Backend',                  resume_priority: 9, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'advanced',      evidence: '' },
    { skill_name: 'REST APIs',        category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 9, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'advanced',      evidence: '' },
    { skill_name: 'Git',              category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools',                  resume_priority: 8, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'advanced',      evidence: '' },
    { skill_name: 'Photoshop',        category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Creative Tools',                   level: 'advanced',      evidence: '' },
    { skill_name: 'Audacity',         category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Creative Tools',                   level: 'intermediate',  evidence: '' },
    { skill_name: 'Photon PUN',       category: 'networking',  display_category: 'Networking',            secondary_categories: 'Networking & Security|Platforms & Engines', resume_priority: 7, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'intermediate',  evidence: '' },
    { skill_name: 'Redis',            category: 'cloud',       display_category: 'Cloud',                 secondary_categories: 'Cloud / Backend',                  resume_priority: 8, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'intermediate',  evidence: '' },
    { skill_name: 'Cosmos DB',        category: 'cloud',       display_category: 'Cloud',                 secondary_categories: 'Cloud / Backend|Data',             resume_priority: 7, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'intermediate',  evidence: '' },
    { skill_name: 'JSON',             category: 'data',        display_category: 'Data',                  secondary_categories: 'Backend|Cloud / Backend',          level: 'expert',        evidence: '' },
    { skill_name: 'SQL',              category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend|Data',             resume_priority: 8, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'intermediate',  evidence: 'used across multiple projects' },
    { skill_name: 'Python',           category: 'language',    display_category: 'Languages',             secondary_categories: 'Tools',                            resume_priority: 5, include_default: 1, require_direct_match: 0, profile_bias: null, resume_visibility: 'show', resume_emphasis: 'experience', resume_group_rank: 90, level: 'basic',         evidence: '' },
    { skill_name: 'Java',             category: 'language',    display_category: 'Languages',             secondary_categories: null,                               resume_priority: 4, include_default: 0, require_direct_match: 0, profile_bias: null, resume_visibility: 'show', resume_emphasis: 'experience', resume_group_rank: 80, level: 'basic',         evidence: '' },
    { skill_name: 'Lua',              category: 'language',    display_category: 'Languages',             secondary_categories: null,                               resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'exposure',      evidence: '' },
    { skill_name: 'C++',              category: 'language',    display_category: 'Languages',             secondary_categories: 'Platforms & Engines',              resume_priority: 5, include_default: 0, require_direct_match: 0, profile_bias: null, resume_visibility: 'show', resume_emphasis: 'experience', resume_group_rank: 70, level: 'basic',         evidence: '' },
    { skill_name: 'Luau',             category: 'language',    display_category: 'Languages',             secondary_categories: 'Roblox',                           resume_priority: 4, include_default: 0, require_direct_match: 1, profile_bias: null, resume_visibility: 'context', resume_emphasis: 'plain', resume_group_rank: 20, level: 'basic',         evidence: '' },
    { skill_name: 'GDScript',         category: 'language',    display_category: 'Languages',             secondary_categories: 'Platforms & Engines',              resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, resume_visibility: 'context', resume_emphasis: 'plain', resume_group_rank: 10, level: 'basic',         evidence: '' },
    { skill_name: 'HTML/CSS',         category: 'frontend',    display_category: 'Frontend',              secondary_categories: 'Tools',                            resume_priority: 4, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'intermediate',  evidence: 'comfortable with markup and styling work when needed' },
    { skill_name: 'English',          category: 'spoken-language', display_category: 'Spoken Languages',  secondary_categories: null,                               level: 'expert',        evidence: 'primary working language' },
    { skill_name: 'Korean',           category: 'spoken-language', display_category: 'Spoken Languages',  secondary_categories: null,                               level: 'advanced',      evidence: 'advanced spoken proficiency' },
    { skill_name: 'Spanish',          category: 'spoken-language', display_category: 'Spoken Languages',  secondary_categories: null,                               level: 'intermediate',  evidence: 'intermediate spoken proficiency' },
    { skill_name: 'Portuguese',       category: 'spoken-language', display_category: 'Spoken Languages',  secondary_categories: null,                               level: 'basic',         evidence: 'basic spoken proficiency' },
    { skill_name: 'Roblox Studio',    category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Roblox|Platforms & Engines',       resume_priority: 4, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'intermediate',  evidence: '' },
    { skill_name: 'GitHub Actions',   category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools|Cloud / Backend',  resume_priority: 6, include_default: 0, require_direct_match: 0, profile_bias: null, level: 'intermediate',  evidence: 'CI/CD pipelines' },
    { skill_name: 'Linux',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools|Networking & Security', resume_priority: 5, include_default: 0, require_direct_match: 0, profile_bias: null, level: 'intermediate', evidence: '' },
    { skill_name: 'TCP/IP',           category: 'networking',  display_category: 'Networking',            secondary_categories: 'Networking & Security',            resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: '' },
    { skill_name: 'Cisco IOS',        category: 'networking',  display_category: 'Networking',            secondary_categories: 'Networking & Security',            resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: 'cert coursework' },
    { skill_name: 'Wireshark',        category: 'security',    display_category: 'Security',              secondary_categories: 'Networking & Security',            resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: 'used in cybersecurity/networking labs' },
    { skill_name: 'Nmap',             category: 'security',    display_category: 'Security',              secondary_categories: 'Networking & Security',            resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: 'used in cybersecurity/networking labs' },
    { skill_name: 'Autopsy',          category: 'security',    display_category: 'Security',              secondary_categories: 'Networking & Security',            resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: 'used in digital forensics labs' },
    { skill_name: 'FTK',              category: 'security',    display_category: 'Security',              secondary_categories: 'Networking & Security',            resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: 'used in digital forensics labs' },
    { skill_name: 'Packet Tracer',    category: 'networking',  display_category: 'Networking',            secondary_categories: 'Networking & Security',            resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: 'used in networking coursework' },
    { skill_name: 'Docker',           category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 5, include_default: 0, require_direct_match: 0, profile_bias: null, level: 'basic',         evidence: '' },
    { skill_name: 'Jenkins',          category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools|Cloud / Backend',  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'exposure',      evidence: 'interacted with it but did not build or implement pipelines directly' },
    { skill_name: 'GitLab CI',        category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools|Cloud / Backend',  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'exposure',      evidence: 'interacted with it but did not build or implement pipelines directly' },
    { skill_name: 'Google Cloud Build', category: 'tool',      display_category: 'Tools',                 secondary_categories: 'Developer Tools|Cloud / Backend',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'exposure',      evidence: 'interacted with it but did not build or implement pipelines directly' },
    { skill_name: 'NUnit',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools',                  resume_priority: 4, include_default: 0, require_direct_match: 0, profile_bias: null, level: 'basic',         evidence: '' },
    { skill_name: 'Unreal Engine',    category: 'engine',      display_category: 'Engines',               secondary_categories: 'Platforms & Engines',              resume_priority: 5, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'intermediate',  evidence: '' },
    { skill_name: 'Godot',            category: 'engine',      display_category: 'Engines',               secondary_categories: 'Platforms & Engines',              resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'intermediate',  evidence: '' },
    { skill_name: 'ScriptableObjects', category: 'engine',     display_category: 'Engines',               secondary_categories: 'Platforms & Engines',              resume_priority: 6, include_default: 0, require_direct_match: 0, profile_bias: null, resume_visibility: 'hidden', resume_emphasis: 'plain', resume_group_rank: 0, level: 'advanced',      evidence: '' },
    { skill_name: 'Socket.IO',        category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'exposure',      evidence: 'interacted with it for real-time event handling but did not build the underlying implementation myself' },
    { skill_name: 'Meta Quest',       category: 'platform',    display_category: 'Platforms',             secondary_categories: 'Platforms & Engines|XR',           resume_priority: 8, include_default: 1, require_direct_match: 0, profile_bias: null, level: 'advanced',      evidence: '' },
    { skill_name: 'PSVR2',            category: 'platform',    display_category: 'Platforms',             secondary_categories: 'Platforms & Engines|XR',           resume_priority: 6, include_default: 0, require_direct_match: 0, profile_bias: null, level: 'intermediate',  evidence: '' },
    { skill_name: 'HTC Vive',         category: 'platform',    display_category: 'Platforms',             secondary_categories: 'Platforms & Engines|XR',           resume_priority: 6, include_default: 0, require_direct_match: 0, profile_bias: null, level: 'intermediate',  evidence: '' },
    { skill_name: 'HoloLens',         category: 'platform',    display_category: 'Platforms',             secondary_categories: 'Platforms & Engines|XR',           resume_priority: 4, include_default: 0, require_direct_match: 0, profile_bias: null, level: 'basic',         evidence: '' },
    { skill_name: 'Wwise',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Platforms & Engines',              resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: '' },
    { skill_name: 'Windows Server',   category: 'networking',  display_category: 'Networking',            secondary_categories: 'Networking & Security',            resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: 'CCNA coursework' },
    { skill_name: 'Perforce',         category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools',                  resume_priority: 5, include_default: 0, require_direct_match: 0, profile_bias: null, level: 'intermediate',  evidence: '' },
    { skill_name: 'Ethical Hacking',  category: 'security',    display_category: 'Security',              secondary_categories: 'Networking & Security',            resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: 'coursework' },
    { skill_name: 'Digital Forensics', category: 'security',   display_category: 'Security',              secondary_categories: 'Networking & Security',            resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'basic',         evidence: 'coursework' },
    { skill_name: 'AWS',              category: 'cloud',       display_category: 'Cloud',                 secondary_categories: 'Cloud / Backend',                  resume_priority: 6, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'frequent target-role requirement; no hands-on evidence recorded yet' },
    { skill_name: 'Azure',            category: 'cloud',       display_category: 'Cloud',                 secondary_categories: 'Cloud / Backend',                  resume_priority: 5, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'frequent target-role requirement; no hands-on evidence recorded yet' },
    { skill_name: 'GCP',              category: 'cloud',       display_category: 'Cloud',                 secondary_categories: 'Cloud / Backend',                  resume_priority: 4, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Kubernetes',       category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 6, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'common target-role requirement; no hands-on evidence recorded yet' },
    { skill_name: 'Terraform',        category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 4, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'common infrastructure requirement; no hands-on evidence recorded yet' },
    { skill_name: 'Helm',             category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'ArgoCD',           category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Flux',             category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'OpenShift',        category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Ansible',          category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'CloudFormation',   category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Packer',           category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: '.NET',             category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'ASP.NET',          category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'ASP.NET Core',     category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'ASP.NET MVC',      category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'React',            category: 'frontend',    display_category: 'Frontend',              secondary_categories: 'Frontend|Cloud / Backend',         resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Node.js',          category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Spring Boot',      category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'GraphQL',          category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend|Frontend',         resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Go',               category: 'language',    display_category: 'Languages',             secondary_categories: null,                               resume_priority: 4, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Kotlin',           category: 'language',    display_category: 'Languages',             secondary_categories: null,                               resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'JavaScript',       category: 'language',    display_category: 'Languages',             secondary_categories: 'Frontend',                         resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'TypeScript',       category: 'language',    display_category: 'Languages',             secondary_categories: 'Frontend',                         resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Groovy',           category: 'language',    display_category: 'Languages',             secondary_categories: null,                               resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',    evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Maven',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools',                  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Gradle',           category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools',                  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'JUnit',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools',                  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Mockito',          category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools',                  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'WebSockets',       category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'gRPC',             category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 3, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'NoSQL',            category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend|Data',             resume_priority: 4, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'common target-role requirement; no hands-on evidence recorded yet' },
    { skill_name: 'Shaders',          category: 'graphics',    display_category: 'Graphics',              secondary_categories: 'Graphics',                         resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in graphics/rendering roles, but no hands-on evidence recorded yet' },
    { skill_name: 'MySQL',            category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend|Data',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'MongoDB',          category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend|Data',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Kafka',            category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend|Data',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Spark',            category: 'data',        display_category: 'Data',                  secondary_categories: 'Cloud / Backend|Data',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Airflow',          category: 'data',        display_category: 'Data',                  secondary_categories: 'Cloud / Backend|Data',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Snowflake',        category: 'data',        display_category: 'Data',                  secondary_categories: 'Cloud / Backend|Data',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'BigQuery',         category: 'data',        display_category: 'Data',                  secondary_categories: 'Cloud / Backend|Data',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Redshift',         category: 'data',        display_category: 'Data',                  secondary_categories: 'Cloud / Backend|Data',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Prometheus',       category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Grafana',          category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Splunk',           category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Datadog',          category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'New Relic',        category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'PyTorch',          category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Data|Developer Tools',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in ML-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'TensorFlow',       category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Data|Developer Tools',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in ML-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'WPF',              category: 'frontend',    display_category: 'Frontend',              secondary_categories: 'Frontend|Developer Tools',         resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in desktop-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Chef',             category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'RenderDoc',        category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Graphics|Developer Tools',         resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in graphics-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Nsight',           category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Graphics|Developer Tools',         resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in graphics-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Vault',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in security-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'VMware',           category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Cloud / Backend|Developer Tools',  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in infrastructure-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'VxWorks',          category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Systems|Developer Tools',          resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, resume_visibility: 'hidden', level: 'none',          evidence: 'appears in embedded-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Polars',           category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Data|Developer Tools',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in data/ML-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'LangChain',        category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Data|Developer Tools',             resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in LLM-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Cameo',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Systems|Developer Tools',          resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in systems-modeling target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'DoDAF',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Systems|Developer Tools',          resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in defense and systems-modeling target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'DOORS',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Systems|Developer Tools',          resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in systems-modeling target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Android',          category: 'platform',    display_category: 'Platforms',             secondary_categories: 'Mobile',                           resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',       evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'CloudXR',          category: 'tool',        display_category: 'Tools',                 secondary_categories: 'XR|Developer Tools',               resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',         evidence: 'appears in XR and robotics target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Tenable',          category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Security|Developer Tools',         resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in security-oriented target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Electron',         category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Frontend|Developer Tools',         resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'FastAPI',          category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'AWS ElastiCache',  category: 'cloud',       display_category: 'Cloud',                 secondary_categories: 'Cloud / Backend',                  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'AWS ELB',          category: 'cloud',       display_category: 'Cloud',                 secondary_categories: 'Cloud / Backend',                  resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Ekahau',           category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Networking & Security|Developer Tools', resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in wireless-networking target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Cisco Wireless',   category: 'networking',  display_category: 'Networking',            secondary_categories: 'Networking & Security',            resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in wireless-networking target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Jest',             category: 'tool',        display_category: 'Tools',                 secondary_categories: 'Developer Tools|Frontend',         resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Ionic',            category: 'frontend',    display_category: 'Frontend',              secondary_categories: 'Mobile|Frontend',                 resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in hybrid mobile target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Informatica PowerCenter', category: 'data', display_category: 'Data',                secondary_categories: 'Data|Developer Tools',            resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in data-platform target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Isaac',            category: 'tool',        display_category: 'Tools',                 secondary_categories: 'XR|Developer Tools|Simulation',   resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in NVIDIA robotics/XR target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'J2EE',             category: 'backend',     display_category: 'Backend',               secondary_categories: 'Cloud / Backend',                 resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in Java enterprise target roles, but no hands-on evidence recorded yet' },
    { skill_name: 'Erlang',           category: 'language',    display_category: 'Languages',             secondary_categories: null,                               resume_priority: 2, include_default: 0, require_direct_match: 1, profile_bias: null, level: 'none',          evidence: 'appears in target roles, but no hands-on evidence recorded yet' },
  ];

  const stmt = db.prepare(`
    INSERT INTO skills_mine (skill_name, skill_normalized, category, display_category, secondary_categories, resume_priority, include_default, require_direct_match, profile_bias, resume_visibility, resume_display, resume_emphasis, resume_group_rank, level, evidence)
    VALUES (@skill_name, @skill_normalized, @category, @display_category, @secondary_categories, @resume_priority, @include_default, @require_direct_match, @profile_bias, @resume_visibility, @resume_display, @resume_emphasis, @resume_group_rank, @level, @evidence)
    ON CONFLICT(skill_normalized) DO UPDATE SET
      skill_name = excluded.skill_name,
      category = excluded.category,
      display_category = excluded.display_category,
      secondary_categories = excluded.secondary_categories,
      resume_priority = excluded.resume_priority,
      include_default = excluded.include_default,
      require_direct_match = excluded.require_direct_match,
      profile_bias = excluded.profile_bias,
      resume_visibility = excluded.resume_visibility,
      resume_display = excluded.resume_display,
      resume_emphasis = excluded.resume_emphasis,
      resume_group_rank = excluded.resume_group_rank,
      level = excluded.level,
      evidence = excluded.evidence
  `);
  for (const s of skills) {
    stmt.run({
      resume_priority: 5,
      include_default: 1,
      require_direct_match: 0,
      profile_bias: null,
      resume_visibility: 'show',
      resume_display: null,
      resume_emphasis: 'plain',
      resume_group_rank: 0,
      ...s,
      skill_normalized: s.skill_name.toLowerCase().trim(),
    });
  }

  const validKeys = skills.map((s) => normalizeSkill(s.skill_name));
  const placeholders = validKeys.map(() => '?').join(', ');
  db.prepare(`DELETE FROM skills_mine WHERE skill_normalized NOT IN (${placeholders})`).run(...validKeys);
}

function seedCapabilities(db) {
  // Capabilities are personal and populated via the CLI (log_job.py --set-capability).
  // Do not seed user-specific capabilities here.
}

// ──────────────────────────────────────────────────────────
// Seed: qualifications
// ──────────────────────────────────────────────────────────
function seedQualifications(db) {
  // Qualifications are personal and populated via the CLI (log_job.py --set-qualification).
  // Do not seed user-specific qualifications here.
}

// ──────────────────────────────────────────────────────────
// Seed: certifications
// ──────────────────────────────────────────────────────────
function seedCertifications(db) {
  // Certifications are populated by scripts/local/personal-seeds.mjs
}

// ──────────────────────────────────────────────────────────
// Seed: education
// ──────────────────────────────────────────────────────────
function seedEducation(db) {
  // Education records are populated by scripts/local/personal-seeds.mjs
}

// ──────────────────────────────────────────────────────────
// Seed: archetypes (ensure all referenced archetypes exist before stories)
// ──────────────────────────────────────────────────────────
function seedArchetypes(db) {
  // Archetypes are populated from config/profile.yml via sync_profile_archetypes.py.
  // Do not seed user-specific archetypes here.
}

// ──────────────────────────────────────────────────────────
// Seed: stories + story_archetypes
// ──────────────────────────────────────────────────────────
function seedStories(db) {
  // Interview stories are populated by scripts/local/personal-seeds.mjs
}

// ──────────────────────────────────────────────────────────
// Parse skill-gap-cache.md -> archetypes + skill_signals
// ──────────────────────────────────────────────────────────
function parseSkillSignals(db) {
  if (!existsSync(SKILL_GAP_PATH)) {
    console.log('  skill-gap-cache.md not found, skipping signal parse.');
    return;
  }

  const raw = readFileSync(SKILL_GAP_PATH, 'utf8');

  // Split into sections by "---" lines; each section may contain one archetype block
  const sections = raw.split(/^---\s*$/m);

  const insertArc = db.prepare('INSERT OR IGNORE INTO archetypes (name, evals) VALUES (?, 0)');
  const updateEvals = db.prepare('UPDATE archetypes SET evals = ? WHERE name = ?');
  const getArcId = db.prepare('SELECT id, evals FROM archetypes WHERE name = ?');

  const upsertSignal = db.prepare(`
    INSERT INTO skill_signals (archetype_id, type, skill_name, skill_normalized, count)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(archetype_id, type, skill_normalized) DO UPDATE SET count = excluded.count
  `);

  for (const section of sections) {
    const lines = section.split('\n').map(l => l.trim()).filter(Boolean);

    // Find ## heading (archetype name)
    const headingLine = lines.find(l => /^##\s+/.test(l));
    if (!headingLine) continue;
    const arcName = headingLine.replace(/^##\s+/, '').trim();
    if (!arcName) continue;

    // Extract evals count from comment <!-- Evals: N -->
    let evals = 0;
    const evalsComment = lines.find(l => /<!--\s*Evals:\s*\d+/.test(l));
    if (evalsComment) {
      const m = evalsComment.match(/Evals:\s*(\d+)/);
      if (m) evals = parseInt(m[1], 10);
    }

    // Find matched: and missing: lines
    const matchedLine = lines.find(l => /^matched:/.test(l));
    const missingLine = lines.find(l => /^missing:/.test(l));

    if (!matchedLine && !missingLine) continue;

    // Ensure archetype exists
    insertArc.run(arcName);
    const arcRow = getArcId.get(arcName);
    if (!arcRow) continue;
    const arcId = arcRow.id;

    // Update evals if parsed value is higher
    if (evals > arcRow.evals) {
      updateEvals.run(evals, arcName);
    }

    const parseSkillList = (line) => {
      if (!line) return [];
      const content = line.replace(/^(matched|missing):\s*/, '');
      // Split by comma, then clean each entry
      return content.split(',').map(entry => {
        const raw = entry.trim();
        if (!raw) return null;
        // Extract count if present e.g. "skill (3x)" or "skill (3x — comment)"
        let count = 1;
        const countMatch = raw.match(/\((\d+)x/);
        if (countMatch) count = parseInt(countMatch[1], 10);
        // Normalize: strip count annotation and any comment after em-dash
        const normalized = normalizeSkill(raw);
        // Use the raw name with count stripped but preserve casing for display
        const displayName = raw.replace(/\s*\(\d+x[^)]*\)/g, '').replace(/\s*--.*$/, '').trim();
        if (!normalized) return null;
        return { displayName, normalized, count };
      }).filter(Boolean);
    };

    const matchedSkills = parseSkillList(matchedLine);
    const missingSkills = parseSkillList(missingLine);

    for (const { displayName, normalized, count } of matchedSkills) {
      upsertSignal.run(arcId, 'matched', displayName, normalized, count);
    }
    for (const { displayName, normalized, count } of missingSkills) {
      upsertSignal.run(arcId, 'missing', displayName, normalized, count);
    }
  }
}

// ──────────────────────────────────────────────────────────
// Parse applications.md / evaluated.md -> roles
// ──────────────────────────────────────────────────────────
function parseRolesFromFile(filePath, db, matcherEntries) {
  if (!existsSync(filePath)) return 0;
  const raw = readFileSync(filePath, 'utf8');
  const lines = raw.split('\n');

  const stmt = db.prepare(`
    INSERT INTO roles (
      num, date, first_seen_date, last_updated_date, company, role,
      location_text, work_model, compensation_text, salary_min, salary_max, salary_currency, salary_period,
      score, status, pdf, report, source, found_via, notes
    )
    VALUES (
      @num, @date, @first_seen_date, @last_updated_date, @company, @role,
      @location_text, @work_model, @compensation_text, @salary_min, @salary_max, @salary_currency, @salary_period,
      @score, @status, @pdf, @report, @source, @found_via, @notes
    )
    ON CONFLICT(company, role) DO UPDATE SET
      date = COALESCE(roles.date, excluded.date),
      first_seen_date = COALESCE(roles.first_seen_date, excluded.first_seen_date, excluded.date),
      last_updated_date = CASE
        WHEN roles.last_updated_date IS NULL THEN COALESCE(excluded.last_updated_date, excluded.date)
        WHEN excluded.last_updated_date IS NULL THEN roles.last_updated_date
        WHEN excluded.last_updated_date > roles.last_updated_date THEN excluded.last_updated_date
        ELSE roles.last_updated_date
      END,
      location_text = COALESCE(excluded.location_text, roles.location_text),
      work_model = COALESCE(excluded.work_model, roles.work_model),
      compensation_text = COALESCE(excluded.compensation_text, roles.compensation_text),
      salary_min = COALESCE(excluded.salary_min, roles.salary_min),
      salary_max = COALESCE(excluded.salary_max, roles.salary_max),
      salary_currency = COALESCE(excluded.salary_currency, roles.salary_currency),
      salary_period = COALESCE(excluded.salary_period, roles.salary_period),
      score = COALESCE(excluded.score, roles.score),
      status = COALESCE(excluded.status, roles.status),
      pdf = COALESCE(excluded.pdf, roles.pdf),
      report = COALESCE(excluded.report, roles.report),
      source = COALESCE(excluded.source, roles.source),
      found_via = COALESCE(excluded.found_via, roles.found_via),
      notes = COALESCE(excluded.notes, roles.notes)
  `);
  const getRoleId = db.prepare('SELECT num, role, notes, location_text, work_model, compensation_text, salary_min, salary_max FROM roles WHERE company = ? AND role = ?');

  let count = 0;
  for (const line of lines) {
    // Must be a table data row: starts and ends with |, not a header/separator
    if (!line.trim().startsWith('|')) continue;
    if (/^\s*\|[-\s|]+\|\s*$/.test(line)) continue; // separator row

    const cells = line.split('|').map(c => c.trim()).filter(c => c !== '');
    if (cells.length < 11) continue;

    const [numRaw, date, company, role, scoreRaw, status, pdfRaw, report, source, via, ...notesParts] = cells;

    // Skip if num is not numeric (header row like "# | Date | ...")
    const num = parseInt(numRaw, 10);
    if (isNaN(num)) continue;

    // Parse score: "7.2/10" -> 7.2, "—" or "" -> null
    let score = null;
    if (scoreRaw && scoreRaw !== '—' && scoreRaw !== '-') {
      const scoreMatch = scoreRaw.match(/(\d+\.?\d*)/);
      if (scoreMatch) score = parseFloat(scoreMatch[1]);
    }

    // pdf: ✅ -> 1, ❌ -> 0, else null
    let pdf = null;
    if (pdfRaw === '✅') pdf = 1;
    else if (pdfRaw === '❌') pdf = 0;

    const notes = notesParts.join(' | ').trim();
    const metadata = parseRoleMetadata(notes);
    const roleRecord = {
      role: role || null,
      work_model: metadata.work_model,
      salary_min: metadata.salary_min,
      salary_max: metadata.salary_max,
    };

    const derivedScore = score ?? null;

    stmt.run({
      num,
      date:    date    || null,
      first_seen_date: date || null,
      last_updated_date: date || null,
      company: company || null,
      role:    role    || null,
      location_text: metadata.location_text,
      work_model: metadata.work_model,
      compensation_text: metadata.compensation_text,
      salary_min: metadata.salary_min,
      salary_max: metadata.salary_max,
      salary_currency: metadata.salary_currency,
      salary_period: metadata.salary_period,
      score: derivedScore,
      status:  status  || null,
      pdf,
      report:  report  !== '—' ? report : null,
      source:  source  !== '—' ? source : null,
      via:     via     !== '—' ? via    : null,
      notes:   notes   || null,
    });

    const stored = getRoleId.get(company || null, role || null);
    if (stored && matcherEntries) {
      const primaryArchetype = inferPrimaryArchetype(db, stored.role, stored.notes);
      const matchedRows = buildRoleRequirements(db, stored.num, stored.role, stored.notes, matcherEntries, primaryArchetype);
      scoreRoleArchetypes(db, stored.num, stored, matchedRows);
      if (stored.score == null) {
        const nextScore = deriveRoleScore(
          {
            role: stored.role,
            work_model: stored.work_model,
            salary_min: stored.salary_min,
            salary_max: stored.salary_max,
          },
          matchedRows.filter((row) => row.matched_entity_type !== 'none'),
          stored.notes,
        );
        db.prepare('UPDATE roles SET score = ? WHERE num = ?').run(nextScore, stored.num);
      }
    }
    count++;
  }
  return count;
}

function parseRoles(db) {
  const matcherEntries = loadRequirementMatcher(db);
  const n1 = parseRolesFromFile(APPLICATIONS_PATH, db, matcherEntries);
  const n2 = parseRolesFromFile(EVALUATED_PATH, db, matcherEntries);
  console.log(`  Parsed ${n1} rows from applications.md, ${n2} rows from evaluated.md`);
}

// ──────────────────────────────────────────────────────────
// Seed: question bank
// ──────────────────────────────────────────────────────────
function seedQuestionBank(db) {
  const ins = db.prepare(`
    INSERT INTO question_bank (slug, prompt, answer, tags, adapt, approved)
    VALUES (?, ?, ?, ?, ?, 1)
    ON CONFLICT(slug) DO NOTHING
  `);
  const questions = [
    {
      slug: 'tell-me-about-yourself',
      prompt: 'Tell me about yourself / Walk me through your background',
      tags: 'intro|universal',
      adapt: 1,
      answer: `[Your answer here — describe your background, years of experience, key technologies, and what you are looking for.]`,
    },
    {
      slug: 'challenge',
      prompt: 'Tell me about a challenge you faced at work and how you handled it',
      tags: 'behavioral|universal',
      adapt: 0,
      answer: `[Your STAR-format answer here — Situation, Task, Action, Result.]`,
    },
    {
      slug: 'leadership',
      prompt: 'Tell me about a time you led a project or team',
      tags: 'behavioral|leadership',
      adapt: 0,
      answer: `[Your STAR-format answer about a leadership or ownership experience.]`,
    },
    {
      slug: 'strength',
      prompt: 'What is your greatest strength?',
      tags: 'soft-skill|universal',
      adapt: 1,
      answer: `[Describe your primary strength with a concrete example.]`,
    },
    {
      slug: 'why-games',
      prompt: 'Why games / Why this industry?',
      tags: 'motivation|games',
      adapt: 0,
      answer: `[Explain your genuine motivation for working in games or this specific industry.]`,
    },
    {
      slug: 'technical-depth',
      prompt: 'Tell me about a technically complex system you built',
      tags: 'technical|systems',
      adapt: 0,
      answer: `[Describe a system you designed and built — focus on architecture decisions and trade-offs.]`,
    },
    {
      slug: 'collab-conflict',
      prompt: 'Tell me about a time you disagreed with a teammate or had a conflict',
      tags: 'behavioral|collaboration',
      adapt: 0,
      answer: `[STAR-format answer about a professional disagreement and how you resolved it.]`,
    },
    {
      slug: 'testing',
      prompt: 'How do you approach testing and quality?',
      tags: 'technical|process',
      adapt: 0,
      answer: `[Describe your testing philosophy and how you applied it in production contexts.]`,
    },
    {
      slug: 'career-goals',
      prompt: 'Where do you see yourself in 3-5 years?',
      tags: 'motivation|soft-skill',
      adapt: 1,
      answer: `[Describe your career trajectory and growth goals in a way that connects to the role.]`,
    },
    {
      slug: 'why-leaving',
      prompt: 'Why are you leaving your current role / why are you looking?',
      tags: 'motivation|universal',
      adapt: 0,
      answer: `[Honest, professional answer about your current situation and what you are looking for next.]`,
    },
  ];
  for (const q of questions) {
    const info = ins.run(q.slug, q.prompt, q.answer, q.tags, q.adapt ? 1 : 0);
    n += info.changes;
  }
  console.log(`  Seeded ${n} questions (skipped existing)`);
}

function copyTemplateDbIfMissing(dbPath, templatePath) {
  if (existsSync(dbPath) || !existsSync(templatePath)) {
    return false;
  }
  mkdirSync(dirname(dbPath), { recursive: true });
  copyFileSync(templatePath, dbPath);
  return true;
}

function applyCoreMigrations(db) {
  db.exec(SCHEMA);
  migrateRolesTable(db);
  migrateSkillsTable(db);
  migrateAliasTables(db);
  migrateSkillResumeRulesTable(db);
  migrateResumePointsTable(db);
  migrateResumeLayoutsTable(db);
}

// ──────────────────────────────────────────────────────────
// Main entry
// ──────────────────────────────────────────────────────────
const mode = process.argv[2] || 'all';

const hadExistingDb = existsSync(DB_PATH);
const copiedTemplateDb = copyTemplateDbIfMissing(DB_PATH, DB_TEMPLATE_PATH);

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

if (mode === 'all') {
  console.log('Creating schema...');
  applyCoreMigrations(db);

  console.log('Seeding profile...');
  seedProfile(db);

  console.log('Seeding experience...');
  seedExperience(db);

  console.log('Seeding archetypes...');
  seedArchetypes(db);

  console.log('Seeding bullets...');
  seedBullets(db);

  console.log('Seeding skills...');
  seedSkills(db);

  console.log('Ensuring job intake config tables exist...');
  seedJobIntakeConfig(db);

  console.log('Seeding found_via sources...');
  seedFoundViaSources(db);

  console.log('Seeding resume schema...');
  seedResumeSchema(db);

  console.log('Seeding resume skill rules...');
  seedResumeSkillRules(db);

  console.log('Seeding resume layouts...');
  seedResumeLayouts(db);

  console.log('Seeding capabilities...');
  seedCapabilities(db);

  console.log('Seeding qualifications...');
  seedQualifications(db);

  console.log('Seeding certifications...');
  seedCertifications(db);

  console.log('Seeding education...');
  seedEducation(db);

  console.log('Seeding stories...');
  seedStories(db);

  console.log('Parsing skill signals...');
  parseSkillSignals(db);

  console.log('Parsing roles...');
  parseRoles(db);

  console.log('Seeding question bank...');
  seedQuestionBank(db);

} else if (mode === 'setup') {
  if (copiedTemplateDb) {
    console.log(`Copied template DB to ${DB_PATH}`);
    console.log('Applying migrations to copied template DB...');
    applyCoreMigrations(db);
  } else if (hadExistingDb) {
    console.log('Using existing DB and applying migrations...');
    applyCoreMigrations(db);
  } else {
    console.log('Template DB not found; bootstrapping setup tables from code...');
    applyCoreMigrations(db);

    console.log('Seeding found_via sources...');
    seedFoundViaSources(db);

    console.log('Seeding qualifications...');
    seedQualifications(db);

    console.log('Seeding resume layouts...');
    seedResumeLayouts(db);
  }

} else if (mode === 'signals') {
  console.log('Creating schema (if not exists)...');
  applyCoreMigrations(db);
  console.log('Parsing skill signals...');
  parseSkillSignals(db);

} else if (mode === 'roles') {
  console.log('Creating schema (if not exists)...');
  applyCoreMigrations(db);
  console.log('Parsing roles...');
  parseRoles(db);

} else if (mode === 'resume') {
  console.log('Creating schema (if not exists)...');
  applyCoreMigrations(db);
  console.log('Seeding resume layouts...');
  seedResumeLayouts(db);
  console.log('Seeding resume schema...');
  seedResumeSchema(db);

} else if (mode === 'resume-config') {
  console.log('Creating schema (if not exists)...');
  applyCoreMigrations(db);
  console.log('Skipping hardcoded resume profiles and signal rules...');
  console.log('Seeding resume skill rules...');
  seedResumeSkillRules(db);
  console.log('Seeding resume layouts...');
  seedResumeLayouts(db);

} else if (mode === 'resume-layouts') {
  console.log('Creating schema (if not exists)...');
  applyCoreMigrations(db);
  console.log('Seeding resume layouts...');
  seedResumeLayouts(db);

} else if (mode === 'job-config') {
  console.log('Creating schema (if not exists)...');
  applyCoreMigrations(db);
  console.log('Ensuring job intake config tables exist...');
  seedJobIntakeConfig(db);

  console.log('Seeding found_via sources...');
  seedFoundViaSources(db);

} else if (mode === 'skills') {
  console.log('Creating schema (if not exists)...');
  applyCoreMigrations(db);
  console.log('Seeding skills...');
  seedSkills(db);
  console.log('Seeding resume skill rules...');
  seedResumeSkillRules(db);
  console.log('Seeding capabilities...');
  seedCapabilities(db);

} else if (mode === 'coverletter') {
  console.log('Creating schema (if not exists)...');
  db.exec(SCHEMA);
  console.log('Seeding cover letter profiles...');
  seedCoverLetterProfiles(db);
  console.log('Seeding cover letter modules...');
  seedCoverLetterModules(db);

} else {
  console.error(`Unknown mode: ${mode}. Use: all | setup | roles | signals | resume | resume-config | resume-layouts | job-config | skills | coverletter`);
  process.exit(1);
}

db.close();
console.log('Done.');

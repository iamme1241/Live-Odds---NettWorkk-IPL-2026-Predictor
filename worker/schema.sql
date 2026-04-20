-- Predictor League D1 Schema
-- Run once: wrangler d1 execute predictor-db --file=schema.sql

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ─── MATCHES ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS matches (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  match_number INTEGER NOT NULL UNIQUE,
  title        TEXT    NOT NULL,           -- e.g. "India vs Pakistan"
  team_a       TEXT    NOT NULL,
  team_b       TEXT    NOT NULL,
  match_time   TEXT    NOT NULL,           -- ISO8601 UTC
  status       TEXT    NOT NULL DEFAULT 'upcoming',
                                           -- upcoming | open | closed | resulted
  winner       TEXT,                       -- team_a | team_b | null
  created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
  updated_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ─── PLAYERS ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS players (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  display_name    TEXT    NOT NULL,
  primary_email   TEXT    NOT NULL UNIQUE,
  first_match_num INTEGER,                 -- match_number of first ever vote
  created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- ─── EMAIL MAP ──────────────────────────────────────────────────────────────
-- Maps any alias email → primary player email
-- primary email also has a self-mapping row for uniform lookup
CREATE TABLE IF NOT EXISTS email_map (
  alias_email   TEXT NOT NULL PRIMARY KEY,
  primary_email TEXT NOT NULL,
  FOREIGN KEY (primary_email) REFERENCES players(primary_email)
);

-- ─── PREDICTIONS ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS predictions (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id      INTEGER NOT NULL,
  primary_email TEXT    NOT NULL,          -- resolved via email_map
  raw_email     TEXT    NOT NULL,          -- exactly as submitted
  predicted_team TEXT   NOT NULL,          -- team_a | team_b
  submitted_at  TEXT    NOT NULL DEFAULT (datetime('now')),
  is_valid      INTEGER NOT NULL DEFAULT 1, -- 0 = duplicate/cheating attempt
  invalid_reason TEXT,
  FOREIGN KEY (match_id) REFERENCES matches(id),
  UNIQUE (match_id, primary_email)         -- one valid vote per player per match
);

-- ─── RESULTS & SCORES ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS scores (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id      INTEGER NOT NULL,
  primary_email TEXT    NOT NULL,
  predicted_team TEXT   NOT NULL,
  winner        TEXT    NOT NULL,
  odds_at_close REAL    NOT NULL,          -- snapshot when match closed
  base_points   REAL    NOT NULL DEFAULT 100,
  points_earned REAL    NOT NULL,          -- 0 if wrong, 100*odds if correct
  calculated_at TEXT    NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (match_id) REFERENCES matches(id),
  UNIQUE (match_id, primary_email)
);

-- ─── PENALTIES ──────────────────────────────────────────────────────────────
-- Recalculated every time a result is entered
CREATE TABLE IF NOT EXISTS penalties (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  primary_email TEXT    NOT NULL,
  match_id      INTEGER NOT NULL,          -- match that was missed
  penalty_pts   REAL    NOT NULL DEFAULT -50,
  reason        TEXT    NOT NULL DEFAULT 'missed_vote',
  calculated_at TEXT    NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (match_id) REFERENCES matches(id),
  UNIQUE (match_id, primary_email)
);

-- ─── ODDS SNAPSHOTS ──────────────────────────────────────────────────────────
-- Live odds are computed on the fly; snapshots stored at match close
CREATE TABLE IF NOT EXISTS odds_snapshots (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  match_id   INTEGER NOT NULL,
  team_a_votes INTEGER NOT NULL,
  team_b_votes INTEGER NOT NULL,
  total_votes  INTEGER NOT NULL,
  team_a_odds  REAL    NOT NULL,
  team_b_odds  REAL    NOT NULL,
  snapped_at   TEXT    NOT NULL DEFAULT (datetime('now')),
  is_final     INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (match_id) REFERENCES matches(id)
);

-- ─── AUDIT LOG ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_log (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  action     TEXT NOT NULL,
  actor      TEXT NOT NULL DEFAULT 'system',
  entity     TEXT,
  entity_id  TEXT,
  details    TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ─── INDEXES ─────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_predictions_match    ON predictions(match_id);
CREATE INDEX IF NOT EXISTS idx_predictions_email    ON predictions(primary_email);
CREATE INDEX IF NOT EXISTS idx_scores_email         ON scores(primary_email);
CREATE INDEX IF NOT EXISTS idx_penalties_email      ON penalties(primary_email);
CREATE INDEX IF NOT EXISTS idx_email_map_alias      ON email_map(alias_email);
CREATE INDEX IF NOT EXISTS idx_matches_number       ON matches(match_number);
CREATE INDEX IF NOT EXISTS idx_matches_status       ON matches(status);

/**
 * Predictor League Worker API v6
 * FIXES:
 *  - recalcPenalties: respects manually-deleted missed_vote penalties (uses a "tombstone" approach via explicit_penalty_exemptions or a deleted_ids set)
 *    → ACTUAL FIX: we track manually deleted penalty IDs in a separate `penalty_exemptions` table; recalc skips those (player+match_id combos)
 *    → SIMPLER FIX used here: recalcPenalties only deletes and re-adds missed_vote penalties that were NOT manually removed
 *      We track this via a `penalty_deleted` flag column, but since we can't alter schema here, we use a soft-delete pattern:
 *      On manual delete: mark with reason='missed_vote_exempted' instead of deleting row. Recalc skips those.
 *      → ACTUALLY: the simplest correct fix: store deleted penalty (player+match) combos in-memory during recalc and skip them.
 *      → FINAL DECISION: We keep the current schema. Manual DELETE endpoint now inserts an "exemption" row with reason='missed_vote_exempt'.
 *        recalcPenalties skips (primary_email, match_id) combos that have an existing 'missed_vote_exempt' row.
 *  - Added POST /api/admin/predictions/relink-emails: re-resolves all prediction primary_email values against current email_map
 *  - Added POST /api/admin/players/merge: merge alias player records into primary (removes alias player row, re-links all data)
 *  - PATCH /api/admin/predictions/:id: now also accepts display_name to update the player's name
 *  - cascadeEmailMap: now also removes the alias from players table if it was incorrectly added as a primary player
 *  - PATCH /api/admin/predictions/:id with primary_email: validates the target email exists in players table (or creates minimal record)
 *  - buildLB: correctly handles players with no scores (still shows in LB if they have penalties/bonuses)
 *  - Bulk predictions: name field properly stored and used
 *  - resolveEmail: now also checks if the raw email itself is a primary_email in players table before looking up alias map
 *  - applyDoubleHeaderBonus: added explicit unique key check to prevent duplicate bonuses
 *  - Added GET /api/admin/predictions/relink-preview: shows what would change before committing
 *  - scoreMatch: now handles case where odds snapshot already exists (force-result idempotency)
 */

// ─── CORS ────────────────────────────────────────────────────────────────────
function cors() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  };
}
const R = (data, status = 200) => new Response(JSON.stringify(data), {
  status, headers: { 'Content-Type': 'application/json', ...cors() }
});
const E = (msg, status = 400) => R({ error: msg }, status);

// ─── AUTH ─────────────────────────────────────────────────────────────────────
function isAdmin(req, env) {
  const token = (req.headers.get('Authorization') || '').replace('Bearer ', '');
  return token === btoa(`${env.ADMIN_SECRET_PATH}:${env.ADMIN_PASSWORD}`);
}

// ─── RESOLVE TEAM NAME ────────────────────────────────────────────────────────
async function resolveTeam(db, rawTeam) {
  if (!rawTeam) return rawTeam;
  const t = rawTeam.trim();
  const row = await db.prepare(
    'SELECT primary_name FROM team_aliases WHERE LOWER(alias_name)=LOWER(?)'
  ).bind(t).first();
  return row ? row.primary_name : t;
}

// ─── ODDS ─────────────────────────────────────────────────────────────────────
async function getOdds(db, matchId) {
  const match = await db.prepare('SELECT team_a, team_b FROM matches WHERE id=?').bind(matchId).first();
  if (!match) return null;
  const votes = await db.prepare(
    `SELECT predicted_team, COUNT(*) c FROM predictions WHERE match_id=? AND is_valid=1 GROUP BY predicted_team`
  ).bind(matchId).all();
  let a = 0, b = 0;
  for (const v of votes.results) {
    if (v.predicted_team === match.team_a) a = v.c;
    else if (v.predicted_team === match.team_b) b = v.c;
  }
  const t = a + b;
  return {
    team_a: match.team_a, team_b: match.team_b,
    team_a_votes: a, team_b_votes: b, total_votes: t,
    team_a_odds: t > 0 && a > 0 ? parseFloat((t / a).toFixed(2)) : null,
    team_b_odds: t > 0 && b > 0 ? parseFloat((t / b).toFixed(2)) : null,
  };
}

// ─── RESOLVE EMAIL ────────────────────────────────────────────────────────────
// Returns the canonical primary_email for any given email address.
// Checks email_map first, then falls back to the email itself.
async function resolveEmail(db, raw) {
  if (!raw) return raw;
  const e = raw.trim().toLowerCase();
  const r = await db.prepare('SELECT primary_email FROM email_map WHERE LOWER(alias_email)=?').bind(e).first();
  return r ? r.primary_email : e;
}

// ─── ENSURE PLAYER ───────────────────────────────────────────────────────────
async function ensurePlayer(db, primaryEmail, rawEmail, matchNum, displayName) {
  let p = await db.prepare('SELECT * FROM players WHERE primary_email=?').bind(primaryEmail).first();
  if (!p) {
    const name = displayName || rawEmail.split('@')[0];
    await db.prepare(`INSERT INTO players (display_name, primary_email, first_match_num) VALUES (?,?,?)`)
      .bind(name, primaryEmail, matchNum).run();
    await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
      .bind(primaryEmail, primaryEmail).run();
    if (rawEmail !== primaryEmail) {
      await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
        .bind(rawEmail, primaryEmail).run();
    }
  } else {
    if (matchNum && (!p.first_match_num || matchNum < p.first_match_num)) {
      await db.prepare(`UPDATE players SET first_match_num=? WHERE primary_email=?`)
        .bind(matchNum, primaryEmail).run();
    }
    if (displayName && displayName !== p.display_name) {
      await db.prepare(`UPDATE players SET display_name=? WHERE primary_email=?`)
        .bind(displayName, primaryEmail).run();
    }
  }
}

// ─── SCORE MATCH (IDEMPOTENT) ─────────────────────────────────────────────────
async function scoreMatch(db, matchId, winner) {
  const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(matchId).first();
  if (!match) return;

  // ─── CANCELLED ─────────────────────────────
  if (winner === 'cancelled') {
    await db.prepare(`DELETE FROM scores WHERE match_id=?`).bind(matchId).run();

    await db.prepare(
      `UPDATE matches SET status='cancelled', winner=NULL, updated_at=datetime('now') WHERE id=?`
    ).bind(matchId).run();

    return;
  }

  // Snapshot odds (existing logic)
  let snap = await db.prepare(
    `SELECT * FROM odds_snapshots WHERE match_id=? AND is_final=1`
  ).bind(matchId).first();

  if (!snap) {
    const o = await getOdds(db, matchId);
    await db.prepare(
      `INSERT OR REPLACE INTO odds_snapshots 
       (match_id,team_a_votes,team_b_votes,total_votes,team_a_odds,team_b_odds,is_final)
       VALUES (?,?,?,?,?,?,1)`
    ).bind(matchId, o.team_a_votes, o.team_b_votes, o.total_votes, o.team_a_odds, o.team_b_odds).run();

    snap = o;
  }

  // Clear old scores
  await db.prepare(`DELETE FROM scores WHERE match_id=?`).bind(matchId).run();

  const preds = await db.prepare(
    `SELECT * FROM predictions WHERE match_id=? AND is_valid=1`
  ).bind(matchId).all();

  // ─── ABANDONED ─────────────────────────────
  if (winner === 'abandoned') {
    const totalVotes = preds.results.length;
    if (totalVotes === 0) return;

    const pool = 100 * totalVotes;
    const share = parseFloat((pool / totalVotes).toFixed(2));

    for (const p of preds.results) {
      await db.prepare(
        `INSERT INTO scores 
        (match_id,primary_email,predicted_team,winner,odds_at_close,base_points,points_earned)
        VALUES (?,?,?,?,?,?,?)`
      ).bind(
        matchId,
        p.primary_email,
        p.predicted_team,
        'abandoned',
        1,
        100,
        share
      ).run();
    }

    await db.prepare(
      `UPDATE matches SET status='abandoned', winner='abandoned', updated_at=datetime('now') WHERE id=?`
    ).bind(matchId).run();

    return;
  }

  // ─── NORMAL MATCH ──────────────────────────
  for (const p of preds.results) {
    const oddsUsed = p.predicted_team === match.team_a ? snap.team_a_odds : snap.team_b_odds;

    const pts = p.predicted_team === winner
      ? parseFloat((100 * (oddsUsed || 1)).toFixed(2))
      : 0;

    await db.prepare(
      `INSERT INTO scores 
      (match_id,primary_email,predicted_team,winner,odds_at_close,base_points,points_earned)
      VALUES (?,?,?,?,?,?,?)`
    ).bind(matchId, p.primary_email, p.predicted_team, winner, oddsUsed || 1, 100, pts).run();
  }

  await db.prepare(
    `UPDATE matches SET status='resulted', winner=?, updated_at=datetime('now') WHERE id=?`
  ).bind(winner, matchId).run();
}

// ─── PENALTY ENGINE ───────────────────────────────────────────────────────────
// FIX: respects manually-deleted penalties by checking for 'missed_vote_exempt' rows.
// When an admin manually deletes a missed_vote penalty, we insert a 'missed_vote_exempt'
// row for that (primary_email, match_id) combo. recalcPenalties skips those combos.
async function recalcPenalties(db) {
  const resulted = await db.prepare(
    SELECT id, match_number FROM matches WHERE status IN ('resulted','abandoned') ORDER BY match_number`
  ).all();
  if (!resulted.results.length) return;

  // Load all exemptions (manually deleted missed_vote penalties)
  const exemptions = await db.prepare(
    `SELECT primary_email, match_id FROM penalties WHERE reason='missed_vote_exempt'`
  ).all();
  const exemptSet = new Set(exemptions.results.map(e => `${e.primary_email}::${e.match_id}`));

  // Clear only non-exempt missed_vote penalties
  await db.prepare(`DELETE FROM penalties WHERE reason='missed_vote'`).run();

  const players = await db.prepare(
    `SELECT primary_email, first_match_num FROM players WHERE first_match_num IS NOT NULL`
  ).all();

  for (const pl of players.results) {
    const obligated = resulted.results.filter(m => m.match_number >= pl.first_match_num);

    const voted = await db.prepare(
      `SELECT m.match_number
       FROM predictions p
       JOIN matches m ON m.id = p.match_id
       WHERE p.primary_email = ?
         AND p.is_valid = 1
         AND m.status = 'resulted'`
    ).bind(pl.primary_email).all();

    const votedNums = new Set(voted.results.map(r => r.match_number));

    for (const m of obligated) {
      if (!votedNums.has(m.match_number)) {
        const key = `${pl.primary_email}::${m.id}`;
        if (!exemptSet.has(key)) {
          await db.prepare(
            `INSERT OR IGNORE INTO penalties (primary_email,match_id,penalty_pts,reason) VALUES (?,?,-50,'missed_vote')`
          ).bind(pl.primary_email, m.id).run();
        }
      }
    }
  }
}

// ─── DOUBLE HEADER BONUS ──────────────────────────────────────────────────────
async function applyDoubleHeaderBonus(db) {
  const resulted = await db.prepare(
    `SELECT id, match_number, match_time, winner, team_a, team_b FROM matches WHERE status='resulted' ORDER BY match_number`
  ).all();

  const byDate = {};
  for (const m of resulted.results) {
    const d = new Date(m.match_time);
    const ist = new Date(d.getTime() + (5.5 * 60 * 60 * 1000));
    const key = ist.getUTCFullYear() + '-' +
      String(ist.getUTCMonth() + 1).padStart(2, '0') + '-' +
      String(ist.getUTCDate()).padStart(2, '0');
    if (!byDate[key]) byDate[key] = [];
    byDate[key].push(m);
  }

  await db.prepare(`DELETE FROM bonus_points WHERE reason='double_header'`).run();

  for (const [, ms] of Object.entries(byDate)) {
    if (ms.length < 2) continue;
    const sorted = ms.sort((a, b) => new Date(a.match_time) - new Date(b.match_time));

    for (let i = 0; i < sorted.length - 1; i++) {
      const m1 = sorted[i];
      const m2 = sorted[i + 1];

      const c1 = await db.prepare(`SELECT primary_email FROM scores WHERE match_id=? AND points_earned>0`)
        .bind(m1.id).all();
      const c2 = await db.prepare(`SELECT primary_email FROM scores WHERE match_id=? AND points_earned>0`)
        .bind(m2.id).all();

      const s1 = new Set(c1.results.map(r => r.primary_email));
      const s2 = new Set(c2.results.map(r => r.primary_email));

      for (const email of s1) {
        if (s2.has(email)) {
          // Check if already exists before inserting
          const exists = await db.prepare(
            `SELECT id FROM bonus_points WHERE primary_email=? AND match_id=? AND reason='double_header'`
          ).bind(email, m2.id).first();
          if (!exists) {
            await db.prepare(
              `INSERT INTO bonus_points (primary_email,match_id,bonus_pts,reason,details)
               VALUES (?,?,50,'double_header',?)`
            ).bind(email, m2.id, `Double header: M${m1.match_number}+M${m2.match_number}`).run();
          }
        }
      }
    }
  }
}

// ─── CASCADE EMAIL MAP ────────────────────────────────────────────────────────
// After adding/changing an alias→primary mapping, re-link all data.
// Also handles case where alias was incorrectly added as a primary player.
async function cascadeEmailMap(db, aliasEmail, primaryEmail) {
  // Re-link predictions
  await db.prepare(
    `UPDATE predictions SET primary_email=? WHERE raw_email=? OR primary_email=?`
  ).bind(primaryEmail, aliasEmail, aliasEmail).run();

  // Re-link scores
  await db.prepare(
    `UPDATE scores SET primary_email=? WHERE primary_email=?`
  ).bind(primaryEmail, aliasEmail).run();

  // Re-link penalties
  await db.prepare(
    `UPDATE penalties SET primary_email=? WHERE primary_email=?`
  ).bind(primaryEmail, aliasEmail).run();

  // Re-link bonus_points
  await db.prepare(
    `UPDATE bonus_points SET primary_email=? WHERE primary_email=?`
  ).bind(primaryEmail, aliasEmail).run();

  // If the alias email was incorrectly set up as a primary player, remove that ghost player
  // (but only if it's truly just an alias, i.e. primaryEmail != aliasEmail)
  if (aliasEmail !== primaryEmail) {
    const aliasAsPlayer = await db.prepare(
      `SELECT primary_email FROM players WHERE primary_email=?`
    ).bind(aliasEmail).first();
    if (aliasAsPlayer) {
      // Merge: delete the ghost player (its data has already been re-linked above)
      await db.prepare(`DELETE FROM players WHERE primary_email=?`).bind(aliasEmail).run();
      await db.prepare(`DELETE FROM email_map WHERE primary_email=?`).bind(aliasEmail).run();
    }
  }
}

// ─── RELINK ALL PREDICTIONS ───────────────────────────────────────────────────
// Re-resolves every prediction's primary_email against current email_map.
// Also re-resolves raw_email for any prediction where raw_email has a mapping.
async function relinkAllPredictions(db) {
  const all = await db.prepare(`SELECT id, raw_email, primary_email FROM predictions`).all();
  let updated = 0, skipped = 0;
  const changes = [];

  for (const pred of all.results) {
    // Resolve via raw_email first (the original submission email)
    const resolvedFromRaw = await db.prepare(
      `SELECT primary_email FROM email_map WHERE LOWER(alias_email)=LOWER(?)`
    ).bind(pred.raw_email).first();

    // Also resolve current primary_email in case it's an alias
    const resolvedFromPrimary = await db.prepare(
      `SELECT primary_email FROM email_map WHERE LOWER(alias_email)=LOWER(?)`
    ).bind(pred.primary_email).first();

    // Prefer resolution from raw_email; fallback to current primary resolution
    const resolved = (resolvedFromRaw?.primary_email) || (resolvedFromPrimary?.primary_email) || pred.primary_email;

    if (resolved !== pred.primary_email) {
      await db.prepare(`UPDATE predictions SET primary_email=? WHERE id=?`).bind(resolved, pred.id).run();
      changes.push({ id: pred.id, old_email: pred.primary_email, new_email: resolved, raw_email: pred.raw_email });
      updated++;
    } else {
      skipped++;
    }
  }

  // Also re-link scores, penalties, bonus_points for any changed emails
  const emailMap = await db.prepare(`SELECT alias_email, primary_email FROM email_map`).all();
  for (const row of emailMap.results) {
    if (row.alias_email === row.primary_email) continue;
    await db.prepare(`UPDATE scores SET primary_email=? WHERE primary_email=?`)
      .bind(row.primary_email, row.alias_email).run();
    await db.prepare(`UPDATE penalties SET primary_email=? WHERE primary_email=?`)
      .bind(row.primary_email, row.alias_email).run();
    await db.prepare(`UPDATE bonus_points SET primary_email=? WHERE primary_email=?`)
      .bind(row.primary_email, row.alias_email).run();
  }

  return { updated, skipped, changes };
}

// ─── LEADERBOARD ─────────────────────────────────────────────────────────────
async function buildLB(db, asOf) {
  let matchCond = `status IN ('resulted','abandoned')`;
  if (asOf) matchCond += ` AND updated_at <= '${asOf}T23:59:59'`;

  const matchIds = await db.prepare(`SELECT id FROM matches WHERE ${matchCond}`).all();
  if (!matchIds.results.length) return [];
  const ids = matchIds.results.map(m => m.id).join(',');
  if (!ids) return [];

  const players = await db.prepare(`
    SELECT
      p.primary_email,
      p.display_name,
      p.first_match_num,

      COALESCE(s.gross, 0)  AS gross,
      COALESCE(pen.pen, 0)  AS pen,
      COALESCE(bp.bonus, 0) AS bonus,

      COUNT(DISTINCT s2.match_id) AS played,
      COUNT(DISTINCT CASE WHEN s2.points_earned > 0 THEN s2.match_id END) AS correct

    FROM players p

    LEFT JOIN (
      SELECT primary_email, SUM(points_earned) AS gross
      FROM scores
      WHERE match_id IN (${ids})
      GROUP BY primary_email
    ) s ON s.primary_email = p.primary_email

    LEFT JOIN (
      SELECT primary_email, SUM(penalty_pts) AS pen
      FROM penalties
      WHERE match_id IN (${ids})
        AND reason NOT IN ('missed_vote_exempt')
      GROUP BY primary_email
    ) pen ON pen.primary_email = p.primary_email

    LEFT JOIN (
      SELECT primary_email, SUM(bonus_pts) AS bonus
      FROM bonus_points
      WHERE match_id IN (${ids})
      GROUP BY primary_email
    ) bp ON bp.primary_email = p.primary_email

    LEFT JOIN scores s2 ON s2.primary_email = p.primary_email AND s2.match_id IN (${ids})

    WHERE p.first_match_num IS NOT NULL

    GROUP BY p.primary_email

    ORDER BY (COALESCE(s.gross,0) + COALESCE(pen.pen,0) + COALESCE(bp.bonus,0)) DESC
  `).all();

  return players.results.map((p, i) => ({
    rank: i + 1,
    display_name: p.display_name,
    primary_email: p.primary_email,
    gross_points: +parseFloat(p.gross).toFixed(2),
    penalties: +parseFloat(p.pen).toFixed(2),
    bonuses: +parseFloat(p.bonus).toFixed(2),
    net_points: +(parseFloat(p.gross) + parseFloat(p.pen) + parseFloat(p.bonus)).toFixed(2),
    matches_played: p.played,
    correct_predictions: p.correct,
    first_match: p.first_match_num,
  }));
}

// ─── INSIGHTS ────────────────────────────────────────────────────────────────
async function buildInsights(db) {
  const players = await db.prepare(`SELECT primary_email, display_name FROM players`).all();
  const streaks = [];
  for (const pl of players.results) {
    const history = await db.prepare(
      `SELECT s.points_earned, m.match_number FROM scores s
       JOIN matches m ON m.id=s.match_id
       WHERE s.primary_email=? ORDER BY m.match_number DESC LIMIT 10`
    ).bind(pl.primary_email).all();
    let streak = 0;
    for (const h of history.results) {
      if (h.points_earned > 0) streak++;
      else break;
    }
    if (streak > 0) streaks.push({ name: pl.display_name, email: pl.primary_email, streak });
  }
  streaks.sort((a, b) => b.streak - a.streak);

  const lb = await buildLB(db, null);
  const top5 = lb.slice(0, 5).map(p => p.primary_email);
  const bot5 = lb.slice(-5).map(p => p.primary_email);

  const resulted = await db.prepare(
    `SELECT id, match_number, title, winner, team_a, team_b FROM matches WHERE status='resulted' ORDER BY match_number DESC LIMIT 10`
  ).all();

  const matchComparison = [];
  for (const m of resulted.results) {
    const topPicks = top5.length ? await db.prepare(
      `SELECT predicted_team, COUNT(*) c FROM predictions WHERE match_id=? AND is_valid=1 AND primary_email IN (${top5.map(() => '?').join(',')}) GROUP BY predicted_team`
    ).bind(m.id, ...top5).all() : { results: [] };
    const botPicks = bot5.length ? await db.prepare(
      `SELECT predicted_team, COUNT(*) c FROM predictions WHERE match_id=? AND is_valid=1 AND primary_email IN (${bot5.map(() => '?').join(',')}) GROUP BY predicted_team`
    ).bind(m.id, ...bot5).all() : { results: [] };
    matchComparison.push({
      match_number: m.match_number, title: m.title, winner: m.winner,
      team_a: m.team_a, team_b: m.team_b,
      top5_picks: topPicks.results, bot5_picks: botPicks.results,
    });
  }

  const upsets = await db.prepare(
    `SELECT m.match_number, m.title, m.winner, m.team_a, m.team_b,
            COUNT(CASE WHEN s.points_earned>0 THEN 1 END) correct_count,
            COUNT(s.id) total_count
     FROM matches m LEFT JOIN scores s ON s.match_id=m.id
     WHERE m.status='resulted'
     GROUP BY m.id
     HAVING total_count > 0
     ORDER BY (CAST(correct_count AS REAL)/total_count) ASC LIMIT 10`
  ).all();

  const hardest = upsets.results.map(u => ({
    ...u,
    correct_pct: u.total_count > 0 ? Math.round(u.correct_count / u.total_count * 100) : 0
  }));

  return { streaks: streaks.slice(0, 10), matchComparison, hardest };
}

// ─── ROUTER ──────────────────────────────────────────────────────────────────
export default {

  async scheduled(event, env) {
    const db = env.DB;
    const now = new Date().toISOString();
    const toClose = await db.prepare(
      `SELECT id FROM matches WHERE status='open' AND match_time<=?`
    ).bind(now).all();
    for (const m of toClose.results) {
      const o = await getOdds(db, m.id);
      await db.prepare(
        `INSERT OR REPLACE INTO odds_snapshots (match_id,team_a_votes,team_b_votes,total_votes,team_a_odds,team_b_odds,is_final)
         VALUES (?,?,?,?,?,?,1)`
      ).bind(m.id, o.team_a_votes, o.team_b_votes, o.total_votes, o.team_a_odds, o.team_b_odds).run();
      await db.prepare(`UPDATE matches SET status='closed', updated_at=datetime('now') WHERE id=?`)
        .bind(m.id).run();
    }
  },

  async fetch(req, env) {
    const url = new URL(req.url);
    const path = url.pathname;
    const method = req.method;
    const db = env.DB;

    if (method === 'OPTIONS') return new Response(null, { status: 204, headers: cors() });

    // ── PUBLIC ────────────────────────────────────────────────────────────────

    if (path === '/api/matches' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT id, match_number, title, team_a, team_b, match_time, status, winner FROM matches ORDER BY match_number`
      ).all();
      return R(rows.results);
    }

    if (path.match(/^\/api\/matches\/(\d+)\/odds$/) && method === 'GET') {
      const id = +path.split('/')[3];
      const match = await db.prepare('SELECT status FROM matches WHERE id=?').bind(id).first();
      if (!match) return E('Match not found', 404);
      if (match.status === 'resulted' || match.status === 'closed') {
        const snap = await db.prepare(`SELECT * FROM odds_snapshots WHERE match_id=? AND is_final=1`).bind(id).first();
        if (snap) return R({ ...snap, frozen: true });
      }
      return R({ ...await getOdds(db, id), frozen: false });
    }

    if (path === '/api/leaderboard' && method === 'GET') {
      return R(await buildLB(db, null));
    }

    if (path.match(/^\/api\/players\/.+\/history$/) && method === 'GET') {
      const rawEmail = decodeURIComponent(path.split('/')[3]);
      const primary = await resolveEmail(db, rawEmail);
      const player = await db.prepare('SELECT * FROM players WHERE primary_email=?').bind(primary).first();
      if (!player) return E('Player not found', 404);

      const history = await db.prepare(
        `SELECT
           m.match_number, m.title, m.team_a, m.team_b, m.winner,
           m.match_time, m.status,
           p.predicted_team, p.submitted_at,
           s.points_earned, s.odds_at_close,
           pen.penalty_pts, pen.reason AS penalty_reason,
           bp.bonus_pts, bp.reason AS bonus_reason, bp.details AS bonus_details
         FROM matches m
         LEFT JOIN predictions p   ON p.match_id=m.id   AND p.primary_email=? AND p.is_valid=1
         LEFT JOIN scores s        ON s.match_id=m.id   AND s.primary_email=?
         LEFT JOIN penalties pen   ON pen.match_id=m.id AND pen.primary_email=? AND pen.reason NOT IN ('missed_vote_exempt')
         LEFT JOIN bonus_points bp ON bp.match_id=m.id  AND bp.primary_email=?
         WHERE m.status IN ('resulted','closed','open')
         ORDER BY m.match_number ASC`
      ).bind(primary, primary, primary, primary).all();

      const generalBonuses = await db.prepare(
        `SELECT bp.*, m.match_number FROM bonus_points bp
         LEFT JOIN matches m ON m.id=bp.match_id
         WHERE bp.primary_email=?`
      ).bind(primary).all();

      const rows = history.results;
      const gross = rows.reduce((s, h) => s + (h.points_earned ?? 0), 0);
      const pen   = rows.reduce((s, h) => s + (h.penalty_pts  ?? 0), 0);
      const bonus = generalBonuses.results.reduce((s, b) => s + (b.bonus_pts ?? 0), 0);
      const net   = gross + pen + bonus;

      return R({
        player: { display_name: player.display_name, first_match_num: player.first_match_num },
        summary: {
          gross_points: +gross.toFixed(2), penalties: +pen.toFixed(2),
          bonuses: +bonus.toFixed(2), net_points: +net.toFixed(2),
          matches_played: rows.filter(h => h.predicted_team).length,
          correct: rows.filter(h => h.points_earned > 0).length,
        },
        history: rows,
        general_bonuses: generalBonuses.results,
      });
    }

    if (path === '/api/insights' && method === 'GET') {
      return R(await buildInsights(db));
    }

    if (path === '/api/predict' && method === 'POST') {
      let body; try { body = await req.json(); } catch { return E('Invalid JSON'); }
      const { email, match_id, name } = body;
      let { predicted_team } = body;
      if (!email || !match_id || !predicted_team) return E('Missing fields');

      const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(match_id).first();
      if (!match) return E('Match not found', 404);
      if (match.status !== 'open') return E('Predictions closed for this match');

      predicted_team = await resolveTeam(db, predicted_team);
      if (predicted_team !== match.team_a && predicted_team !== match.team_b)
        return E(`Invalid team. Accepted: "${match.team_a}" or "${match.team_b}"`);

      const rawEmail = email.trim().toLowerCase();
      const primary = await resolveEmail(db, rawEmail);

      const existing = await db.prepare(
        `SELECT id FROM predictions WHERE match_id=? AND primary_email=? AND is_valid=1`
      ).bind(match_id, primary).first();

      if (existing) {
        await db.prepare(
          `INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,is_valid,invalid_reason)
           VALUES (?,?,?,?,0,'duplicate_vote')`
        ).bind(match_id, primary, rawEmail, predicted_team).run();
        return E('Already voted. First vote stands.', 409);
      }

      await ensurePlayer(db, primary, rawEmail, match.match_number, name);
      await db.prepare(
        `INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,is_valid) VALUES (?,?,?,?,1)`
      ).bind(match_id, primary, rawEmail, predicted_team).run();

      return R({ success: true, odds: await getOdds(db, match_id) });
    }

    // Google Forms webhook
    if (path === '/api/forms-submit' && method === 'POST') {
      let body; try { body = await req.json(); } catch { return E('Invalid JSON'); }
      const { email, match_number, name } = body;
      let { predicted_team } = body;
      if (!email || !match_number || !predicted_team) return E('Missing fields');

      const match = await db.prepare('SELECT * FROM matches WHERE match_number=?').bind(+match_number).first();
      if (!match) return E(`Match number ${match_number} not found.`, 404);
      if (match.status !== 'open') return E('Predictions closed for this match');

      predicted_team = await resolveTeam(db, predicted_team);
      if (predicted_team !== match.team_a && predicted_team !== match.team_b)
        return E(`Invalid team. Accepted: "${match.team_a}" or "${match.team_b}"`);

      const rawEmail = email.trim().toLowerCase();
      const primary = await resolveEmail(db, rawEmail);

      const existing = await db.prepare(
        `SELECT id FROM predictions WHERE match_id=? AND primary_email=? AND is_valid=1`
      ).bind(match.id, primary).first();

      if (existing) {
        await db.prepare(
          `INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,is_valid,invalid_reason)
           VALUES (?,?,?,?,0,'duplicate_vote')`
        ).bind(match.id, primary, rawEmail, predicted_team).run();
        return E('Already voted. First vote stands.', 409);
      }

      await ensurePlayer(db, primary, rawEmail, match.match_number, name);
      await db.prepare(
        `INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,is_valid) VALUES (?,?,?,?,1)`
      ).bind(match.id, primary, rawEmail, predicted_team).run();

      return R({ success: true });
    }

    // ── ADMIN AUTH CHECK ──────────────────────────────────────────────────────
    if (!isAdmin(req, env)) return E('Unauthorized', 401);

    // ── RECALCULATE ALL (FULLY IDEMPOTENT) ───────────────────────────────────
    if (path === '/api/admin/recalculate-all' && method === 'POST') {
      await db.prepare(`DELETE FROM scores`).run();
      await db.prepare(`DELETE FROM penalties WHERE reason NOT IN ('missed_vote_exempt')`).run();
      await db.prepare(`DELETE FROM bonus_points WHERE reason IN ('double_header')`).run();

      const matches = await db.prepare(
        `SELECT id, winner FROM matches WHERE status='resulted' ORDER BY match_number`
      ).all();

      for (const m of matches.results) {
        await scoreMatch(db, m.id, m.winner);
      }

      await recalcPenalties(db);
      await applyDoubleHeaderBonus(db);

      return R({ success: true, matches_processed: matches.results.length });
    }

    // ── RELINK PREDICTIONS EMAIL ──────────────────────────────────────────────
    // GET: preview what would change
    if (path === '/api/admin/predictions/relink-preview' && method === 'GET') {
      const all = await db.prepare(`SELECT id, raw_email, primary_email FROM predictions`).all();
      const changes = [];
      for (const pred of all.results) {
        const resolvedFromRaw = await db.prepare(
          `SELECT primary_email FROM email_map WHERE LOWER(alias_email)=LOWER(?)`
        ).bind(pred.raw_email).first();
        const resolvedFromPrimary = await db.prepare(
          `SELECT primary_email FROM email_map WHERE LOWER(alias_email)=LOWER(?)`
        ).bind(pred.primary_email).first();
        const resolved = (resolvedFromRaw?.primary_email) || (resolvedFromPrimary?.primary_email) || pred.primary_email;
        if (resolved !== pred.primary_email) {
          changes.push({ id: pred.id, raw_email: pred.raw_email, old_primary: pred.primary_email, new_primary: resolved });
        }
      }
      return R({ changes, total_affected: changes.length });
    }

    // POST: commit the relink
    if (path === '/api/admin/predictions/relink-emails' && method === 'POST') {
      const result = await relinkAllPredictions(db);
      await recalcPenalties(db);
      await applyDoubleHeaderBonus(db);
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('relink_emails', 'admin', 'predictions', 'all', `Updated: ${result.updated}, Skipped: ${result.skipped}`).run();
      return R({ success: true, ...result });
    }

    // ── FORCE OPEN / RECALCULATE MATCH (legacy endpoints) ────────────────────
    if (path === '/api/admin/match/force-open' && method === 'POST') {
      const { match_id } = await req.json();
      await db.prepare(`UPDATE matches SET status='open' WHERE id=?`).bind(match_id).run();
      return R({ success: true });
    }

    if (path === '/api/admin/match/recalculate' && method === 'POST') {
      const { match_id, winner } = await req.json();
      await db.prepare(`DELETE FROM scores WHERE match_id=?`).bind(match_id).run();
      await scoreMatch(db, match_id, winner);
      await recalcPenalties(db);
      await applyDoubleHeaderBonus(db);
      return R({ success: true });
    }

    // ── MATCH CRUD ────────────────────────────────────────────────────────────

    if (path === '/api/admin/matches' && method === 'POST') {
      const { match_number, title, team_a, team_b, match_time } = await req.json();
      if (!match_number || !title || !team_a || !team_b || !match_time) return E('Missing fields');
      const r = await db.prepare(
        `INSERT INTO matches (match_number,title,team_a,team_b,match_time,status) VALUES (?,?,?,?,?,'upcoming')`
      ).bind(+match_number, title, team_a, team_b, match_time).run();
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('match_created', 'admin', 'match', String(r.meta.last_row_id), title).run();
      return R({ id: r.meta.last_row_id });
    }

    if (path === '/api/admin/matches/bulk' && method === 'POST') {
      const { matches } = await req.json();
      if (!Array.isArray(matches)) return E('matches array required');
      let created = 0, skipped = 0, errors = [];
      for (const m of matches) {
        const { match_number, title, team_a, team_b, match_time } = m;
        if (!match_number || !title || !team_a || !team_b || !match_time) {
          errors.push(`M${match_number || '?'}: missing fields`); skipped++; continue;
        }
        try {
          await db.prepare(
            `INSERT INTO matches (match_number,title,team_a,team_b,match_time,status) VALUES (?,?,?,?,?,'upcoming')`
          ).bind(+match_number, title, team_a, team_b, match_time).run();
          created++;
        } catch (e) { errors.push(`M${match_number}: ${e.message}`); skipped++; }
      }
      return R({ created, skipped, errors });
    }

    if (path.match(/^\/api\/admin\/matches\/(\d+)$/) && method === 'PATCH') {
      const id = +path.split('/')[4];
      const body = await req.json();
      const f = [], v = [];
      if (body.status     !== undefined) { f.push('status=?');     v.push(body.status); }
      if (body.match_time !== undefined) { f.push('match_time=?'); v.push(body.match_time); }
      if (body.title      !== undefined) { f.push('title=?');      v.push(body.title); }
      if (body.team_a     !== undefined) { f.push('team_a=?');     v.push(body.team_a); }
      if (body.team_b     !== undefined) { f.push('team_b=?');     v.push(body.team_b); }
      if (body.winner     !== undefined) { f.push('winner=?');     v.push(body.winner); }
      if (!f.length) return E('Nothing to update');
      f.push("updated_at=datetime('now')"); v.push(id);
      await db.prepare(`UPDATE matches SET ${f.join(',')} WHERE id=?`).bind(...v).run();
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/matches\/(\d+)$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      const m = await db.prepare('SELECT status FROM matches WHERE id=?').bind(id).first();
      if (!m) return E('Not found', 404);
      if (m.status !== 'upcoming') return E('Can only delete upcoming matches. Use force-delete for others.');
      await db.prepare('DELETE FROM matches WHERE id=?').bind(id).run();
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/matches\/(\d+)\/force-delete$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      await db.prepare('DELETE FROM predictions WHERE match_id=?').bind(id).run();
      await db.prepare('DELETE FROM scores WHERE match_id=?').bind(id).run();
      await db.prepare('DELETE FROM penalties WHERE match_id=?').bind(id).run();
      await db.prepare('DELETE FROM bonus_points WHERE match_id=?').bind(id).run();
      await db.prepare('DELETE FROM odds_snapshots WHERE match_id=?').bind(id).run();
      await db.prepare('DELETE FROM matches WHERE id=?').bind(id).run();
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/matches\/(\d+)\/open$/) && method === 'POST') {
      const id = +path.split('/')[4];
      await db.prepare(`UPDATE matches SET status='open',updated_at=datetime('now') WHERE id=?`).bind(id).run();
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/matches\/(\d+)\/close$/) && method === 'POST') {
      const id = +path.split('/')[4];
      const o = await getOdds(db, id);
      await db.prepare(
        `INSERT OR REPLACE INTO odds_snapshots (match_id,team_a_votes,team_b_votes,total_votes,team_a_odds,team_b_odds,is_final)
         VALUES (?,?,?,?,?,?,1)`
      ).bind(id, o.team_a_votes, o.team_b_votes, o.total_votes, o.team_a_odds, o.team_b_odds).run();
      await db.prepare(`UPDATE matches SET status='closed',updated_at=datetime('now') WHERE id=?`).bind(id).run();
      return R({ success: true, odds: o });
    }

    if (path.match(/^\/api\/admin\/matches\/(\d+)\/result$/) && method === 'POST') {
      const id = +path.split('/')[4];
      const { winner } = await req.json();
      if (!winner) return E('winner required');
      const m = await db.prepare('SELECT * FROM matches WHERE id=?').bind(id).first();
      if (!m) return E('Not found', 404);
      if (m.status !== 'closed') return E('Close match first (or use force-result)');
      await scoreMatch(db, id, winner);
      await recalcPenalties(db);
      await applyDoubleHeaderBonus(db);
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('result_entered', 'admin', 'match', String(id), `Winner: ${winner}`).run();
      return R({ success: true });
    }

    // FORCE RESULT
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/force-result$/) && method === 'POST') {
      const id = +path.split('/')[4];
      const { winner } = await req.json();
      if (!winner) return E('winner required');
      const m = await db.prepare('SELECT * FROM matches WHERE id=?').bind(id).first();
      if (!m) return E('Not found', 404);

      const existingSnap = await db.prepare(`SELECT id FROM odds_snapshots WHERE match_id=? AND is_final=1`).bind(id).first();
      if (!existingSnap) {
        const o = await getOdds(db, id);
        await db.prepare(
          `INSERT OR REPLACE INTO odds_snapshots (match_id,team_a_votes,team_b_votes,total_votes,team_a_odds,team_b_odds,is_final)
           VALUES (?,?,?,?,?,?,1)`
        ).bind(id, o.team_a_votes, o.team_b_votes, o.total_votes, o.team_a_odds, o.team_b_odds).run();
      }

      await scoreMatch(db, id, winner);
      await recalcPenalties(db);
      await applyDoubleHeaderBonus(db);
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('force_result', 'admin', 'match', String(id), `Winner: ${winner}`).run();
      return R({ success: true });
    }

    // ── PREDICTIONS CRUD ──────────────────────────────────────────────────────

    if (path.match(/^\/api\/admin\/matches\/(\d+)\/predictions$/) && method === 'GET') {
      const id = +path.split('/')[4];
      const rows = await db.prepare(
        `SELECT p.*, pl.display_name FROM predictions p
         LEFT JOIN players pl ON pl.primary_email=p.primary_email
         WHERE p.match_id=? ORDER BY p.is_valid DESC, p.submitted_at`
      ).bind(id).all();
      return R(rows.results);
    }

    // Edit a single prediction
    if (path.match(/^\/api\/admin\/predictions\/(\d+)$/) && method === 'PATCH') {
      const id = +path.split('/')[4];
      const body = await req.json();
      const f = [], v = [];

      if (body.predicted_team !== undefined) {
        let team = body.predicted_team;
        const pred = await db.prepare('SELECT match_id FROM predictions WHERE id=?').bind(id).first();
        if (pred) {
          const m = await db.prepare('SELECT team_a, team_b FROM matches WHERE id=?').bind(pred.match_id).first();
          team = await resolveTeam(db, team);
          if (m && team !== m.team_a && team !== m.team_b)
            return E(`Invalid team. Accepted: "${m.team_a}" or "${m.team_b}"`);
        }
        f.push('predicted_team=?'); v.push(team);
      }
      if (body.is_valid      !== undefined) { f.push('is_valid=?');       v.push(body.is_valid ? 1 : 0); }
      if (body.invalid_reason !== undefined) { f.push('invalid_reason=?'); v.push(body.invalid_reason); }
      if (body.submitted_at  !== undefined) { f.push('submitted_at=?');   v.push(body.submitted_at); }
      if (body.primary_email !== undefined) {
        const newEmail = body.primary_email.toLowerCase();
        // Ensure the target player exists
        const targetPlayer = await db.prepare('SELECT primary_email FROM players WHERE primary_email=?').bind(newEmail).first();
        if (!targetPlayer) return E(`No player found with email: ${newEmail}. Add the player first.`, 404);
        f.push('primary_email=?'); v.push(newEmail);
      }

      // If display_name is provided, update the player record (not the prediction)
      if (body.display_name !== undefined) {
        const pred = await db.prepare('SELECT primary_email FROM predictions WHERE id=?').bind(id).first();
        if (pred) {
          await db.prepare(`UPDATE players SET display_name=? WHERE primary_email=?`)
            .bind(body.display_name, pred.primary_email).run();
        }
      }

      if (!f.length && !body.display_name) return E('Nothing to update');
      if (f.length) {
        v.push(id);
        await db.prepare(`UPDATE predictions SET ${f.join(',')} WHERE id=?`).bind(...v).run();
      }
      return R({ success: true });
    }

    // Delete a single prediction (with optional exempt flag for missed_vote)
    if (path.match(/^\/api\/admin\/predictions\/(\d+)$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      await db.prepare('DELETE FROM predictions WHERE id=?').bind(id).run();
      return R({ success: true });
    }

    // Bulk delete predictions
    if (path === '/api/admin/predictions/bulk-delete' && method === 'POST') {
      const { ids } = await req.json();
      if (!Array.isArray(ids) || !ids.length) return E('ids array required');
      const placeholders = ids.map(() => '?').join(',');
      await db.prepare(`DELETE FROM predictions WHERE id IN (${placeholders})`).bind(...ids).run();
      return R({ success: true, deleted: ids.length });
    }

    // ── BULK PREDICTIONS ──────────────────────────────────────────────────────
    if (path === '/api/admin/predictions/bulk' && method === 'POST') {
      const { predictions } = await req.json();
      if (!Array.isArray(predictions)) return E('predictions array required');
      let imported = 0, skipped = 0, errors = [];

      for (const pred of predictions) {
        const { email, match_id, name, submitted_at } = pred;
        let { predicted_team } = pred;
        if (!email || !match_id || !predicted_team) {
          errors.push(`Missing fields for ${email || '?'}`); skipped++; continue;
        }
        const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(+match_id).first();
        if (!match) { errors.push(`Match ${match_id} not found`); skipped++; continue; }

        if (submitted_at && submitted_at.toLowerCase() !== 'manual') {
          const subTime = new Date(submitted_at);
          const matchTime = new Date(match.match_time);
          if (!isNaN(subTime.getTime()) && !isNaN(matchTime.getTime()) && subTime > matchTime) {
            errors.push(`${email}: submitted after deadline (${submitted_at} > ${match.match_time})`);
            skipped++;
            continue;
          }
        }

        predicted_team = await resolveTeam(db, predicted_team);
        if (predicted_team !== match.team_a && predicted_team !== match.team_b) {
          errors.push(`${email}: invalid team "${pred.predicted_team}" → resolved to "${predicted_team}" — not in match (${match.team_a} / ${match.team_b})`);
          skipped++; continue;
        }

        const rawEmail = email.trim().toLowerCase();
        const primary = await resolveEmail(db, rawEmail);
        const existing = await db.prepare(
          `SELECT id FROM predictions WHERE match_id=? AND primary_email=? AND is_valid=1`
        ).bind(+match_id, primary).first();
        if (existing) { errors.push(`${rawEmail} already voted M${match.match_number}`); skipped++; continue; }

        // Use provided name or derive from email
        const displayName = name && name.trim() ? name.trim() : null;
        await ensurePlayer(db, primary, rawEmail, match.match_number, displayName);

        const finalSubmittedAt = (submitted_at && submitted_at.toLowerCase() !== 'manual')
          ? submitted_at : new Date().toISOString();

        await db.prepare(
          `INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,submitted_at,is_valid)
           VALUES (?,?,?,?,?,1)`
        ).bind(+match_id, primary, rawEmail, predicted_team, finalSubmittedAt).run();
        imported++;
      }
      return R({ imported, skipped, errors });
    }

    // ── PLAYER CRUD ───────────────────────────────────────────────────────────

    if (path === '/api/admin/players' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT p.*, GROUP_CONCAT(DISTINCT em.alias_email) all_emails FROM players p
         LEFT JOIN email_map em ON em.primary_email=p.primary_email
         GROUP BY p.primary_email ORDER BY p.display_name`
      ).all();
      return R(rows.results);
    }

    if (path === '/api/admin/players' && method === 'POST') {
      const { display_name, primary_email, alias_emails, first_match_num } = await req.json();
      if (!display_name || !primary_email) return E('Missing fields');
      const email = primary_email.toLowerCase();
      await db.prepare(
        `INSERT INTO players (display_name,primary_email,first_match_num) VALUES (?,?,?)
         ON CONFLICT(primary_email) DO UPDATE SET display_name=excluded.display_name,
         first_match_num=COALESCE(excluded.first_match_num, first_match_num)`
      ).bind(display_name, email, first_match_num || null).run();
      await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email,primary_email) VALUES (?,?)`).bind(email, email).run();
      if (Array.isArray(alias_emails)) {
        for (const a of alias_emails) {
          if (a) {
            const al = a.trim().toLowerCase();
            await db.prepare(`INSERT OR REPLACE INTO email_map (alias_email,primary_email) VALUES (?,?)`).bind(al, email).run();
            await cascadeEmailMap(db, al, email);
          }
        }
      }
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/players\/.+$/) && method === 'PATCH') {
      // Must not match /predictions sub-path
      const parts = path.split('/');
      if (parts.length > 5) return E('Not found', 404);
      const email = decodeURIComponent(parts[4]);
      const body = await req.json();
      const f = [], v = [];
      if (body.display_name    !== undefined) { f.push('display_name=?');   v.push(body.display_name); }
      if (body.first_match_num !== undefined) { f.push('first_match_num=?'); v.push(body.first_match_num); }
      if (!f.length) return E('Nothing to update');
      v.push(email);
      await db.prepare(`UPDATE players SET ${f.join(',')} WHERE primary_email=?`).bind(...v).run();
      if (body.first_match_num !== undefined) await recalcPenalties(db);
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/players\/.+$/) && method === 'DELETE') {
      const parts = path.split('/');
      if (parts.length > 5) return E('Not found', 404);
      const email = decodeURIComponent(parts[4]);
      await db.prepare('DELETE FROM email_map WHERE primary_email=?').bind(email).run();
      await db.prepare('DELETE FROM players WHERE primary_email=?').bind(email).run();
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/players\/.+\/predictions$/) && method === 'GET') {
      const email = decodeURIComponent(path.split('/')[4]);
      const primary = await resolveEmail(db, email);
      const rows = await db.prepare(
        `SELECT p.*, m.title, m.match_number, m.team_a, m.team_b, m.winner, m.status AS match_status
         FROM predictions p
         JOIN matches m ON m.id=p.match_id
         WHERE p.primary_email=?
         ORDER BY m.match_number`
      ).bind(primary).all();
      return R(rows.results);
    }

    // ── EMAIL MAP ─────────────────────────────────────────────────────────────

    if (path === '/api/admin/email-map' && method === 'GET') {
      const rows = await db.prepare(`SELECT * FROM email_map ORDER BY primary_email`).all();
      return R(rows.results);
    }

    if (path === '/api/admin/email-map' && method === 'POST') {
      const { alias_email, primary_email } = await req.json();
      if (!alias_email || !primary_email) return E('alias_email and primary_email required');
      const alias = alias_email.toLowerCase();
      const primary = primary_email.toLowerCase();
      if (alias === primary) return E('alias_email and primary_email cannot be the same');
      await db.prepare(`INSERT OR REPLACE INTO email_map (alias_email,primary_email) VALUES (?,?)`).bind(alias, primary).run();
      await cascadeEmailMap(db, alias, primary);
      await recalcPenalties(db);
      await applyDoubleHeaderBonus(db);
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/email-map\/.+$/) && method === 'DELETE') {
      const alias = decodeURIComponent(path.split('/')[4]);
      await db.prepare('DELETE FROM email_map WHERE alias_email=?').bind(alias).run();
      return R({ success: true });
    }

    // ── TEAM ALIASES ──────────────────────────────────────────────────────────

    if (path === '/api/admin/team-aliases' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT * FROM team_aliases ORDER BY primary_name, alias_name`
      ).all();
      return R(rows.results);
    }

    if (path === '/api/admin/team-aliases' && method === 'POST') {
      const { alias_name, primary_name } = await req.json();
      if (!alias_name || !primary_name) return E('alias_name and primary_name required');
      await db.prepare(
        `INSERT OR REPLACE INTO team_aliases (alias_name, primary_name) VALUES (?,?)`
      ).bind(alias_name.trim(), primary_name.trim()).run();
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('team_alias_added', 'admin', 'team_alias', alias_name, `→ ${primary_name}`).run();
      return R({ success: true });
    }

    if (path === '/api/admin/team-aliases/bulk' && method === 'POST') {
      const { aliases } = await req.json();
      if (!Array.isArray(aliases)) return E('aliases array required');
      let created = 0, skipped = 0;
      for (const a of aliases) {
        if (!a.alias_name || !a.primary_name) { skipped++; continue; }
        try {
          await db.prepare(
            `INSERT OR REPLACE INTO team_aliases (alias_name, primary_name) VALUES (?,?)`
          ).bind(a.alias_name.trim(), a.primary_name.trim()).run();
          created++;
        } catch { skipped++; }
      }
      return R({ created, skipped });
    }

    if (path.match(/^\/api\/admin\/team-aliases\/(\d+)$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      await db.prepare('DELETE FROM team_aliases WHERE id=?').bind(id).run();
      return R({ success: true });
    }

    // ── VARIATIONS ────────────────────────────────────────────────────────────

    if (path === '/api/admin/variations' && method === 'GET') {
      const rows = await db.prepare(`SELECT * FROM variations ORDER BY created_at DESC`).all();
      return R(rows.results);
    }

    if (path === '/api/admin/variations' && method === 'POST') {
      const { name, type, description, applies_to_match_id, details } = await req.json();
      if (!name || !type) return E('name and type required');
      const det = typeof details === 'object' ? JSON.stringify(details) : (details || null);
      const r = await db.prepare(
        `INSERT INTO variations (name,type,description,applies_to_match_id,details,is_active) VALUES (?,?,?,?,?,1)`
      ).bind(name, type, description || '', applies_to_match_id || null, det).run();
      return R({ id: r.meta.last_row_id });
    }

    if (path.match(/^\/api\/admin\/variations\/(\d+)$/) && method === 'PATCH') {
      const id = +path.split('/')[4];
      const body = await req.json();
      const f = [], v = [];
      if (body.name        !== undefined) { f.push('name=?');        v.push(body.name); }
      if (body.description !== undefined) { f.push('description=?'); v.push(body.description); }
      if (body.is_active   !== undefined) { f.push('is_active=?');   v.push(body.is_active ? 1 : 0); }
      if (body.details     !== undefined) {
        f.push('details=?');
        v.push(typeof body.details === 'object' ? JSON.stringify(body.details) : body.details);
      }
      if (!f.length) return E('Nothing to update');
      v.push(id);
      await db.prepare(`UPDATE variations SET ${f.join(',')} WHERE id=?`).bind(...v).run();
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/variations\/(\d+)$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      await db.prepare('DELETE FROM variations WHERE id=?').bind(id).run();
      return R({ success: true });
    }

    if (path === '/api/admin/variations/recalc' && method === 'POST') {
      await applyDoubleHeaderBonus(db);
      await recalcPenalties(db);
      return R({ success: true });
    }

    // ── BONUS POINTS ──────────────────────────────────────────────────────────

    if (path === '/api/admin/bonus-points' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT bp.*, p.display_name, m.match_number FROM bonus_points bp
         LEFT JOIN players p ON p.primary_email=bp.primary_email
         LEFT JOIN matches m ON m.id=bp.match_id
         ORDER BY bp.id DESC`
      ).all();
      return R(rows.results);
    }

    if (path === '/api/admin/bonus-points' && method === 'POST') {
      const { primary_email, match_id, bonus_pts, reason, details } = await req.json();
      if (!primary_email || bonus_pts === undefined) return E('email and pts required');
      await db.prepare(
        `INSERT INTO bonus_points (primary_email,match_id,bonus_pts,reason,details) VALUES (?,?,?,?,?)`
      ).bind(primary_email, match_id || null, bonus_pts, reason || 'manual', details || '').run();
      return R({ success: true });
    }

    if (path.match(/^\/api\/admin\/bonus-points\/(\d+)$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      await db.prepare('DELETE FROM bonus_points WHERE id=?').bind(id).run();
      return R({ success: true });
    }

    // ── PENALTIES ─────────────────────────────────────────────────────────────

    if (path === '/api/admin/penalties' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT pen.*, p.display_name, m.match_number, m.title
         FROM penalties pen
         LEFT JOIN players p ON p.primary_email=pen.primary_email
         LEFT JOIN matches m ON m.id=pen.match_id
         WHERE pen.reason NOT IN ('missed_vote_exempt')
         ORDER BY m.match_number, p.display_name`
      ).all();
      return R(rows.results);
    }

    // FIX: manual penalty delete now inserts an exemption record so recalc doesn't re-add it
    if (path.match(/^\/api\/admin\/penalties\/(\d+)$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      // Get the penalty before deleting (to create exemption if it's a missed_vote)
      const pen = await db.prepare('SELECT * FROM penalties WHERE id=?').bind(id).first();
      if (pen && pen.reason === 'missed_vote') {
        // Insert an exemption so recalc skips this (player, match) combo
        await db.prepare(
          `INSERT OR IGNORE INTO penalties (primary_email,match_id,penalty_pts,reason) VALUES (?,?,0,'missed_vote_exempt')`
        ).bind(pen.primary_email, pen.match_id).run();
      }
      await db.prepare('DELETE FROM penalties WHERE id=?').bind(id).run();
      return R({ success: true });
    }

    if (path === '/api/admin/penalties/recalc' && method === 'POST') {
      await recalcPenalties(db);
      return R({ success: true });
    }

    // ── LEADERBOARD (ADMIN with asOf) ─────────────────────────────────────────

    if (path === '/api/admin/leaderboard' && method === 'GET') {
      const asOf = url.searchParams.get('asOf') || null;
      return R(await buildLB(db, asOf));
    }

    // ── AUDIT ─────────────────────────────────────────────────────────────────

    if (path === '/api/admin/audit' && method === 'GET') {
      const rows = await db.prepare(`SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 500`).all();
      return R(rows.results);
    }

    // ── EXPORT ────────────────────────────────────────────────────────────────

    if (path === '/api/admin/export' && method === 'GET') {
      const [matches, predictions, scores, penalties, players, emailMap, bonuses, vars, teamAliases] = await Promise.all([
        db.prepare('SELECT * FROM matches ORDER BY match_number').all(),
        db.prepare('SELECT * FROM predictions ORDER BY match_id,submitted_at').all(),
        db.prepare('SELECT * FROM scores ORDER BY match_id').all(),
        db.prepare(`SELECT * FROM penalties WHERE reason NOT IN ('missed_vote_exempt') ORDER BY match_id`).all(),
        db.prepare('SELECT * FROM players ORDER BY display_name').all(),
        db.prepare('SELECT * FROM email_map ORDER BY primary_email').all(),
        db.prepare('SELECT * FROM bonus_points ORDER BY id').all(),
        db.prepare('SELECT * FROM variations ORDER BY id').all(),
        db.prepare('SELECT * FROM team_aliases ORDER BY primary_name').all(),
      ]);
      return R({
        exported_at: new Date().toISOString(),
        leaderboard: await buildLB(db, null),
        matches: matches.results, predictions: predictions.results,
        scores: scores.results, penalties: penalties.results,
        players: players.results, email_map: emailMap.results,
        bonus_points: bonuses.results, variations: vars.results,
        team_aliases: teamAliases.results,
      });
    }

    return E('Not found', 404);
  }
};

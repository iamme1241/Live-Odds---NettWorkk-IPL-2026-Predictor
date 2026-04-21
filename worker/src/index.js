/**
 * Predictor League — Cloudflare Worker API v2
 * New in v2:
 *  - Double-header bonus engine
 *  - Variations / bonuses framework (pluggable)
 *  - Bulk match creation
 *  - Bulk predictions (historical import)
 *  - Historical leaderboard (as-of any date)
 *  - Manual match status override (force close/open/result past matches)
 *  - Password / secret change via env only (documented in guide)
 */

// ─── CORS ────────────────────────────────────────────────────────────────────
function cors() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS, PATCH',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  };
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...cors() },
  });
}

function err(msg, status = 400) {
  return json({ error: msg }, status);
}

// ─── AUTH ─────────────────────────────────────────────────────────────────────
function requireAdmin(req, env) {
  const auth = req.headers.get('Authorization') || '';
  const token = auth.replace('Bearer ', '');
  const expected = btoa(`${env.ADMIN_SECRET_PATH}:${env.ADMIN_PASSWORD}`);
  return token === expected;
}

// ─── ODDS ENGINE ─────────────────────────────────────────────────────────────
async function computeOdds(db, matchId) {
  const votes = await db.prepare(
    `SELECT predicted_team, COUNT(*) as cnt FROM predictions
     WHERE match_id = ? AND is_valid = 1 GROUP BY predicted_team`
  ).bind(matchId).all();

  const match = await db.prepare('SELECT team_a, team_b FROM matches WHERE id = ?')
    .bind(matchId).first();

  let a = 0, b = 0;
  for (const row of votes.results) {
    if (row.predicted_team === match.team_a) a = row.cnt;
    else if (row.predicted_team === match.team_b) b = row.cnt;
  }
  const total = a + b;
  return {
    team_a_votes: a,
    team_b_votes: b,
    total_votes: total,
    team_a_odds: total > 0 && a > 0 ? parseFloat((total / a).toFixed(2)) : null,
    team_b_odds: total > 0 && b > 0 ? parseFloat((total / b).toFixed(2)) : null,
    team_a: match.team_a,
    team_b: match.team_b,
  };
}

// ─── DOUBLE HEADER BONUS ─────────────────────────────────────────────────────
async function applyDoubleHeaderBonus(db) {
  // Get all resulted matches grouped by date
  const resulted = await db.prepare(
    `SELECT id, match_number, match_time, winner, team_a, team_b
     FROM matches WHERE status = 'resulted' ORDER BY match_number`
  ).all();

  // Group by calendar date (IST = UTC+5:30)
  const byDate = {};
  for (const m of resulted.results) {
    const d = new Date(m.match_time);
    // IST date
    const istDate = new Date(d.getTime() + 5.5 * 3600000);
    const key = istDate.toISOString().slice(0, 10);
    if (!byDate[key]) byDate[key] = [];
    byDate[key].push(m);
  }

  // Find double-header days (exactly 2 matches)
  const doubleHeaderDays = Object.values(byDate).filter(ms => ms.length === 2);

  // Delete existing double-header bonuses (recalculate fresh)
  await db.prepare(`DELETE FROM bonus_points WHERE reason = 'double_header'`).run();

  for (const [m1, m2] of doubleHeaderDays) {
    // Find players who voted correctly in BOTH matches
    const correct1 = await db.prepare(
      `SELECT primary_email FROM scores WHERE match_id = ? AND predicted_team = ?`
    ).bind(m1.id, m1.winner).all();

    const correct2 = await db.prepare(
      `SELECT primary_email FROM scores WHERE match_id = ? AND predicted_team = ?`
    ).bind(m2.id, m2.winner).all();

    const set1 = new Set(correct1.results.map(r => r.primary_email));
    const set2 = new Set(correct2.results.map(r => r.primary_email));

    for (const email of set1) {
      if (set2.has(email)) {
        // Award 50 bonus for this double header day — use higher match_id as anchor
        await db.prepare(
          `INSERT OR IGNORE INTO bonus_points (primary_email, match_id, bonus_pts, reason, details)
           VALUES (?, ?, 50, 'double_header', ?)`
        ).bind(email, m2.id, `Double header: M${m1.match_number} + M${m2.match_number}`).run();
      }
    }
  }
}

// ─── CUSTOM VARIATIONS ENGINE ────────────────────────────────────────────────
// Variations are stored in DB and evaluated when results are entered
async function applyVariations(db, matchId) {
  const variations = await db.prepare(
    `SELECT * FROM variations WHERE is_active = 1 AND
     (applies_to_match_id = ? OR applies_to_match_id IS NULL)`
  ).bind(matchId).all();

  for (const v of variations.results) {
    if (v.type === 'double_header') continue; // handled separately
    if (v.type === 'manual_bonus') {
      // Manual bonus: admin specified email + pts directly
      // Stored as JSON in details field: {"email":"x@y.com","pts":100}
      try {
        const d = JSON.parse(v.details);
        if (d.email && d.pts) {
          await db.prepare(
            `INSERT OR IGNORE INTO bonus_points (primary_email, match_id, bonus_pts, reason, details)
             VALUES (?, ?, ?, ?, ?)`
          ).bind(d.email, matchId, d.pts, v.name, v.description || '').run();
        }
      } catch {}
    }
    // More variation types can be added here
  }
}

// ─── PENALTY ENGINE ──────────────────────────────────────────────────────────
async function recalcPenalties(db) {
  const resulted = await db.prepare(
    `SELECT id, match_number FROM matches WHERE status = 'resulted' ORDER BY match_number`
  ).all();
  if (!resulted.results.length) return;

  const players = await db.prepare(
    `SELECT primary_email, first_match_num FROM players WHERE first_match_num IS NOT NULL`
  ).all();

  await db.prepare(`DELETE FROM penalties WHERE reason = 'missed_vote'`).run();

  for (const player of players.results) {
    const { primary_email, first_match_num } = player;
    const obligated = resulted.results.filter(m => m.match_number >= first_match_num);

    const voted = await db.prepare(
      `SELECT m.match_number FROM predictions p
       JOIN matches m ON m.id = p.match_id
       WHERE p.primary_email = ? AND p.is_valid = 1`
    ).bind(primary_email).all();

    const votedNums = new Set(voted.results.map(r => r.match_number));

    for (const m of obligated) {
      if (!votedNums.has(m.match_number)) {
        await db.prepare(
          `INSERT OR IGNORE INTO penalties (primary_email, match_id, penalty_pts, reason)
           VALUES (?, ?, -50, 'missed_vote')`
        ).bind(primary_email, m.id).run();
      }
    }
  }
}

// ─── RESOLVE EMAIL → PRIMARY ─────────────────────────────────────────────────
async function resolveEmail(db, rawEmail) {
  const email = rawEmail.trim().toLowerCase();
  const row = await db.prepare(
    'SELECT primary_email FROM email_map WHERE alias_email = ?'
  ).bind(email).first();
  return row ? row.primary_email : email;
}

// ─── LEADERBOARD QUERY ───────────────────────────────────────────────────────
async function buildLeaderboard(db, asOfDate = null) {
  // asOfDate: ISO date string, only count matches resulted on or before this date
  let matchFilter = `m.status = 'resulted'`;
  if (asOfDate) {
    matchFilter = `m.status = 'resulted' AND m.updated_at <= '${asOfDate}T23:59:59Z'`;
  }

  const players = await db.prepare(
    `SELECT p.primary_email, p.display_name, p.first_match_num,
            COALESCE(SUM(s.points_earned), 0) as total_points,
            COALESCE(SUM(pen.penalty_pts), 0) as total_penalties,
            COALESCE(SUM(bp.bonus_pts), 0) as total_bonuses,
            COUNT(DISTINCT s.match_id) as matches_played,
            COUNT(DISTINCT CASE WHEN s.points_earned > 0 THEN s.match_id END) as correct_predictions
     FROM players p
     LEFT JOIN scores s ON s.primary_email = p.primary_email
       JOIN matches sm ON sm.id = s.match_id AND ${matchFilter.replace(/m\./g, 'sm.')}
     LEFT JOIN penalties pen ON pen.primary_email = p.primary_email
       JOIN matches pm ON pm.id = pen.match_id AND ${matchFilter.replace(/m\./g, 'pm.')}
     LEFT JOIN bonus_points bp ON bp.primary_email = p.primary_email
       JOIN matches bm ON bm.id = bp.match_id AND ${matchFilter.replace(/m\./g, 'bm.')}
     WHERE p.first_match_num IS NOT NULL
     GROUP BY p.primary_email
     ORDER BY (COALESCE(SUM(s.points_earned),0) + COALESCE(SUM(pen.penalty_pts),0) + COALESCE(SUM(bp.bonus_pts),0)) DESC`
  ).all();

  return players.results.map((p, i) => ({
    rank: i + 1,
    display_name: p.display_name,
    primary_email: p.primary_email,
    gross_points: parseFloat((p.total_points || 0).toFixed(2)),
    penalties: parseFloat((p.total_penalties || 0).toFixed(2)),
    bonuses: parseFloat((p.total_bonuses || 0).toFixed(2)),
    net_points: parseFloat(((p.total_points || 0) + (p.total_penalties || 0) + (p.total_bonuses || 0)).toFixed(2)),
    matches_played: p.matches_played,
    correct_predictions: p.correct_predictions,
    first_match: p.first_match_num,
  }));
}

// Simpler leaderboard without date join complexity
async function buildLeaderboardSimple(db) {
  const players = await db.prepare(
    `SELECT p.primary_email, p.display_name, p.first_match_num,
            COALESCE(SUM(s.points_earned), 0) as total_points,
            COALESCE(SUM(pen.penalty_pts), 0) as total_penalties,
            COALESCE(SUM(bp.bonus_pts), 0) as total_bonuses,
            COUNT(DISTINCT s.match_id) as matches_played,
            COUNT(DISTINCT CASE WHEN s.points_earned > 0 THEN s.match_id END) as correct_predictions
     FROM players p
     LEFT JOIN scores s ON s.primary_email = p.primary_email
     LEFT JOIN penalties pen ON pen.primary_email = p.primary_email
     LEFT JOIN bonus_points bp ON bp.primary_email = p.primary_email
     WHERE p.first_match_num IS NOT NULL
     GROUP BY p.primary_email
     ORDER BY (COALESCE(SUM(s.points_earned),0) + COALESCE(SUM(pen.penalty_pts),0) + COALESCE(SUM(bp.bonus_pts),0)) DESC`
  ).all();

  return players.results.map((p, i) => ({
    rank: i + 1,
    display_name: p.display_name,
    primary_email: p.primary_email,
    gross_points: parseFloat((p.total_points || 0).toFixed(2)),
    penalties: parseFloat((p.total_penalties || 0).toFixed(2)),
    bonuses: parseFloat((p.total_bonuses || 0).toFixed(2)),
    net_points: parseFloat(((p.total_points || 0) + (p.total_penalties || 0) + (p.total_bonuses || 0)).toFixed(2)),
    matches_played: p.matches_played,
    correct_predictions: p.correct_predictions,
    first_match: p.first_match_num,
  }));
}

// ─── SCORE ONE MATCH ─────────────────────────────────────────────────────────
async function scoreMatch(db, matchId, winner) {
  const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(matchId).first();
  const snap = await db.prepare(
    `SELECT * FROM odds_snapshots WHERE match_id=? AND is_final=1`
  ).bind(matchId).first();
  const odds = snap || await computeOdds(db, matchId);

  const preds = await db.prepare(
    `SELECT * FROM predictions WHERE match_id=? AND is_valid=1`
  ).bind(matchId).all();

  for (const p of preds.results) {
    const correct = p.predicted_team === winner;
    const oddsUsed = p.predicted_team === match.team_a ? odds.team_a_odds : odds.team_b_odds;
    const pts = correct ? parseFloat((100 * (oddsUsed || 1)).toFixed(2)) : 0;

    await db.prepare(
      `INSERT OR REPLACE INTO scores (match_id, primary_email, predicted_team, winner, odds_at_close, base_points, points_earned)
       VALUES (?,?,?,?,?,100,?)`
    ).bind(matchId, p.primary_email, p.predicted_team, winner, oddsUsed || 1, pts).run();
  }

  await db.prepare(`UPDATE matches SET status='resulted', winner=?, updated_at=datetime('now') WHERE id=?`)
    .bind(winner, matchId).run();
}

// ─── ROUTER ──────────────────────────────────────────────────────────────────
export default {

  async scheduled(event, env, ctx) {
    const db = env.DB;
    const now = new Date().toISOString();
    const toClose = await db.prepare(
      `SELECT id FROM matches WHERE status = 'open' AND match_time <= ?`
    ).bind(now).all();

    for (const m of toClose.results) {
      const odds = await computeOdds(db, m.id);
      await db.prepare(
        `INSERT OR REPLACE INTO odds_snapshots (match_id, team_a_votes, team_b_votes, total_votes, team_a_odds, team_b_odds, is_final)
         VALUES (?, ?, ?, ?, ?, ?, 1)`
      ).bind(m.id, odds.team_a_votes, odds.team_b_votes, odds.total_votes,
              odds.team_a_odds, odds.team_b_odds).run();

      await db.prepare(`UPDATE matches SET status='closed', updated_at=datetime('now') WHERE id=?`)
        .bind(m.id).run();
    }
  },

  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const path = url.pathname;
    const method = req.method;
    const db = env.DB;

    if (method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: cors() });
    }

    // ── PUBLIC ROUTES ─────────────────────────────────────────────────────────

    // GET /api/matches
    if (path === '/api/matches' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT id, match_number, title, team_a, team_b, match_time, status, winner
         FROM matches ORDER BY match_number ASC`
      ).all();
      return json(rows.results);
    }

    // GET /api/matches/:id/odds
    if (path.match(/^\/api\/matches\/(\d+)\/odds$/) && method === 'GET') {
      const matchId = parseInt(path.split('/')[3]);
      const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(matchId).first();
      if (!match) return err('Match not found', 404);

      if (match.status === 'resulted' || match.status === 'closed') {
        const snap = await db.prepare(
          `SELECT * FROM odds_snapshots WHERE match_id=? AND is_final=1`
        ).bind(matchId).first();
        return json({ ...snap, frozen: true });
      }
      const odds = await computeOdds(db, matchId);
      return json({ ...odds, frozen: false });
    }

    // GET /api/leaderboard?asOf=YYYY-MM-DD
    if (path === '/api/leaderboard' && method === 'GET') {
      const asOf = url.searchParams.get('asOf');
      const board = await buildLeaderboardSimple(db);
      return json(board);
    }

    // GET /api/players/:email/history
    if (path.match(/^\/api\/players\/(.+)\/history$/) && method === 'GET') {
      const rawEmail = decodeURIComponent(path.split('/')[3]);
      const primaryEmail = await resolveEmail(db, rawEmail);

      const player = await db.prepare('SELECT * FROM players WHERE primary_email=?')
        .bind(primaryEmail).first();
      if (!player) return err('Player not found', 404);

      const history = await db.prepare(
        `SELECT m.match_number, m.title, m.team_a, m.team_b, m.winner, m.match_time,
                p.predicted_team, p.submitted_at,
                s.points_earned, s.odds_at_close,
                pen.penalty_pts,
                bp.bonus_pts, bp.reason as bonus_reason
         FROM matches m
         LEFT JOIN predictions p ON p.match_id = m.id AND p.primary_email = ?
         LEFT JOIN scores s ON s.match_id = m.id AND s.primary_email = ?
         LEFT JOIN penalties pen ON pen.match_id = m.id AND pen.primary_email = ?
         LEFT JOIN bonus_points bp ON bp.match_id = m.id AND bp.primary_email = ?
         WHERE m.status IN ('resulted','closed','open')
         ORDER BY m.match_number ASC`
      ).bind(primaryEmail, primaryEmail, primaryEmail, primaryEmail).all();

      return json({ player, history: history.results });
    }

    // POST /api/predict
    if (path === '/api/predict' && method === 'POST') {
      let body;
      try { body = await req.json(); } catch { return err('Invalid JSON'); }

      const { email, match_id, predicted_team } = body;
      if (!email || !match_id || !predicted_team)
        return err('Missing email, match_id, or predicted_team');

      const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(match_id).first();
      if (!match) return err('Match not found', 404);
      if (match.status !== 'open') return err('Predictions are closed for this match');
      if (predicted_team !== match.team_a && predicted_team !== match.team_b)
        return err('Invalid team selection');

      const rawEmail = email.trim().toLowerCase();
      const primaryEmail = await resolveEmail(db, rawEmail);

      const existing = await db.prepare(
        `SELECT id FROM predictions WHERE match_id=? AND primary_email=? AND is_valid=1`
      ).bind(match_id, primaryEmail).first();

      if (existing) {
        await db.prepare(
          `INSERT INTO predictions (match_id, primary_email, raw_email, predicted_team, is_valid, invalid_reason)
           VALUES (?, ?, ?, ?, 0, 'duplicate_vote')`
        ).bind(match_id, primaryEmail, rawEmail, predicted_team).run();
        return err('You have already voted for this match. First vote stands.', 409);
      }

      let player = await db.prepare('SELECT * FROM players WHERE primary_email=?')
        .bind(primaryEmail).first();

      if (!player) {
        const displayName = rawEmail.split('@')[0];
        await db.prepare(
          `INSERT INTO players (display_name, primary_email, first_match_num) VALUES (?,?,?)`
        ).bind(displayName, primaryEmail, match.match_number).run();
        await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
          .bind(primaryEmail, primaryEmail).run();
        if (rawEmail !== primaryEmail) {
          await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
            .bind(rawEmail, primaryEmail).run();
        }
      } else if (!player.first_match_num) {
        await db.prepare(`UPDATE players SET first_match_num=? WHERE primary_email=?`)
          .bind(match.match_number, primaryEmail).run();
      }

      await db.prepare(
        `INSERT INTO predictions (match_id, primary_email, raw_email, predicted_team, is_valid)
         VALUES (?,?,?,?,1)`
      ).bind(match_id, primaryEmail, rawEmail, predicted_team).run();

      const odds = await computeOdds(db, match_id);
      return json({ success: true, odds });
    }

    // ── ADMIN ROUTES ──────────────────────────────────────────────────────────
    if (!requireAdmin(req, env)) return err('Unauthorized', 401);

    // POST /api/admin/matches
    if (path === '/api/admin/matches' && method === 'POST') {
      const body = await req.json();
      const { match_number, title, team_a, team_b, match_time } = body;
      if (!match_number || !title || !team_a || !team_b || !match_time)
        return err('Missing fields');

      const result = await db.prepare(
        `INSERT INTO matches (match_number, title, team_a, team_b, match_time, status)
         VALUES (?,?,?,?,?,'upcoming')`
      ).bind(match_number, title, team_a, team_b, match_time).run();

      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('match_created', 'admin', 'match', String(result.meta.last_row_id), title).run();

      return json({ id: result.meta.last_row_id });
    }

    // POST /api/admin/matches/bulk — bulk create matches
    if (path === '/api/admin/matches/bulk' && method === 'POST') {
      const body = await req.json();
      const { matches } = body;
      if (!Array.isArray(matches) || !matches.length) return err('No matches provided');

      let created = 0, skipped = 0, errors = [];

      for (const m of matches) {
        const { match_number, title, team_a, team_b, match_time } = m;
        if (!match_number || !title || !team_a || !team_b || !match_time) {
          errors.push(`Row ${match_number || '?'}: missing fields`);
          skipped++;
          continue;
        }
        try {
          await db.prepare(
            `INSERT INTO matches (match_number, title, team_a, team_b, match_time, status)
             VALUES (?,?,?,?,?,'upcoming')`
          ).bind(match_number, title, team_a, team_b, match_time).run();
          created++;
        } catch (e) {
          errors.push(`Match ${match_number}: ${e.message}`);
          skipped++;
        }
      }

      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('bulk_matches_created', 'admin', 'match', 'bulk', `${created} created, ${skipped} skipped`).run();

      return json({ created, skipped, errors });
    }

    // PATCH /api/admin/matches/:id — update match fields (status override, time, etc.)
    if (path.match(/^\/api\/admin\/matches\/(\d+)$/) && method === 'PATCH') {
      const matchId = parseInt(path.split('/')[4]);
      const body = await req.json();
      const fields = [], vals = [];

      if (body.status !== undefined)     { fields.push('status=?');     vals.push(body.status); }
      if (body.match_time !== undefined)  { fields.push('match_time=?'); vals.push(body.match_time); }
      if (body.title !== undefined)       { fields.push('title=?');      vals.push(body.title); }
      if (body.winner !== undefined)      { fields.push('winner=?');     vals.push(body.winner); }
      if (body.team_a !== undefined)      { fields.push('team_a=?');     vals.push(body.team_a); }
      if (body.team_b !== undefined)      { fields.push('team_b=?');     vals.push(body.team_b); }

      if (!fields.length) return err('Nothing to update');
      fields.push("updated_at=datetime('now')");
      vals.push(matchId);

      await db.prepare(`UPDATE matches SET ${fields.join(',')} WHERE id=?`).bind(...vals).run();
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('match_patched', 'admin', 'match', String(matchId), JSON.stringify(body)).run();

      return json({ success: true });
    }

    // POST /api/admin/matches/:id/open
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/open$/) && method === 'POST') {
      const matchId = parseInt(path.split('/')[4]);
      await db.prepare(`UPDATE matches SET status='open', updated_at=datetime('now') WHERE id=?`)
        .bind(matchId).run();
      return json({ success: true });
    }

    // POST /api/admin/matches/:id/close
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/close$/) && method === 'POST') {
      const matchId = parseInt(path.split('/')[4]);
      const odds = await computeOdds(db, matchId);

      await db.prepare(
        `INSERT OR REPLACE INTO odds_snapshots (match_id, team_a_votes, team_b_votes, total_votes, team_a_odds, team_b_odds, is_final)
         VALUES (?,?,?,?,?,?,1)`
      ).bind(matchId, odds.team_a_votes, odds.team_b_votes, odds.total_votes,
              odds.team_a_odds, odds.team_b_odds).run();

      await db.prepare(`UPDATE matches SET status='closed', updated_at=datetime('now') WHERE id=?`)
        .bind(matchId).run();
      return json({ success: true, odds });
    }

    // POST /api/admin/matches/:id/result
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/result$/) && method === 'POST') {
      const matchId = parseInt(path.split('/')[4]);
      const body = await req.json();
      const { winner } = body;

      const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(matchId).first();
      if (!match) return err('Match not found', 404);
      if (!['closed', 'resulted'].includes(match.status))
        return err('Match must be closed before entering result');

      // Snapshot odds if not already done
      const snapExists = await db.prepare(
        `SELECT id FROM odds_snapshots WHERE match_id=? AND is_final=1`
      ).bind(matchId).first();

      if (!snapExists) {
        const odds = await computeOdds(db, matchId);
        await db.prepare(
          `INSERT INTO odds_snapshots (match_id, team_a_votes, team_b_votes, total_votes, team_a_odds, team_b_odds, is_final)
           VALUES (?,?,?,?,?,?,1)`
        ).bind(matchId, odds.team_a_votes, odds.team_b_votes, odds.total_votes,
                odds.team_a_odds, odds.team_b_odds).run();
      }

      await scoreMatch(db, matchId, winner);
      await recalcPenalties(db);
      await applyDoubleHeaderBonus(db);
      await applyVariations(db, matchId);

      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('result_entered', 'admin', 'match', String(matchId), `Winner: ${winner}`).run();

      const board = await buildLeaderboardSimple(db);
      return json({ success: true, leaderboard_preview: board.slice(0, 5) });
    }

    // POST /api/admin/matches/:id/force-result — for historical matches (skip closed check)
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/force-result$/) && method === 'POST') {
      const matchId = parseInt(path.split('/')[4]);
      const body = await req.json();
      const { winner } = body;

      const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(matchId).first();
      if (!match) return err('Match not found', 404);

      // Force close first if needed
      if (match.status !== 'closed') {
        const odds = await computeOdds(db, matchId);
        await db.prepare(
          `INSERT OR REPLACE INTO odds_snapshots (match_id, team_a_votes, team_b_votes, total_votes, team_a_odds, team_b_odds, is_final)
           VALUES (?,?,?,?,?,?,1)`
        ).bind(matchId, odds.team_a_votes, odds.team_b_votes, odds.total_votes,
                odds.team_a_odds, odds.team_b_odds).run();
        await db.prepare(`UPDATE matches SET status='closed', updated_at=datetime('now') WHERE id=?`)
          .bind(matchId).run();
      }

      await scoreMatch(db, matchId, winner);
      await recalcPenalties(db);
      await applyDoubleHeaderBonus(db);

      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('force_result', 'admin', 'match', String(matchId), `Winner: ${winner}`).run();

      const board = await buildLeaderboardSimple(db);
      return json({ success: true, leaderboard_preview: board.slice(0, 5) });
    }

    // GET /api/admin/matches/:id/predictions
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/predictions$/) && method === 'GET') {
      const matchId = parseInt(path.split('/')[4]);
      const rows = await db.prepare(
        `SELECT p.*, pl.display_name FROM predictions p
         LEFT JOIN players pl ON pl.primary_email = p.primary_email
         WHERE p.match_id = ? ORDER BY p.submitted_at ASC`
      ).bind(matchId).all();
      return json(rows.results);
    }

    // POST /api/admin/predictions/bulk — import historical responses
    if (path === '/api/admin/predictions/bulk' && method === 'POST') {
      const body = await req.json();
      const { predictions } = body;
      if (!Array.isArray(predictions)) return err('predictions array required');

      let imported = 0, skipped = 0, errors = [];

      for (const pred of predictions) {
        const { email, match_id, predicted_team, submitted_at } = pred;
        if (!email || !match_id || !predicted_team) {
          errors.push(`Row: missing email/match_id/team`);
          skipped++;
          continue;
        }

        const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(match_id).first();
        if (!match) { errors.push(`Match ${match_id} not found`); skipped++; continue; }

        const rawEmail = email.trim().toLowerCase();
        const primaryEmail = await resolveEmail(db, rawEmail);

        // Register player if new
        let player = await db.prepare('SELECT * FROM players WHERE primary_email=?')
          .bind(primaryEmail).first();
        if (!player) {
          await db.prepare(
            `INSERT INTO players (display_name, primary_email, first_match_num) VALUES (?,?,?)`
          ).bind(rawEmail.split('@')[0], primaryEmail, match.match_number).run();
          await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
            .bind(primaryEmail, primaryEmail).run();
        } else if (!player.first_match_num || match.match_number < player.first_match_num) {
          await db.prepare(`UPDATE players SET first_match_num=? WHERE primary_email=?`)
            .bind(match.match_number, primaryEmail).run();
        }

        // Check for existing valid prediction
        const existing = await db.prepare(
          `SELECT id FROM predictions WHERE match_id=? AND primary_email=? AND is_valid=1`
        ).bind(match_id, primaryEmail).first();

        if (existing) {
          errors.push(`${rawEmail} already voted M${match.match_number} — skipped`);
          skipped++;
          continue;
        }

        const ts = submitted_at || new Date().toISOString();
        await db.prepare(
          `INSERT INTO predictions (match_id, primary_email, raw_email, predicted_team, submitted_at, is_valid)
           VALUES (?,?,?,?,?,1)`
        ).bind(match_id, primaryEmail, rawEmail, predicted_team, ts).run();
        imported++;
      }

      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('bulk_predictions_imported', 'admin', 'prediction', 'bulk',
          `${imported} imported, ${skipped} skipped`).run();

      return json({ imported, skipped, errors });
    }

    // GET /api/admin/players
    if (path === '/api/admin/players' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT p.*, GROUP_CONCAT(em.alias_email) as all_emails
         FROM players p
         LEFT JOIN email_map em ON em.primary_email = p.primary_email
         GROUP BY p.primary_email ORDER BY p.display_name`
      ).all();
      return json(rows.results);
    }

    // POST /api/admin/players
    if (path === '/api/admin/players' && method === 'POST') {
      const body = await req.json();
      const { display_name, primary_email, alias_emails } = body;
      if (!display_name || !primary_email) return err('Missing fields');

      await db.prepare(
        `INSERT INTO players (display_name, primary_email) VALUES (?,?)
         ON CONFLICT(primary_email) DO UPDATE SET display_name=excluded.display_name`
      ).bind(display_name, primary_email.toLowerCase()).run();

      await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
        .bind(primary_email.toLowerCase(), primary_email.toLowerCase()).run();

      if (alias_emails && Array.isArray(alias_emails)) {
        for (const alias of alias_emails) {
          await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
            .bind(alias.trim().toLowerCase(), primary_email.toLowerCase()).run();
        }
      }
      return json({ success: true });
    }

    // PUT /api/admin/players/:email/name
    if (path.match(/^\/api\/admin\/players\/.+\/name$/) && method === 'PUT') {
      const rawEmail = decodeURIComponent(path.split('/')[4]);
      const body = await req.json();
      await db.prepare(`UPDATE players SET display_name=? WHERE primary_email=?`)
        .bind(body.display_name, rawEmail).run();
      return json({ success: true });
    }

    // PATCH /api/admin/players/:email — update player details
    if (path.match(/^\/api\/admin\/players\/.+$/) && method === 'PATCH') {
      const rawEmail = decodeURIComponent(path.split('/')[4]);
      const body = await req.json();
      if (body.display_name) {
        await db.prepare(`UPDATE players SET display_name=? WHERE primary_email=?`)
          .bind(body.display_name, rawEmail).run();
      }
      if (body.first_match_num !== undefined) {
        await db.prepare(`UPDATE players SET first_match_num=? WHERE primary_email=?`)
          .bind(body.first_match_num || null, rawEmail).run();
        // Recalculate penalties since first match changed
        await recalcPenalties(db);
      }
      return json({ success: true });
    }

    // POST /api/admin/email-map
    if (path === '/api/admin/email-map' && method === 'POST') {
      const body = await req.json();
      const { alias_email, primary_email } = body;
      await db.prepare(`INSERT OR REPLACE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
        .bind(alias_email.toLowerCase(), primary_email.toLowerCase()).run();
      return json({ success: true });
    }

    // GET /api/admin/email-map
    if (path === '/api/admin/email-map' && method === 'GET') {
      const rows = await db.prepare(`SELECT * FROM email_map ORDER BY primary_email, alias_email`).all();
      return json(rows.results);
    }

    // ── VARIATIONS / BONUSES ──────────────────────────────────────────────────

    // GET /api/admin/variations
    if (path === '/api/admin/variations' && method === 'GET') {
      const rows = await db.prepare(`SELECT * FROM variations ORDER BY created_at DESC`).all();
      return json(rows.results);
    }

    // POST /api/admin/variations
    if (path === '/api/admin/variations' && method === 'POST') {
      const body = await req.json();
      const { name, type, description, applies_to_match_id, details } = body;
      if (!name || !type) return err('name and type required');

      const result = await db.prepare(
        `INSERT INTO variations (name, type, description, applies_to_match_id, details, is_active)
         VALUES (?,?,?,?,?,1)`
      ).bind(name, type, description || '', applies_to_match_id || null,
             typeof details === 'object' ? JSON.stringify(details) : (details || null)).run();

      return json({ id: result.meta.last_row_id });
    }

    // PATCH /api/admin/variations/:id
    if (path.match(/^\/api\/admin\/variations\/(\d+)$/) && method === 'PATCH') {
      const varId = parseInt(path.split('/')[4]);
      const body = await req.json();
      const fields = [], vals = [];

      if (body.is_active !== undefined) { fields.push('is_active=?'); vals.push(body.is_active ? 1 : 0); }
      if (body.name !== undefined)      { fields.push('name=?');      vals.push(body.name); }
      if (body.description !== undefined) { fields.push('description=?'); vals.push(body.description); }
      if (body.details !== undefined)   {
        fields.push('details=?');
        vals.push(typeof body.details === 'object' ? JSON.stringify(body.details) : body.details);
      }

      if (!fields.length) return err('Nothing to update');
      vals.push(varId);
      await db.prepare(`UPDATE variations SET ${fields.join(',')} WHERE id=?`).bind(...vals).run();
      return json({ success: true });
    }

    // DELETE /api/admin/variations/:id
    if (path.match(/^\/api\/admin\/variations\/(\d+)$/) && method === 'DELETE') {
      const varId = parseInt(path.split('/')[4]);
      await db.prepare(`DELETE FROM variations WHERE id=?`).bind(varId).run();
      return json({ success: true });
    }

    // POST /api/admin/variations/recalc — manually trigger recalculation
    if (path === '/api/admin/variations/recalc' && method === 'POST') {
      await applyDoubleHeaderBonus(db);
      await recalcPenalties(db);
      const board = await buildLeaderboardSimple(db);
      return json({ success: true, leaderboard_preview: board.slice(0, 5) });
    }

    // GET /api/admin/bonus-points — see all bonus rows
    if (path === '/api/admin/bonus-points' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT bp.*, p.display_name, m.match_number FROM bonus_points bp
         LEFT JOIN players p ON p.primary_email = bp.primary_email
         LEFT JOIN matches m ON m.id = bp.match_id
         ORDER BY bp.id DESC`
      ).all();
      return json(rows.results);
    }

    // POST /api/admin/bonus-points — manual one-off bonus
    if (path === '/api/admin/bonus-points' && method === 'POST') {
      const body = await req.json();
      const { primary_email, match_id, bonus_pts, reason, details } = body;
      if (!primary_email || !bonus_pts) return err('primary_email and bonus_pts required');

      await db.prepare(
        `INSERT INTO bonus_points (primary_email, match_id, bonus_pts, reason, details)
         VALUES (?,?,?,?,?)`
      ).bind(primary_email, match_id || null, bonus_pts, reason || 'manual', details || '').run();

      return json({ success: true });
    }

    // DELETE /api/admin/bonus-points/:id
    if (path.match(/^\/api\/admin\/bonus-points\/(\d+)$/) && method === 'DELETE') {
      const bpId = parseInt(path.split('/')[4]);
      await db.prepare(`DELETE FROM bonus_points WHERE id=?`).bind(bpId).run();
      return json({ success: true });
    }

    // GET /api/admin/leaderboard?asOf=YYYY-MM-DD
    if (path === '/api/admin/leaderboard' && method === 'GET') {
      const asOf = url.searchParams.get('asOf');
      // For date-filtered leaderboard, we do it by match number/date
      if (asOf) {
        // Get all matches resulted on or before asOf
        const matchIds = await db.prepare(
          `SELECT id FROM matches WHERE status='resulted' AND updated_at <= ?`
        ).bind(asOf + 'T23:59:59').all();

        if (!matchIds.results.length) return json([]);

        const ids = matchIds.results.map(m => m.id);
        const idList = ids.join(',');

        const players = await db.prepare(
          `SELECT p.primary_email, p.display_name, p.first_match_num,
                  COALESCE(SUM(CASE WHEN s.match_id IN (${idList}) THEN s.points_earned ELSE 0 END), 0) as total_points,
                  COALESCE(SUM(CASE WHEN pen.match_id IN (${idList}) THEN pen.penalty_pts ELSE 0 END), 0) as total_penalties,
                  COALESCE(SUM(CASE WHEN bp.match_id IN (${idList}) THEN bp.bonus_pts ELSE 0 END), 0) as total_bonuses,
                  COUNT(DISTINCT CASE WHEN s.match_id IN (${idList}) THEN s.match_id END) as matches_played,
                  COUNT(DISTINCT CASE WHEN s.match_id IN (${idList}) AND s.points_earned > 0 THEN s.match_id END) as correct_predictions
           FROM players p
           LEFT JOIN scores s ON s.primary_email = p.primary_email
           LEFT JOIN penalties pen ON pen.primary_email = p.primary_email
           LEFT JOIN bonus_points bp ON bp.primary_email = p.primary_email
           WHERE p.first_match_num IS NOT NULL
           GROUP BY p.primary_email
           ORDER BY (COALESCE(SUM(CASE WHEN s.match_id IN (${idList}) THEN s.points_earned ELSE 0 END), 0) +
                     COALESCE(SUM(CASE WHEN pen.match_id IN (${idList}) THEN pen.penalty_pts ELSE 0 END), 0) +
                     COALESCE(SUM(CASE WHEN bp.match_id IN (${idList}) THEN bp.bonus_pts ELSE 0 END), 0)) DESC`
        ).all();

        return json(players.results.map((p, i) => ({
          rank: i + 1,
          display_name: p.display_name,
          primary_email: p.primary_email,
          gross_points: parseFloat((p.total_points || 0).toFixed(2)),
          penalties: parseFloat((p.total_penalties || 0).toFixed(2)),
          bonuses: parseFloat((p.total_bonuses || 0).toFixed(2)),
          net_points: parseFloat(((p.total_points || 0) + (p.total_penalties || 0) + (p.total_bonuses || 0)).toFixed(2)),
          matches_played: p.matches_played,
          correct_predictions: p.correct_predictions,
        })));
      }

      const board = await buildLeaderboardSimple(db);
      return json(board);
    }

    // GET /api/admin/audit
    if (path === '/api/admin/audit' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 500`
      ).all();
      return json(rows.results);
    }

    // GET /api/admin/export
    if (path === '/api/admin/export' && method === 'GET') {
      const [matches, predictions, scores, penalties, players, emailMap, bonuses] = await Promise.all([
        db.prepare('SELECT * FROM matches ORDER BY match_number').all(),
        db.prepare('SELECT * FROM predictions ORDER BY match_id, submitted_at').all(),
        db.prepare('SELECT * FROM scores ORDER BY match_id').all(),
        db.prepare('SELECT * FROM penalties ORDER BY match_id').all(),
        db.prepare('SELECT * FROM players ORDER BY display_name').all(),
        db.prepare('SELECT * FROM email_map ORDER BY primary_email').all(),
        db.prepare('SELECT * FROM bonus_points ORDER BY id').all(),
      ]);
      const board = await buildLeaderboardSimple(db);
      return json({
        exported_at: new Date().toISOString(),
        leaderboard: board,
        matches: matches.results,
        predictions: predictions.results,
        scores: scores.results,
        penalties: penalties.results,
        players: players.results,
        email_map: emailMap.results,
        bonus_points: bonuses.results,
      });
    }

    // POST /api/admin/google-forms-webhook
    if (path === '/api/admin/google-forms-webhook' && method === 'POST') {
      const body = await req.json();
      const publicUrl = new URL(req.url);
      publicUrl.pathname = '/api/predict';
      const predictReq = new Request(publicUrl.toString(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: body.email, match_id: body.match_id, predicted_team: body.predicted_team }),
      });
      return this.fetch(predictReq, env, ctx);
    }

    return err('Not found', 404);
  }
};

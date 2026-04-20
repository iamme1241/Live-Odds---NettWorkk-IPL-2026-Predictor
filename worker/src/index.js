/**
 * Predictor League — Cloudflare Worker API
 * All game logic lives here: dedup, odds, penalties, scoring, leaderboard
 */

// ─── CORS ────────────────────────────────────────────────────────────────────
function cors(env, req) {
  const origin = req.headers.get('Origin') || '';
  const allowed = [env.PUBLIC_ORIGIN, env.ADMIN_ORIGIN, 'http://localhost:3000'];
  const allow = allowed.includes(origin) ? origin : env.PUBLIC_ORIGIN;
  return {
    'Access-Control-Allow-Origin': allow,
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
  };
}

function json(data, status = 200, env, req) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...cors(env, req) },
  });
}

function err(msg, status = 400, env, req) {
  return json({ error: msg }, status, env, req);
}

// ─── AUTH ─────────────────────────────────────────────────────────────────────
function requireAdmin(req, env) {
  const auth = req.headers.get('Authorization') || '';
  const token = auth.replace('Bearer ', '');
  // Token = base64(secretPath:password)
  const expected = btoa(`${env.ADMIN_SECRET_PATH}:${env.ADMIN_PASSWORD}`);
  return token === expected;
}

// ─── ODDS ENGINE ─────────────────────────────────────────────────────────────
async function computeOdds(db, matchId) {
  const votes = await db.prepare(
    `SELECT predicted_team, COUNT(*) as cnt FROM predictions
     WHERE match_id = ? AND is_valid = 1 GROUP BY predicted_team`
  ).bind(matchId).all();

  let a = 0, b = 0;
  const match = await db.prepare('SELECT team_a, team_b FROM matches WHERE id = ?')
    .bind(matchId).first();

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

// ─── PENALTY ENGINE ──────────────────────────────────────────────────────────
async function recalcPenalties(db) {
  // Get all resulted matches in order
  const resulted = await db.prepare(
    `SELECT id, match_number FROM matches WHERE status = 'resulted' ORDER BY match_number`
  ).all();
  if (!resulted.results.length) return;

  // Get all players with their first match
  const players = await db.prepare(
    `SELECT primary_email, first_match_num FROM players WHERE first_match_num IS NOT NULL`
  ).all();

  // Clear existing auto-penalties (keep manual overrides)
  await db.prepare(`DELETE FROM penalties WHERE reason = 'missed_vote'`).run();

  const resultedNums = resulted.results.map(m => m.match_number);

  for (const player of players.results) {
    const { primary_email, first_match_num } = player;

    // Matches they were obligated to vote in (from first_match_num onward)
    const obligated = resulted.results.filter(m => m.match_number >= first_match_num);

    // Matches they actually voted in
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
async function buildLeaderboard(db) {
  const players = await db.prepare(
    `SELECT p.primary_email, p.display_name, p.first_match_num,
            COALESCE(SUM(s.points_earned), 0) as total_points,
            COALESCE(SUM(pen.penalty_pts), 0) as total_penalties,
            COUNT(DISTINCT s.match_id) as matches_played,
            COUNT(DISTINCT CASE WHEN s.points_earned > 0 THEN s.match_id END) as correct_predictions
     FROM players p
     LEFT JOIN scores s ON s.primary_email = p.primary_email
     LEFT JOIN penalties pen ON pen.primary_email = p.primary_email
     WHERE p.first_match_num IS NOT NULL
     GROUP BY p.primary_email
     ORDER BY (COALESCE(SUM(s.points_earned),0) + COALESCE(SUM(pen.penalty_pts),0)) DESC`
  ).all();

  return players.results.map((p, i) => ({
    rank: i + 1,
    display_name: p.display_name,
    primary_email: p.primary_email,
    gross_points: parseFloat(p.total_points.toFixed(2)),
    penalties: parseFloat(p.total_penalties.toFixed(2)),
    net_points: parseFloat((p.total_points + p.total_penalties).toFixed(2)),
    matches_played: p.matches_played,
    correct_predictions: p.correct_predictions,
    first_match: p.first_match_num,
  }));
}

// ─── ROUTER ──────────────────────────────────────────────────────────────────
export default {

  // Cron: auto-close matches past their start time
  async scheduled(event, env, ctx) {
    const db = env.DB;
    const now = new Date().toISOString();
    const toClose = await db.prepare(
      `SELECT id FROM matches WHERE status = 'open' AND match_time <= ?`
    ).bind(now).all();

    for (const m of toClose.results) {
      // Snapshot odds
      const odds = await computeOdds(db, m.id);
      await db.prepare(
        `INSERT INTO odds_snapshots (match_id, team_a_votes, team_b_votes, total_votes, team_a_odds, team_b_odds, is_final)
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
      return new Response(null, { status: 204, headers: cors(env, req) });
    }

    // ── PUBLIC ROUTES ────────────────────────────────────────────────────────

    // GET /api/matches — list all matches (public summary)
    if (path === '/api/matches' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT id, match_number, title, team_a, team_b, match_time, status, winner
         FROM matches ORDER BY match_number ASC`
      ).all();
      return json(rows.results, 200, env, req);
    }

    // GET /api/matches/:id/odds — live odds for a match
    if (path.match(/^\/api\/matches\/(\d+)\/odds$/) && method === 'GET') {
      const matchId = parseInt(path.split('/')[3]);
      const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(matchId).first();
      if (!match) return err('Match not found', 404, env, req);

      if (match.status === 'resulted' || match.status === 'closed') {
        const snap = await db.prepare(
          `SELECT * FROM odds_snapshots WHERE match_id=? AND is_final=1`
        ).bind(matchId).first();
        return json({ ...snap, frozen: true }, 200, env, req);
      }
      const odds = await computeOdds(db, matchId);
      return json({ ...odds, frozen: false }, 200, env, req);
    }

    // GET /api/leaderboard
    if (path === '/api/leaderboard' && method === 'GET') {
      const board = await buildLeaderboard(db);
      return json(board, 200, env, req);
    }

    // GET /api/players/:email/history
    if (path.match(/^\/api\/players\/(.+)\/history$/) && method === 'GET') {
      const rawEmail = decodeURIComponent(path.split('/')[3]);
      const primaryEmail = await resolveEmail(db, rawEmail);

      const player = await db.prepare('SELECT * FROM players WHERE primary_email=?')
        .bind(primaryEmail).first();
      if (!player) return err('Player not found', 404, env, req);

      const history = await db.prepare(
        `SELECT m.match_number, m.title, m.team_a, m.team_b, m.winner, m.match_time,
                p.predicted_team, p.submitted_at,
                s.points_earned, s.odds_at_close,
                pen.penalty_pts
         FROM matches m
         LEFT JOIN predictions p ON p.match_id = m.id AND p.primary_email = ?
         LEFT JOIN scores s ON s.match_id = m.id AND s.primary_email = ?
         LEFT JOIN penalties pen ON pen.match_id = m.id AND pen.primary_email = ?
         WHERE m.status IN ('resulted','closed','open')
         ORDER BY m.match_number ASC`
      ).bind(primaryEmail, primaryEmail, primaryEmail).all();

      return json({ player, history: history.results }, 200, env, req);
    }

    // POST /api/predict — submit a prediction
    if (path === '/api/predict' && method === 'POST') {
      let body;
      try { body = await req.json(); } catch { return err('Invalid JSON', 400, env, req); }

      const { email, match_id, predicted_team } = body;
      if (!email || !match_id || !predicted_team)
        return err('Missing email, match_id, or predicted_team', 400, env, req);

      // Get match
      const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(match_id).first();
      if (!match) return err('Match not found', 404, env, req);
      if (match.status !== 'open') return err('Predictions are closed for this match', 400, env, req);
      if (predicted_team !== match.team_a && predicted_team !== match.team_b)
        return err('Invalid team selection', 400, env, req);

      const rawEmail = email.trim().toLowerCase();
      const primaryEmail = await resolveEmail(db, rawEmail);

      // Check for existing valid prediction (dedup)
      const existing = await db.prepare(
        `SELECT id FROM predictions WHERE match_id=? AND primary_email=? AND is_valid=1`
      ).bind(match_id, primaryEmail).first();

      if (existing) {
        // Log the invalid attempt
        await db.prepare(
          `INSERT INTO predictions (match_id, primary_email, raw_email, predicted_team, is_valid, invalid_reason)
           VALUES (?, ?, ?, ?, 0, 'duplicate_vote')`
        ).bind(match_id, primaryEmail, rawEmail, predicted_team).run();
        await db.prepare(
          `INSERT INTO audit_log (action, actor, entity, entity_id, details) VALUES (?,?,?,?,?)`
        ).bind('duplicate_blocked', rawEmail, 'prediction', String(match_id),
          `Attempted duplicate vote for match ${match.match_number}`).run();
        return err('You have already voted for this match. First vote stands.', 409, env, req);
      }

      // Register player if new
      let player = await db.prepare('SELECT * FROM players WHERE primary_email=?')
        .bind(primaryEmail).first();

      if (!player) {
        // New player — try to find a display name from existing mapping or use email prefix
        const displayName = rawEmail.split('@')[0];
        await db.prepare(
          `INSERT INTO players (display_name, primary_email, first_match_num) VALUES (?,?,?)`
        ).bind(displayName, primaryEmail, match.match_number).run();

        // Self-map primary email
        await db.prepare(
          `INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`
        ).bind(primaryEmail, primaryEmail).run();

        // Map raw email if different
        if (rawEmail !== primaryEmail) {
          await db.prepare(
            `INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`
          ).bind(rawEmail, primaryEmail).run();
        }
      } else if (!player.first_match_num) {
        await db.prepare(
          `UPDATE players SET first_match_num=? WHERE primary_email=?`
        ).bind(match.match_number, primaryEmail).run();
      }

      // Record prediction
      await db.prepare(
        `INSERT INTO predictions (match_id, primary_email, raw_email, predicted_team, is_valid)
         VALUES (?,?,?,?,1)`
      ).bind(match_id, primaryEmail, rawEmail, predicted_team).run();

      const odds = await computeOdds(db, match_id);
      return json({ success: true, odds }, 200, env, req);
    }

    // ── ADMIN ROUTES ────────────────────────────────────────────────────────
    if (!requireAdmin(req, env))
      return err('Unauthorized', 401, env, req);

    // POST /api/admin/matches — create a match
    if (path === '/api/admin/matches' && method === 'POST') {
      const body = await req.json();
      const { match_number, title, team_a, team_b, match_time } = body;
      if (!match_number || !title || !team_a || !team_b || !match_time)
        return err('Missing fields', 400, env, req);

      const result = await db.prepare(
        `INSERT INTO matches (match_number, title, team_a, team_b, match_time, status)
         VALUES (?,?,?,?,?,'upcoming')`
      ).bind(match_number, title, team_a, team_b, match_time).run();

      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('match_created', 'admin', 'match', String(result.meta.last_row_id), title).run();

      return json({ id: result.meta.last_row_id }, 200, env, req);
    }

    // PUT /api/admin/matches/:id — update match (status, time override)
    if (path.match(/^\/api\/admin\/matches\/(\d+)$/) && method === 'PUT') {
      const matchId = parseInt(path.split('/')[4]);
      const body = await req.json();
      const fields = [];
      const vals = [];

      if (body.status)     { fields.push('status=?');     vals.push(body.status); }
      if (body.match_time) { fields.push('match_time=?'); vals.push(body.match_time); }
      if (body.title)      { fields.push('title=?');      vals.push(body.title); }
      fields.push("updated_at=datetime('now')");
      vals.push(matchId);

      await db.prepare(`UPDATE matches SET ${fields.join(',')} WHERE id=?`).bind(...vals).run();
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('match_updated', 'admin', 'match', String(matchId), JSON.stringify(body)).run();
      return json({ success: true }, 200, env, req);
    }

    // POST /api/admin/matches/:id/open
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/open$/) && method === 'POST') {
      const matchId = parseInt(path.split('/')[4]);
      await db.prepare(`UPDATE matches SET status='open', updated_at=datetime('now') WHERE id=?`)
        .bind(matchId).run();
      return json({ success: true }, 200, env, req);
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
      return json({ success: true, odds }, 200, env, req);
    }

    // POST /api/admin/matches/:id/result — enter result, auto-score all
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/result$/) && method === 'POST') {
      const matchId = parseInt(path.split('/')[4]);
      const body = await req.json();
      const { winner } = body;

      const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(matchId).first();
      if (!match) return err('Match not found', 404, env, req);
      if (match.status !== 'closed') return err('Match must be closed before entering result', 400, env, req);

      // Get final odds snapshot
      const snap = await db.prepare(
        `SELECT * FROM odds_snapshots WHERE match_id=? AND is_final=1`
      ).bind(matchId).first();

      const odds = snap || await computeOdds(db, matchId);
      const winnerOdds = winner === match.team_a ? odds.team_a_odds : odds.team_b_odds;

      // Score all valid predictions
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

      // Update match
      await db.prepare(`UPDATE matches SET status='resulted', winner=?, updated_at=datetime('now') WHERE id=?`)
        .bind(winner, matchId).run();

      // Recalculate all penalties
      await recalcPenalties(db);

      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`)
        .bind('result_entered', 'admin', 'match', String(matchId), `Winner: ${winner}`).run();

      const board = await buildLeaderboard(db);
      return json({ success: true, leaderboard_preview: board.slice(0, 5) }, 200, env, req);
    }

    // GET /api/admin/matches/:id/predictions — all predictions for a match
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/predictions$/) && method === 'GET') {
      const matchId = parseInt(path.split('/')[4]);
      const rows = await db.prepare(
        `SELECT p.*, pl.display_name FROM predictions p
         LEFT JOIN players pl ON pl.primary_email = p.primary_email
         WHERE p.match_id = ? ORDER BY p.submitted_at ASC`
      ).bind(matchId).all();
      return json(rows.results, 200, env, req);
    }

    // GET /api/admin/players — all players
    if (path === '/api/admin/players' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT p.*, GROUP_CONCAT(em.alias_email) as all_emails
         FROM players p
         LEFT JOIN email_map em ON em.primary_email = p.primary_email
         GROUP BY p.primary_email ORDER BY p.display_name`
      ).all();
      return json(rows.results, 200, env, req);
    }

    // POST /api/admin/players — add/update player
    if (path === '/api/admin/players' && method === 'POST') {
      const body = await req.json();
      const { display_name, primary_email, alias_emails } = body;
      if (!display_name || !primary_email) return err('Missing fields', 400, env, req);

      await db.prepare(
        `INSERT INTO players (display_name, primary_email) VALUES (?,?)
         ON CONFLICT(primary_email) DO UPDATE SET display_name=excluded.display_name`
      ).bind(display_name, primary_email.toLowerCase()).run();

      // Self-map
      await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
        .bind(primary_email.toLowerCase(), primary_email.toLowerCase()).run();

      // Map aliases
      if (alias_emails && Array.isArray(alias_emails)) {
        for (const alias of alias_emails) {
          await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
            .bind(alias.trim().toLowerCase(), primary_email.toLowerCase()).run();
        }
      }
      return json({ success: true }, 200, env, req);
    }

    // PUT /api/admin/players/:email/name
    if (path.match(/^\/api\/admin\/players\/.+\/name$/) && method === 'PUT') {
      const rawEmail = decodeURIComponent(path.split('/')[4]);
      const body = await req.json();
      await db.prepare(`UPDATE players SET display_name=? WHERE primary_email=?`)
        .bind(body.display_name, rawEmail).run();
      return json({ success: true }, 200, env, req);
    }

    // POST /api/admin/email-map — add email alias mapping
    if (path === '/api/admin/email-map' && method === 'POST') {
      const body = await req.json();
      const { alias_email, primary_email } = body;
      await db.prepare(`INSERT OR REPLACE INTO email_map (alias_email, primary_email) VALUES (?,?)`)
        .bind(alias_email.toLowerCase(), primary_email.toLowerCase()).run();
      return json({ success: true }, 200, env, req);
    }

    // GET /api/admin/email-map
    if (path === '/api/admin/email-map' && method === 'GET') {
      const rows = await db.prepare(`SELECT * FROM email_map ORDER BY primary_email, alias_email`).all();
      return json(rows.results, 200, env, req);
    }

    // GET /api/admin/audit
    if (path === '/api/admin/audit' && method === 'GET') {
      const rows = await db.prepare(
        `SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 200`
      ).all();
      return json(rows.results, 200, env, req);
    }

    // GET /api/admin/export — full data export for Sheets
    if (path === '/api/admin/export' && method === 'GET') {
      const [matches, predictions, scores, penalties, players, emailMap] = await Promise.all([
        db.prepare('SELECT * FROM matches ORDER BY match_number').all(),
        db.prepare('SELECT * FROM predictions ORDER BY match_id, submitted_at').all(),
        db.prepare('SELECT * FROM scores ORDER BY match_id').all(),
        db.prepare('SELECT * FROM penalties ORDER BY match_id').all(),
        db.prepare('SELECT * FROM players ORDER BY display_name').all(),
        db.prepare('SELECT * FROM email_map ORDER BY primary_email').all(),
      ]);
      const board = await buildLeaderboard(db);
      return json({
        exported_at: new Date().toISOString(),
        leaderboard: board,
        matches: matches.results,
        predictions: predictions.results,
        scores: scores.results,
        penalties: penalties.results,
        players: players.results,
        email_map: emailMap.results,
      }, 200, env, req);
    }

    // POST /api/admin/google-forms-webhook — receive Google Forms submissions
    if (path === '/api/admin/google-forms-webhook' && method === 'POST') {
      const body = await req.json();
      // Expects: { email, match_id, predicted_team }
      // Simulate a predict call
      const fakeReq = new Request(req.url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: body.email,
          match_id: body.match_id,
          predicted_team: body.predicted_team,
        }),
      });
      // Remove admin auth so it goes through public predict route
      const publicUrl = new URL(req.url);
      publicUrl.pathname = '/api/predict';
      const predictReq = new Request(publicUrl.toString(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: body.email, match_id: body.match_id, predicted_team: body.predicted_team }),
      });
      return this.fetch(predictReq, env, ctx);
    }

    return err('Not found', 404, env, req);
  }
};

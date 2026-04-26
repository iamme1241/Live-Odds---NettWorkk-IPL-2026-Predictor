/**
 * Predictor League Worker API v9
 * KEY FIX: recalculate-all rewritten to use batch operations instead of
 * sequential per-match loops. 36 matches was timing out at 12-13s (Cloudflare
 * Workers CPU limit is 10s on free plan, 30s on paid). 
 * 
 * Strategy:
 * - Load ALL data upfront in bulk queries
 * - Compute everything in JS memory
 * - Write back in batch statements
 * - Eliminates hundreds of round-trips to D1
 */

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

function isAdmin(req, env) {
  const token = (req.headers.get('Authorization') || '').replace('Bearer ', '');
  return token === btoa(`${env.ADMIN_SECRET_PATH}:${env.ADMIN_PASSWORD}`);
}

async function resolveTeam(db, rawTeam) {
  if (!rawTeam) return rawTeam;
  const t = rawTeam.trim();
  const row = await db.prepare('SELECT primary_name FROM team_aliases WHERE LOWER(alias_name)=LOWER(?)').bind(t).first();
  return row ? row.primary_name : t;
}

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

async function resolveEmail(db, raw) {
  if (!raw) return raw;
  const e = raw.trim().toLowerCase();
  const r = await db.prepare('SELECT primary_email FROM email_map WHERE LOWER(alias_email)=?').bind(e).first();
  return r ? r.primary_email : e;
}

async function ensurePlayer(db, primaryEmail, rawEmail, matchNum, displayName) {
  let p = await db.prepare('SELECT * FROM players WHERE primary_email=?').bind(primaryEmail).first();
  if (!p) {
    const name = displayName || rawEmail.split('@')[0];
    await db.prepare(`INSERT INTO players (display_name, primary_email, first_match_num) VALUES (?,?,?)`)
      .bind(name, primaryEmail, matchNum).run();
    await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`).bind(primaryEmail, primaryEmail).run();
    if (rawEmail !== primaryEmail) {
      await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email, primary_email) VALUES (?,?)`).bind(rawEmail, primaryEmail).run();
    }
  } else {
    if (matchNum && (!p.first_match_num || matchNum < p.first_match_num)) {
      await db.prepare(`UPDATE players SET first_match_num=? WHERE primary_email=?`).bind(matchNum, primaryEmail).run();
    }
    if (displayName && displayName !== p.display_name) {
      await db.prepare(`UPDATE players SET display_name=? WHERE primary_email=?`).bind(displayName, primaryEmail).run();
    }
  }
}

async function ensureEqualizerTable(db) {
  await db.prepare(`CREATE TABLE IF NOT EXISTS equalizer_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    primary_email TEXT NOT NULL,
    from_match_number INTEGER NOT NULL,
    custom_multiplier REAL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(primary_email)
  )`).run();
}

function calcEqualizerMultiplier(joinMatchNumber) {
  const remaining = 71 - joinMatchNumber;
  if (remaining <= 0) return 1;
  return parseFloat((70 / remaining).toFixed(4));
}

// ─── FAST BATCH RECALCULATE ───────────────────────────────────────────────────
// Loads all data upfront, computes in memory, writes back in batch
// This replaces the slow per-match sequential approach
async function recalculateAllFast(db) {
  await ensureEqualizerTable(db);

  // ── LOAD ALL DATA UPFRONT ──
  const [matchesRes, predsRes, snapsRes, playersRes, exemptionsRes, equalizerRes] = await Promise.all([
    db.prepare(`SELECT * FROM matches WHERE status IN ('resulted','abandoned') ORDER BY match_number`).all(),
    db.prepare(`SELECT * FROM predictions WHERE is_valid=1`).all(),
    db.prepare(`SELECT * FROM odds_snapshots WHERE is_final=1`).all(),
    db.prepare(`SELECT primary_email, first_match_num FROM players WHERE first_match_num IS NOT NULL`).all(),
    db.prepare(`SELECT primary_email, match_id FROM penalties WHERE reason='missed_vote_exempt'`).all(),
    db.prepare(`SELECT * FROM equalizer_configs WHERE is_active=1`).all(),
  ]);

  const allMatches = matchesRes.results;
  const allPreds = predsRes.results;
  const snapMap = {}; // matchId -> snapshot
  for (const s of snapsRes.results) snapMap[s.match_id] = s;
  const players = playersRes.results;
  const exemptSet = new Set(exemptionsRes.results.map(e => `${e.primary_email}::${e.match_id}`));
  const equalizerConfigs = equalizerRes.results;

  // Group predictions by match
  const predsByMatch = {};
  for (const p of allPreds) {
    if (!predsByMatch[p.match_id]) predsByMatch[p.match_id] = [];
    predsByMatch[p.match_id].push(p);
  }

  // ── CLEAR DERIVED DATA ──
  await db.prepare(`DELETE FROM scores`).run();
  await db.prepare(`DELETE FROM penalties WHERE reason='missed_vote'`).run();
  await db.prepare(`DELETE FROM bonus_points WHERE reason IN ('double_header','equalizer')`).run();

  // ── COMPUTE SCORES IN MEMORY ──
  const newScores = [];       // {match_id, primary_email, predicted_team, winner, odds_at_close, base_points, points_earned}
  const newSnapshots = [];    // snapshots that need to be created

  for (const match of allMatches) {
    const preds = predsByMatch[match.id] || [];
    let snap = snapMap[match.id];

    if (match.winner === 'cancelled') continue;

    // Ensure snapshot exists (compute from predictions if missing)
    if (!snap) {
      const aVotes = preds.filter(p => p.predicted_team === match.team_a).length;
      const bVotes = preds.filter(p => p.predicted_team === match.team_b).length;
      const tot = aVotes + bVotes;
      snap = {
        match_id: match.id,
        team_a_votes: aVotes,
        team_b_votes: bVotes,
        total_votes: tot,
        team_a_odds: tot > 0 && aVotes > 0 ? parseFloat((tot / aVotes).toFixed(2)) : null,
        team_b_odds: tot > 0 && bVotes > 0 ? parseFloat((tot / bVotes).toFixed(2)) : null,
      };
      newSnapshots.push(snap);
    }

    if (match.winner === 'abandoned') {
      // All voters get 100 pts each
      for (const p of preds) {
        newScores.push({
          match_id: match.id, primary_email: p.primary_email,
          predicted_team: p.predicted_team, winner: 'abandoned',
          odds_at_close: 1, base_points: 100, points_earned: 100
        });
      }
      continue;
    }

    // Normal match
    for (const p of preds) {
      const oddsUsed = p.predicted_team === match.team_a ? snap.team_a_odds : snap.team_b_odds;
      const pts = p.predicted_team === match.winner
        ? parseFloat((100 * (oddsUsed || 1)).toFixed(2))
        : 0;
      newScores.push({
        match_id: match.id, primary_email: p.primary_email,
        predicted_team: p.predicted_team, winner: match.winner,
        odds_at_close: oddsUsed || 1, base_points: 100, points_earned: pts
      });
    }
  }

  // ── WRITE SCORES IN BATCHES OF 50 ──
  for (let i = 0; i < newScores.length; i += 50) {
    const batch = newScores.slice(i, i + 50);
    const stmts = batch.map(s =>
      db.prepare(`INSERT INTO scores (match_id,primary_email,predicted_team,winner,odds_at_close,base_points,points_earned) VALUES (?,?,?,?,?,?,?)`)
        .bind(s.match_id, s.primary_email, s.predicted_team, s.winner, s.odds_at_close, s.base_points, s.points_earned)
    );
    await db.batch(stmts);
  }

  // Write any new snapshots
  for (const snap of newSnapshots) {
    await db.prepare(`INSERT OR IGNORE INTO odds_snapshots (match_id,team_a_votes,team_b_votes,total_votes,team_a_odds,team_b_odds,is_final) VALUES (?,?,?,?,?,?,1)`)
      .bind(snap.match_id, snap.team_a_votes, snap.team_b_votes, snap.total_votes, snap.team_a_odds, snap.team_b_odds).run();
  }

  // ── COMPUTE PENALTIES IN MEMORY ──
  // Build set of match_numbers each player voted in
  const votedByPlayer = {}; // primary_email -> Set of match_numbers
  for (const p of allPreds) {
    const match = allMatches.find(m => m.id === p.match_id);
    if (!match) continue;
    if (!votedByPlayer[p.primary_email]) votedByPlayer[p.primary_email] = new Set();
    votedByPlayer[p.primary_email].add(match.match_number);
  }

  const newPenalties = [];
  for (const player of players) {
    const obligated = allMatches.filter(m => m.match_number >= player.first_match_num);
    const voted = votedByPlayer[player.primary_email] || new Set();
    for (const m of obligated) {
      if (!voted.has(m.match_number)) {
        const key = `${player.primary_email}::${m.id}`;
        if (!exemptSet.has(key)) {
          newPenalties.push({ primary_email: player.primary_email, match_id: m.id });
        }
      }
    }
  }

  // Write penalties in batches
  for (let i = 0; i < newPenalties.length; i += 50) {
    const batch = newPenalties.slice(i, i + 50);
    const stmts = batch.map(p =>
      db.prepare(`INSERT OR IGNORE INTO penalties (primary_email,match_id,penalty_pts,reason) VALUES (?,?,-50,'missed_vote')`)
        .bind(p.primary_email, p.match_id)
    );
    await db.batch(stmts);
  }

  // ── COMPUTE DOUBLE HEADER BONUSES IN MEMORY ──
  // Build scores map for quick lookup: matchId -> Set of winning emails
  const winnersByMatch = {}; // matchId -> Set of primary_emails who scored > 0
  for (const s of newScores) {
    if (s.points_earned > 0) {
      if (!winnersByMatch[s.match_id]) winnersByMatch[s.match_id] = new Set();
      winnersByMatch[s.match_id].add(s.primary_email);
    }
  }

  // Group resulted matches by IST date
  const resultedMatches = allMatches.filter(m => m.winner !== 'abandoned' && m.winner !== 'cancelled');
  const byDate = {};
  for (const m of resultedMatches) {
    const d = new Date(m.match_time);
    const ist = new Date(d.getTime() + (5.5 * 60 * 60 * 1000));
    const key = `${ist.getUTCFullYear()}-${String(ist.getUTCMonth()+1).padStart(2,'0')}-${String(ist.getUTCDate()).padStart(2,'0')}`;
    if (!byDate[key]) byDate[key] = [];
    byDate[key].push(m);
  }

  const newDoubleHeader = [];
  for (const [, ms] of Object.entries(byDate)) {
    if (ms.length < 2) continue;
    const sorted = ms.sort((a, b) => new Date(a.match_time) - new Date(b.match_time));
    for (let i = 0; i < sorted.length - 1; i++) {
      const m1 = sorted[i], m2 = sorted[i + 1];
      const w1 = winnersByMatch[m1.id] || new Set();
      const w2 = winnersByMatch[m2.id] || new Set();
      for (const email of w1) {
        if (w2.has(email)) {
          newDoubleHeader.push({
            primary_email: email, match_id: m2.id, bonus_pts: 50,
            details: `Double header: M${m1.match_number}+M${m2.match_number}`
          });
        }
      }
    }
  }

  if (newDoubleHeader.length > 0) {
    for (let i = 0; i < newDoubleHeader.length; i += 50) {
      const batch = newDoubleHeader.slice(i, i + 50);
      const stmts = batch.map(b =>
        db.prepare(`INSERT OR IGNORE INTO bonus_points (primary_email,match_id,bonus_pts,reason,details) VALUES (?,?,50,'double_header',?)`)
          .bind(b.primary_email, b.match_id, b.details)
      );
      await db.batch(stmts);
    }
  }

  // ── COMPUTE EQUALIZER BONUSES IN MEMORY ──
  if (equalizerConfigs.length > 0) {
    const newEqualizer = [];
    // Build scores map: matchId+email -> points_earned
    const scoreMap = {};
    for (const s of newScores) {
      if (s.points_earned > 0) scoreMap[`${s.match_id}::${s.primary_email}`] = s.points_earned;
    }

    for (const cfg of equalizerConfigs) {
      const multiplier = cfg.custom_multiplier || calcEqualizerMultiplier(cfg.from_match_number);
      for (const match of allMatches) {
        if (match.match_number < cfg.from_match_number) continue;
        const earned = scoreMap[`${match.id}::${cfg.primary_email}`];
        if (!earned || earned <= 0) continue;
        const bonusPts = parseFloat((earned * (multiplier - 1)).toFixed(2));
        if (bonusPts <= 0) continue;
        newEqualizer.push({
          primary_email: cfg.primary_email, match_id: match.id, bonus_pts: bonusPts,
          details: `Equalizer ${multiplier}x (joined M${cfg.from_match_number}): ${earned} × ${(multiplier-1).toFixed(4)}`
        });
      }
    }

    if (newEqualizer.length > 0) {
      for (let i = 0; i < newEqualizer.length; i += 50) {
        const batch = newEqualizer.slice(i, i + 50);
        const stmts = batch.map(b =>
          db.prepare(`INSERT OR IGNORE INTO bonus_points (primary_email,match_id,bonus_pts,reason,details) VALUES (?,?,?,'equalizer',?)`)
            .bind(b.primary_email, b.match_id, b.bonus_pts, b.details)
        );
        await db.batch(stmts);
      }
    }
  }

  return {
    matches_processed: allMatches.length,
    scores_written: newScores.length,
    penalties_written: newPenalties.length,
    double_header_bonuses: newDoubleHeader.length,
  };
}

// ─── SINGLE MATCH SCORE (used for result/force-result) ───────────────────────
async function scoreMatch(db, matchId, winner) {
  const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(matchId).first();
  if (!match) return;

  if (winner === 'cancelled') {
    await db.prepare(`DELETE FROM scores WHERE match_id=?`).bind(matchId).run();
    await db.prepare(`DELETE FROM bonus_points WHERE match_id=? AND reason NOT IN ('manual')`).bind(matchId).run();
    await db.prepare(`UPDATE matches SET status='cancelled', winner='cancelled', updated_at=datetime('now') WHERE id=?`).bind(matchId).run();
    return;
  }

  let snap = await db.prepare(`SELECT * FROM odds_snapshots WHERE match_id=? AND is_final=1`).bind(matchId).first();
  if (!snap) {
    const o = await getOdds(db, matchId);
    await db.prepare(`INSERT OR REPLACE INTO odds_snapshots (match_id,team_a_votes,team_b_votes,total_votes,team_a_odds,team_b_odds,is_final) VALUES (?,?,?,?,?,?,1)`)
      .bind(matchId, o.team_a_votes, o.team_b_votes, o.total_votes, o.team_a_odds, o.team_b_odds).run();
    snap = o;
  }

  await db.prepare(`DELETE FROM scores WHERE match_id=?`).bind(matchId).run();
  const preds = await db.prepare(`SELECT * FROM predictions WHERE match_id=? AND is_valid=1`).bind(matchId).all();

  if (winner === 'abandoned') {
    for (const p of preds.results) {
      await db.prepare(`INSERT INTO scores (match_id,primary_email,predicted_team,winner,odds_at_close,base_points,points_earned) VALUES (?,?,?,?,?,?,?)`)
        .bind(matchId, p.primary_email, p.predicted_team, 'abandoned', 1, 100, 100).run();
    }
    await db.prepare(`UPDATE matches SET status='abandoned', winner='abandoned', updated_at=datetime('now') WHERE id=?`).bind(matchId).run();
    return;
  }

  for (const p of preds.results) {
    const oddsUsed = p.predicted_team === match.team_a ? snap.team_a_odds : snap.team_b_odds;
    const pts = p.predicted_team === winner ? parseFloat((100 * (oddsUsed || 1)).toFixed(2)) : 0;
    await db.prepare(`INSERT INTO scores (match_id,primary_email,predicted_team,winner,odds_at_close,base_points,points_earned) VALUES (?,?,?,?,?,?,?)`)
      .bind(matchId, p.primary_email, p.predicted_team, winner, oddsUsed || 1, 100, pts).run();
  }
  await db.prepare(`UPDATE matches SET status='resulted', winner=?, updated_at=datetime('now') WHERE id=?`).bind(winner, matchId).run();
}

// ─── RECALC PENALTIES (single match result) ───────────────────────────────────
async function recalcPenalties(db) {
  const resulted = await db.prepare(`SELECT id, match_number FROM matches WHERE status IN ('resulted','abandoned') ORDER BY match_number`).all();
  if (!resulted.results.length) return;

  const exemptions = await db.prepare(`SELECT primary_email, match_id FROM penalties WHERE reason='missed_vote_exempt'`).all();
  const exemptSet = new Set(exemptions.results.map(e => `${e.primary_email}::${e.match_id}`));
  await db.prepare(`DELETE FROM penalties WHERE reason='missed_vote'`).run();

  const players = await db.prepare(`SELECT primary_email, first_match_num FROM players WHERE first_match_num IS NOT NULL`).all();
  const allPreds = await db.prepare(`SELECT p.primary_email, m.match_number FROM predictions p JOIN matches m ON m.id=p.match_id WHERE p.is_valid=1 AND m.status IN ('resulted','abandoned')`).all();

  const votedByPlayer = {};
  for (const p of allPreds.results) {
    if (!votedByPlayer[p.primary_email]) votedByPlayer[p.primary_email] = new Set();
    votedByPlayer[p.primary_email].add(p.match_number);
  }

  const newPenalties = [];
  for (const pl of players.results) {
    const obligated = resulted.results.filter(m => m.match_number >= pl.first_match_num);
    const voted = votedByPlayer[pl.primary_email] || new Set();
    for (const m of obligated) {
      if (!voted.has(m.match_number)) {
        const key = `${pl.primary_email}::${m.id}`;
        if (!exemptSet.has(key)) newPenalties.push({ primary_email: pl.primary_email, match_id: m.id });
      }
    }
  }

  if (newPenalties.length > 0) {
    for (let i = 0; i < newPenalties.length; i += 50) {
      const batch = newPenalties.slice(i, i + 50).map(p =>
        db.prepare(`INSERT OR IGNORE INTO penalties (primary_email,match_id,penalty_pts,reason) VALUES (?,?,-50,'missed_vote')`).bind(p.primary_email, p.match_id)
      );
      await db.batch(batch);
    }
  }
}

async function applyDoubleHeaderBonus(db) {
  const resulted = await db.prepare(`SELECT id, match_number, match_time FROM matches WHERE status='resulted' ORDER BY match_number`).all();
  const byDate = {};
  for (const m of resulted.results) {
    const d = new Date(m.match_time);
    const ist = new Date(d.getTime() + (5.5 * 60 * 60 * 1000));
    const key = `${ist.getUTCFullYear()}-${String(ist.getUTCMonth()+1).padStart(2,'0')}-${String(ist.getUTCDate()).padStart(2,'0')}`;
    if (!byDate[key]) byDate[key] = [];
    byDate[key].push(m);
  }
  await db.prepare(`DELETE FROM bonus_points WHERE reason='double_header'`).run();
  for (const [, ms] of Object.entries(byDate)) {
    if (ms.length < 2) continue;
    const sorted = ms.sort((a, b) => new Date(a.match_time) - new Date(b.match_time));
    for (let i = 0; i < sorted.length - 1; i++) {
      const m1 = sorted[i], m2 = sorted[i + 1];
      const c1 = await db.prepare(`SELECT primary_email FROM scores WHERE match_id=? AND points_earned>0`).bind(m1.id).all();
      const c2 = await db.prepare(`SELECT primary_email FROM scores WHERE match_id=? AND points_earned>0`).bind(m2.id).all();
      const s1 = new Set(c1.results.map(r => r.primary_email));
      const s2 = new Set(c2.results.map(r => r.primary_email));
      for (const email of s1) {
        if (s2.has(email)) {
          await db.prepare(`INSERT OR IGNORE INTO bonus_points (primary_email,match_id,bonus_pts,reason,details) VALUES (?,?,50,'double_header',?)`)
            .bind(email, m2.id, `Double header: M${m1.match_number}+M${m2.match_number}`).run();
        }
      }
    }
  }
}

async function applyEqualizerBonus(db, matchId) {
  await ensureEqualizerTable(db);
  const configs = await db.prepare(`SELECT * FROM equalizer_configs WHERE is_active=1`).all();
  if (!configs.results.length) return;
  await db.prepare(`DELETE FROM bonus_points WHERE match_id=? AND reason='equalizer'`).bind(matchId).run();
  const match = await db.prepare('SELECT match_number FROM matches WHERE id=?').bind(matchId).first();
  if (!match) return;
  for (const cfg of configs.results) {
    if (match.match_number < cfg.from_match_number) continue;
    const score = await db.prepare(`SELECT points_earned FROM scores WHERE match_id=? AND primary_email=? AND points_earned>0`).bind(matchId, cfg.primary_email).first();
    if (!score) continue;
    const multiplier = cfg.custom_multiplier || calcEqualizerMultiplier(cfg.from_match_number);
    const bonusPts = parseFloat((score.points_earned * (multiplier - 1)).toFixed(2));
    if (bonusPts <= 0) continue;
    await db.prepare(`INSERT OR IGNORE INTO bonus_points (primary_email,match_id,bonus_pts,reason,details) VALUES (?,?,?,'equalizer',?)`)
      .bind(cfg.primary_email, matchId, bonusPts, `Equalizer ${multiplier}x (joined M${cfg.from_match_number}): ${score.points_earned} × ${(multiplier-1).toFixed(4)}`).run();
  }
}

async function cascadeEmailMap(db, aliasEmail, primaryEmail) {
  await db.prepare(`UPDATE predictions SET primary_email=? WHERE raw_email=? OR primary_email=?`).bind(primaryEmail, aliasEmail, aliasEmail).run();
  await db.prepare(`UPDATE scores SET primary_email=? WHERE primary_email=?`).bind(primaryEmail, aliasEmail).run();
  await db.prepare(`UPDATE penalties SET primary_email=? WHERE primary_email=?`).bind(primaryEmail, aliasEmail).run();
  await db.prepare(`UPDATE bonus_points SET primary_email=? WHERE primary_email=?`).bind(primaryEmail, aliasEmail).run();
  if (aliasEmail !== primaryEmail) {
    const aliasAsPlayer = await db.prepare(`SELECT primary_email FROM players WHERE primary_email=?`).bind(aliasEmail).first();
    if (aliasAsPlayer) {
      await db.prepare(`DELETE FROM players WHERE primary_email=?`).bind(aliasEmail).run();
      await db.prepare(`DELETE FROM email_map WHERE alias_email=? AND primary_email=?`).bind(aliasEmail, aliasEmail).run();
    }
  }
}

async function relinkAllPredictions(db) {
  const all = await db.prepare(`SELECT id, raw_email, primary_email FROM predictions`).all();
  let updated = 0, skipped = 0;
  for (const pred of all.results) {
    const resolvedFromRaw = await db.prepare(`SELECT primary_email FROM email_map WHERE LOWER(alias_email)=LOWER(?)`).bind(pred.raw_email).first();
    const resolvedFromPrimary = await db.prepare(`SELECT primary_email FROM email_map WHERE LOWER(alias_email)=LOWER(?)`).bind(pred.primary_email).first();
    const resolved = (resolvedFromRaw?.primary_email) || (resolvedFromPrimary?.primary_email) || pred.primary_email;
    if (resolved !== pred.primary_email) {
      await db.prepare(`UPDATE predictions SET primary_email=? WHERE id=?`).bind(resolved, pred.id).run();
      updated++;
    } else { skipped++; }
  }
  const emailMap = await db.prepare(`SELECT alias_email, primary_email FROM email_map`).all();
  for (const row of emailMap.results) {
    if (row.alias_email === row.primary_email) continue;
    await db.prepare(`UPDATE scores SET primary_email=? WHERE primary_email=?`).bind(row.primary_email, row.alias_email).run();
    await db.prepare(`UPDATE penalties SET primary_email=? WHERE primary_email=?`).bind(row.primary_email, row.alias_email).run();
    await db.prepare(`UPDATE bonus_points SET primary_email=? WHERE primary_email=?`).bind(row.primary_email, row.alias_email).run();
  }
  return { updated, skipped };
}

async function buildLB(db, asOf) {
  let matchCond = `status IN ('resulted','abandoned')`;
  if (asOf) matchCond += ` AND updated_at <= '${asOf}T23:59:59'`;
  const matchIds = await db.prepare(`SELECT id FROM matches WHERE ${matchCond}`).all();
  if (!matchIds.results.length) return [];
  const ids = matchIds.results.map(m => m.id).join(',');
  if (!ids) return [];
  const players = await db.prepare(`
    SELECT p.primary_email, p.display_name, p.first_match_num,
      COALESCE(s.gross,0) AS gross, COALESCE(pen.pen,0) AS pen, COALESCE(bp.bonus,0) AS bonus,
      COUNT(DISTINCT s2.match_id) AS played,
      COUNT(DISTINCT CASE WHEN s2.points_earned>0 THEN s2.match_id END) AS correct
    FROM players p
    LEFT JOIN (SELECT primary_email, SUM(points_earned) AS gross FROM scores WHERE match_id IN (${ids}) GROUP BY primary_email) s ON s.primary_email=p.primary_email
    LEFT JOIN (SELECT primary_email, SUM(penalty_pts) AS pen FROM penalties WHERE match_id IN (${ids}) AND reason NOT IN ('missed_vote_exempt') GROUP BY primary_email) pen ON pen.primary_email=p.primary_email
    LEFT JOIN (SELECT primary_email, SUM(bonus_pts) AS bonus FROM bonus_points WHERE (match_id IN (${ids}) OR match_id IS NULL) GROUP BY primary_email) bp ON bp.primary_email=p.primary_email
    LEFT JOIN scores s2 ON s2.primary_email=p.primary_email AND s2.match_id IN (${ids})
    WHERE p.first_match_num IS NOT NULL
    GROUP BY p.primary_email
    ORDER BY (COALESCE(s.gross,0)+COALESCE(pen.pen,0)+COALESCE(bp.bonus,0)) DESC
  `).all();
  return players.results.map((p,i) => ({
    rank: i+1, display_name: p.display_name, primary_email: p.primary_email,
    gross_points: +parseFloat(p.gross).toFixed(2),
    penalties: +parseFloat(p.pen).toFixed(2),
    bonuses: +parseFloat(p.bonus).toFixed(2),
    net_points: +(parseFloat(p.gross)+parseFloat(p.pen)+parseFloat(p.bonus)).toFixed(2),
    matches_played: p.played, correct_predictions: p.correct, first_match: p.first_match_num,
  }));
}

async function buildInsights(db) {
  const players = await db.prepare(`SELECT primary_email, display_name FROM players`).all();
  const streaks = [];
  for (const pl of players.results) {
    const history = await db.prepare(`SELECT s.points_earned, m.match_number FROM scores s JOIN matches m ON m.id=s.match_id WHERE s.primary_email=? ORDER BY m.match_number DESC LIMIT 10`).bind(pl.primary_email).all();
    let streak = 0;
    for (const h of history.results) { if (h.points_earned > 0) streak++; else break; }
    if (streak > 0) streaks.push({ name: pl.display_name, email: pl.primary_email, streak });
  }
  streaks.sort((a,b) => b.streak - a.streak);
  const upsets = await db.prepare(`SELECT m.match_number, m.title, m.winner, COUNT(CASE WHEN s.points_earned>0 THEN 1 END) correct_count, COUNT(s.id) total_count FROM matches m LEFT JOIN scores s ON s.match_id=m.id WHERE m.status='resulted' GROUP BY m.id HAVING total_count>0 ORDER BY (CAST(correct_count AS REAL)/total_count) ASC LIMIT 10`).all();
  return {
    streaks: streaks.slice(0,10),
    hardest: upsets.results.map(u => ({ ...u, correct_pct: u.total_count>0?Math.round(u.correct_count/u.total_count*100):0 }))
  };
}

// ─── ROUTER ──────────────────────────────────────────────────────────────────
export default {
  async scheduled(event, env) {
    const db = env.DB;
    const now = new Date().toISOString();
    const toClose = await db.prepare(`SELECT id FROM matches WHERE status='open' AND match_time<=?`).bind(now).all();
    for (const m of toClose.results) {
      const o = await getOdds(db, m.id);
      await db.prepare(`INSERT OR REPLACE INTO odds_snapshots (match_id,team_a_votes,team_b_votes,total_votes,team_a_odds,team_b_odds,is_final) VALUES (?,?,?,?,?,?,1)`)
        .bind(m.id, o.team_a_votes, o.team_b_votes, o.total_votes, o.team_a_odds, o.team_b_odds).run();
      await db.prepare(`UPDATE matches SET status='closed', updated_at=datetime('now') WHERE id=?`).bind(m.id).run();
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
      const rows = await db.prepare(`SELECT id,match_number,title,team_a,team_b,match_time,status,winner FROM matches ORDER BY match_number`).all();
      return R(rows.results);
    }
    if (path.match(/^\/api\/matches\/(\d+)\/odds$/) && method === 'GET') {
      const id = +path.split('/')[3];
      const match = await db.prepare('SELECT status FROM matches WHERE id=?').bind(id).first();
      if (!match) return E('Match not found',404);
      if (['resulted','closed','abandoned'].includes(match.status)) {
        const snap = await db.prepare(`SELECT * FROM odds_snapshots WHERE match_id=? AND is_final=1`).bind(id).first();
        if (snap) return R({...snap, frozen:true});
      }
      return R({...await getOdds(db,id), frozen:false});
    }
    if (path === '/api/leaderboard' && method === 'GET') return R(await buildLB(db,null));

    if (path.match(/^\/api\/players\/.+\/history$/) && method === 'GET') {
      const rawEmail = decodeURIComponent(path.split('/')[3]);
      const primary = await resolveEmail(db, rawEmail);
      const player = await db.prepare('SELECT * FROM players WHERE primary_email=?').bind(primary).first();
      if (!player) return E('Player not found',404);
      const history = await db.prepare(`
        SELECT m.match_number,m.title,m.team_a,m.team_b,m.winner,m.match_time,m.status,
               p.predicted_team,p.submitted_at,s.points_earned,s.odds_at_close,
               pen.penalty_pts,pen.reason AS penalty_reason,
               bp.bonus_pts,bp.reason AS bonus_reason,bp.details AS bonus_details
        FROM matches m
        LEFT JOIN predictions p ON p.match_id=m.id AND p.primary_email=? AND p.is_valid=1
        LEFT JOIN scores s ON s.match_id=m.id AND s.primary_email=?
        LEFT JOIN penalties pen ON pen.match_id=m.id AND pen.primary_email=? AND pen.reason NOT IN ('missed_vote_exempt')
        LEFT JOIN bonus_points bp ON bp.match_id=m.id AND bp.primary_email=?
        WHERE m.status IN ('resulted','closed','open','abandoned')
        ORDER BY m.match_number ASC`).bind(primary,primary,primary,primary).all();
      const generalBonuses = await db.prepare(`SELECT bp.*,m.match_number FROM bonus_points bp LEFT JOIN matches m ON m.id=bp.match_id WHERE bp.primary_email=?`).bind(primary).all();
      const rows = history.results;
      const gross = rows.reduce((s,h)=>s+(h.points_earned??0),0);
      const pen = rows.reduce((s,h)=>s+(h.penalty_pts??0),0);
      const bonus = generalBonuses.results.reduce((s,b)=>s+(b.bonus_pts??0),0);
      return R({
        player:{display_name:player.display_name,primary_email:player.primary_email,first_match_num:player.first_match_num},
        summary:{gross_points:+gross.toFixed(2),penalties:+pen.toFixed(2),bonuses:+bonus.toFixed(2),net_points:+(gross+pen+bonus).toFixed(2),matches_played:rows.filter(h=>h.predicted_team).length,correct:rows.filter(h=>h.points_earned>0).length},
        history:rows, general_bonuses:generalBonuses.results,
      });
    }

    if (path === '/api/insights' && method === 'GET') return R(await buildInsights(db));

    if (path === '/api/predict' && method === 'POST') {
      let body; try { body = await req.json(); } catch { return E('Invalid JSON'); }
      const {email,match_id,name} = body; let {predicted_team} = body;
      if (!email||!match_id||!predicted_team) return E('Missing fields');
      const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(match_id).first();
      if (!match) return E('Match not found',404);
      if (match.status!=='open') return E('Predictions closed for this match');
      predicted_team = await resolveTeam(db,predicted_team);
      if (predicted_team!==match.team_a&&predicted_team!==match.team_b) return E(`Invalid team. Accepted: "${match.team_a}" or "${match.team_b}"`);
      const rawEmail = email.trim().toLowerCase();
      const primary = await resolveEmail(db,rawEmail);
      const existing = await db.prepare(`SELECT id FROM predictions WHERE match_id=? AND primary_email=? AND is_valid=1`).bind(match_id,primary).first();
      if (existing) {
        await db.prepare(`INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,is_valid,invalid_reason) VALUES (?,?,?,?,0,'duplicate_vote')`).bind(match_id,primary,rawEmail,predicted_team).run();
        return E('Already voted. First vote stands.',409);
      }
      await ensurePlayer(db,primary,rawEmail,match.match_number,name);
      await db.prepare(`INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,is_valid) VALUES (?,?,?,?,1)`).bind(match_id,primary,rawEmail,predicted_team).run();
      return R({success:true,odds:await getOdds(db,match_id)});
    }

    if (path === '/api/forms-submit' && method === 'POST') {
      let body; try { body = await req.json(); } catch { return E('Invalid JSON'); }
      const {email,match_number,name} = body; let {predicted_team} = body;
      if (!email||!match_number||!predicted_team) return E('Missing fields');
      const match = await db.prepare('SELECT * FROM matches WHERE match_number=?').bind(+match_number).first();
      if (!match) return E(`Match number ${match_number} not found.`,404);
      if (match.status!=='open') return E('Predictions closed for this match');
      predicted_team = await resolveTeam(db,predicted_team);
      if (predicted_team!==match.team_a&&predicted_team!==match.team_b) return E(`Invalid team. Accepted: "${match.team_a}" or "${match.team_b}"`);
      const rawEmail = email.trim().toLowerCase();
      const primary = await resolveEmail(db,rawEmail);
      const existing = await db.prepare(`SELECT id FROM predictions WHERE match_id=? AND primary_email=? AND is_valid=1`).bind(match.id,primary).first();
      if (existing) {
        await db.prepare(`INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,is_valid,invalid_reason) VALUES (?,?,?,?,0,'duplicate_vote')`).bind(match.id,primary,rawEmail,predicted_team).run();
        return E('Already voted. First vote stands.',409);
      }
      await ensurePlayer(db,primary,rawEmail,match.match_number,name);
      await db.prepare(`INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,is_valid) VALUES (?,?,?,?,1)`).bind(match.id,primary,rawEmail,predicted_team).run();
      return R({success:true});
    }

    // ── ADMIN ─────────────────────────────────────────────────────────────────
    if (!isAdmin(req,env)) return E('Unauthorized',401);
    await ensureEqualizerTable(db);

    // ── RECALCULATE ALL — now uses fast batch approach ────────────────────────
    if (path === '/api/admin/recalculate-all' && method === 'POST') {
      try {
        const result = await recalculateAllFast(db);
        return R({success:true,...result});
      } catch(e) {
        return E(`Recalculate failed: ${e.message}`, 500);
      }
    }

    if (path === '/api/admin/predictions/relink-preview' && method === 'GET') {
      const all = await db.prepare(`SELECT id,raw_email,primary_email FROM predictions`).all();
      const changes = [];
      for (const pred of all.results) {
        const rr = await db.prepare(`SELECT primary_email FROM email_map WHERE LOWER(alias_email)=LOWER(?)`).bind(pred.raw_email).first();
        const rp = await db.prepare(`SELECT primary_email FROM email_map WHERE LOWER(alias_email)=LOWER(?)`).bind(pred.primary_email).first();
        const resolved = (rr?.primary_email)||(rp?.primary_email)||pred.primary_email;
        if (resolved!==pred.primary_email) changes.push({id:pred.id,raw_email:pred.raw_email,old_primary:pred.primary_email,new_primary:resolved});
      }
      return R({changes,total_affected:changes.length});
    }

    if (path === '/api/admin/predictions/relink-emails' && method === 'POST') {
      const result = await relinkAllPredictions(db);
      await recalcPenalties(db);
      await applyDoubleHeaderBonus(db);
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`).bind('relink_emails','admin','predictions','all',`Updated:${result.updated},Skipped:${result.skipped}`).run();
      return R({success:true,...result});
    }

    // ── MATCH CRUD ────────────────────────────────────────────────────────────
    if (path === '/api/admin/matches' && method === 'POST') {
      const {match_number,title,team_a,team_b,match_time} = await req.json();
      if (!match_number||!title||!team_a||!team_b||!match_time) return E('Missing fields');
      const r = await db.prepare(`INSERT INTO matches (match_number,title,team_a,team_b,match_time,status) VALUES (?,?,?,?,?,'upcoming')`).bind(+match_number,title,team_a,team_b,match_time).run();
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`).bind('match_created','admin','match',String(r.meta.last_row_id),title).run();
      return R({id:r.meta.last_row_id});
    }
    if (path === '/api/admin/matches/bulk' && method === 'POST') {
      const {matches} = await req.json();
      if (!Array.isArray(matches)) return E('matches array required');
      let created=0,skipped=0,errors=[];
      for (const m of matches) {
        const {match_number,title,team_a,team_b,match_time} = m;
        if (!match_number||!title||!team_a||!team_b||!match_time){errors.push(`M${match_number||'?'}: missing fields`);skipped++;continue;}
        try{await db.prepare(`INSERT INTO matches (match_number,title,team_a,team_b,match_time,status) VALUES (?,?,?,?,?,'upcoming')`).bind(+match_number,title,team_a,team_b,match_time).run();created++;}
        catch(e){errors.push(`M${match_number}: ${e.message}`);skipped++;}
      }
      return R({created,skipped,errors});
    }
    if (path.match(/^\/api\/admin\/matches\/(\d+)$/) && method === 'PATCH') {
      const id = +path.split('/')[4]; const body = await req.json();
      const f=[],v=[];
      if (body.status!==undefined){f.push('status=?');v.push(body.status);}
      if (body.match_time!==undefined){f.push('match_time=?');v.push(body.match_time);}
      if (body.title!==undefined){f.push('title=?');v.push(body.title);}
      if (body.team_a!==undefined){f.push('team_a=?');v.push(body.team_a);}
      if (body.team_b!==undefined){f.push('team_b=?');v.push(body.team_b);}
      if (body.winner!==undefined){f.push('winner=?');v.push(body.winner);}
      if (!f.length) return E('Nothing to update');
      f.push("updated_at=datetime('now')");v.push(id);
      await db.prepare(`UPDATE matches SET ${f.join(',')} WHERE id=?`).bind(...v).run();
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/matches\/(\d+)$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      const m = await db.prepare('SELECT status FROM matches WHERE id=?').bind(id).first();
      if (!m) return E('Not found',404);
      if (m.status!=='upcoming') return E('Can only delete upcoming matches. Use force-delete for others.');
      await db.prepare('DELETE FROM matches WHERE id=?').bind(id).run();
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/force-delete$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      await db.batch([
        db.prepare('DELETE FROM predictions WHERE match_id=?').bind(id),
        db.prepare('DELETE FROM scores WHERE match_id=?').bind(id),
        db.prepare('DELETE FROM penalties WHERE match_id=?').bind(id),
        db.prepare('DELETE FROM bonus_points WHERE match_id=?').bind(id),
        db.prepare('DELETE FROM odds_snapshots WHERE match_id=?').bind(id),
        db.prepare('DELETE FROM matches WHERE id=?').bind(id),
      ]);
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/open$/) && method === 'POST') {
      const id = +path.split('/')[4];
      await db.prepare(`UPDATE matches SET status='open',updated_at=datetime('now') WHERE id=?`).bind(id).run();
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/close$/) && method === 'POST') {
      const id = +path.split('/')[4];
      const o = await getOdds(db,id);
      await db.prepare(`INSERT OR REPLACE INTO odds_snapshots (match_id,team_a_votes,team_b_votes,total_votes,team_a_odds,team_b_odds,is_final) VALUES (?,?,?,?,?,?,1)`).bind(id,o.team_a_votes,o.team_b_votes,o.total_votes,o.team_a_odds,o.team_b_odds).run();
      await db.prepare(`UPDATE matches SET status='closed',updated_at=datetime('now') WHERE id=?`).bind(id).run();
      return R({success:true,odds:o});
    }
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/result$/) && method === 'POST') {
      const id = +path.split('/')[4]; const {winner} = await req.json();
      if (!winner) return E('winner required');
      const m = await db.prepare('SELECT * FROM matches WHERE id=?').bind(id).first();
      if (!m) return E('Not found',404);
      if (m.status!=='closed') return E('Close match first (or use force-result)');
      await scoreMatch(db,id,winner); await recalcPenalties(db); await applyDoubleHeaderBonus(db); await applyEqualizerBonus(db,id);
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`).bind('result_entered','admin','match',String(id),`Winner:${winner}`).run();
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/force-result$/) && method === 'POST') {
      const id = +path.split('/')[4]; const {winner} = await req.json();
      if (!winner) return E('winner required');
      const m = await db.prepare('SELECT * FROM matches WHERE id=?').bind(id).first();
      if (!m) return E('Not found',404);
      const existingSnap = await db.prepare(`SELECT id FROM odds_snapshots WHERE match_id=? AND is_final=1`).bind(id).first();
      if (!existingSnap) {
        const o = await getOdds(db,id);
        await db.prepare(`INSERT OR REPLACE INTO odds_snapshots (match_id,team_a_votes,team_b_votes,total_votes,team_a_odds,team_b_odds,is_final) VALUES (?,?,?,?,?,?,1)`).bind(id,o.team_a_votes,o.team_b_votes,o.total_votes,o.team_a_odds,o.team_b_odds).run();
      }
      await scoreMatch(db,id,winner); await recalcPenalties(db); await applyDoubleHeaderBonus(db); await applyEqualizerBonus(db,id);
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`).bind('force_result','admin','match',String(id),`Winner:${winner}`).run();
      return R({success:true});
    }

    // ── PREDICTIONS CRUD ──────────────────────────────────────────────────────
    if (path.match(/^\/api\/admin\/matches\/(\d+)\/predictions$/) && method === 'GET') {
      const id = +path.split('/')[4];
      const rows = await db.prepare(`SELECT p.*,pl.display_name FROM predictions p LEFT JOIN players pl ON pl.primary_email=p.primary_email WHERE p.match_id=? ORDER BY p.is_valid DESC,p.submitted_at`).bind(id).all();
      return R(rows.results);
    }
    if (path.match(/^\/api\/admin\/predictions\/(\d+)$/) && method === 'PATCH') {
      const id = +path.split('/')[4]; const body = await req.json();
      const f=[],v=[];
      if (body.predicted_team!==undefined){
        let team=body.predicted_team;
        const pred=await db.prepare('SELECT match_id FROM predictions WHERE id=?').bind(id).first();
        if(pred){const m=await db.prepare('SELECT team_a,team_b FROM matches WHERE id=?').bind(pred.match_id).first();team=await resolveTeam(db,team);if(m&&team!==m.team_a&&team!==m.team_b)return E(`Invalid team. Accepted: "${m.team_a}" or "${m.team_b}"`);}
        f.push('predicted_team=?');v.push(team);
      }
      if (body.is_valid!==undefined){f.push('is_valid=?');v.push(body.is_valid?1:0);}
      if (body.invalid_reason!==undefined){f.push('invalid_reason=?');v.push(body.invalid_reason);}
      if (body.submitted_at!==undefined){f.push('submitted_at=?');v.push(body.submitted_at);}
      if (body.primary_email!==undefined){
        const ne=body.primary_email.toLowerCase();
        const tp=await db.prepare('SELECT primary_email FROM players WHERE primary_email=?').bind(ne).first();
        if(!tp)return E(`No player found with email: ${ne}. Add the player first.`,404);
        f.push('primary_email=?');v.push(ne);
      }
      if (body.display_name!==undefined){
        const pred=await db.prepare('SELECT primary_email FROM predictions WHERE id=?').bind(id).first();
        if(pred)await db.prepare(`UPDATE players SET display_name=? WHERE primary_email=?`).bind(body.display_name,pred.primary_email).run();
      }
      if (f.length){v.push(id);await db.prepare(`UPDATE predictions SET ${f.join(',')} WHERE id=?`).bind(...v).run();}
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/predictions\/(\d+)$/) && method === 'DELETE') {
      const id = +path.split('/')[4];
      await db.prepare('DELETE FROM predictions WHERE id=?').bind(id).run();
      return R({success:true});
    }
    if (path === '/api/admin/predictions/bulk-delete' && method === 'POST') {
      const {ids} = await req.json();
      if (!Array.isArray(ids)||!ids.length) return E('ids array required');
      await db.prepare(`DELETE FROM predictions WHERE id IN (${ids.map(()=>'?').join(',')})`).bind(...ids).run();
      return R({success:true,deleted:ids.length});
    }
    if (path === '/api/admin/predictions/bulk' && method === 'POST') {
      const {predictions} = await req.json();
      if (!Array.isArray(predictions)) return E('predictions array required');
      let imported=0,skipped=0,errors=[];
      for (const pred of predictions) {
        const {email,match_id,name} = pred; let {predicted_team,submitted_at} = pred;
        if (!email||!match_id||!predicted_team){errors.push(`Missing fields for ${email||'?'}`);skipped++;continue;}
        const match = await db.prepare('SELECT * FROM matches WHERE id=?').bind(+match_id).first();
        if (!match){errors.push(`Match ${match_id} not found`);skipped++;continue;}
        const isManual = !submitted_at||String(submitted_at).trim().toLowerCase()==='manual'||String(submitted_at).trim()==='';
        if (!isManual) {
          const subTime=new Date(submitted_at),matchTime=new Date(match.match_time);
          if(!isNaN(subTime.getTime())&&!isNaN(matchTime.getTime())&&subTime>matchTime){errors.push(`${email}: submitted after deadline`);skipped++;continue;}
        }
        predicted_team = await resolveTeam(db,predicted_team);
        if (predicted_team!==match.team_a&&predicted_team!==match.team_b){errors.push(`${email}: invalid team "${pred.predicted_team}"`);skipped++;continue;}
        const rawEmail=email.trim().toLowerCase(), primary=await resolveEmail(db,rawEmail);
        const existing=await db.prepare(`SELECT id FROM predictions WHERE match_id=? AND primary_email=? AND is_valid=1`).bind(+match_id,primary).first();
        if(existing){errors.push(`${rawEmail} already voted M${match.match_number}`);skipped++;continue;}
        const displayName=name&&name.trim()?name.trim():null;
        await ensurePlayer(db,primary,rawEmail,match.match_number,displayName);
        let finalSubmittedAt;
        if (isManual){const d=new Date(match.match_time);d.setMinutes(d.getMinutes()-1);finalSubmittedAt=d.toISOString();}
        else{finalSubmittedAt=submitted_at;}
        await db.prepare(`INSERT INTO predictions (match_id,primary_email,raw_email,predicted_team,submitted_at,is_valid) VALUES (?,?,?,?,?,1)`).bind(+match_id,primary,rawEmail,predicted_team,finalSubmittedAt).run();
        imported++;
      }
      return R({imported,skipped,errors});
    }

    // ── PLAYER CRUD ───────────────────────────────────────────────────────────
    if (path === '/api/admin/players' && method === 'GET') {
      const rows = await db.prepare(`SELECT p.*,GROUP_CONCAT(DISTINCT em.alias_email) all_emails FROM players p LEFT JOIN email_map em ON em.primary_email=p.primary_email GROUP BY p.primary_email ORDER BY p.display_name`).all();
      return R(rows.results);
    }
    if (path === '/api/admin/players' && method === 'POST') {
      const {display_name,primary_email,alias_emails,first_match_num} = await req.json();
      if (!display_name||!primary_email) return E('Missing fields');
      const email=primary_email.toLowerCase();
      await db.prepare(`INSERT INTO players (display_name,primary_email,first_match_num) VALUES (?,?,?) ON CONFLICT(primary_email) DO UPDATE SET display_name=excluded.display_name,first_match_num=COALESCE(excluded.first_match_num,first_match_num)`).bind(display_name,email,first_match_num||null).run();
      await db.prepare(`INSERT OR IGNORE INTO email_map (alias_email,primary_email) VALUES (?,?)`).bind(email,email).run();
      if (Array.isArray(alias_emails)) {
        for (const a of alias_emails) {
          if(a){const al=a.trim().toLowerCase();await db.prepare(`INSERT OR REPLACE INTO email_map (alias_email,primary_email) VALUES (?,?)`).bind(al,email).run();await cascadeEmailMap(db,al,email);}
        }
      }
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/players\/[^/]+$/) && method === 'PATCH') {
      const email=decodeURIComponent(path.split('/')[4]); const body=await req.json();
      const f=[],v=[];
      if(body.display_name!==undefined){f.push('display_name=?');v.push(body.display_name);}
      if(body.first_match_num!==undefined){f.push('first_match_num=?');v.push(body.first_match_num);}
      if(!f.length)return E('Nothing to update');
      v.push(email);await db.prepare(`UPDATE players SET ${f.join(',')} WHERE primary_email=?`).bind(...v).run();
      if(body.first_match_num!==undefined)await recalcPenalties(db);
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/players\/[^/]+$/) && method === 'DELETE') {
      const email=decodeURIComponent(path.split('/')[4]);
      await db.prepare('DELETE FROM email_map WHERE primary_email=?').bind(email).run();
      await db.prepare('DELETE FROM players WHERE primary_email=?').bind(email).run();
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/players\/.+\/predictions$/) && method === 'GET') {
      const email=decodeURIComponent(path.split('/')[4]);
      const primary=await resolveEmail(db,email);
      const rows=await db.prepare(`SELECT p.*,m.title,m.match_number,m.team_a,m.team_b,m.winner,m.status AS match_status FROM predictions p JOIN matches m ON m.id=p.match_id WHERE p.primary_email=? ORDER BY m.match_number`).bind(primary).all();
      return R(rows.results);
    }

    // ── EMAIL MAP ─────────────────────────────────────────────────────────────
    if (path === '/api/admin/email-map' && method === 'GET') {
      return R((await db.prepare(`SELECT * FROM email_map ORDER BY primary_email`).all()).results);
    }
    if (path === '/api/admin/email-map' && method === 'POST') {
      const {alias_email,primary_email} = await req.json();
      if (!alias_email||!primary_email) return E('alias_email and primary_email required');
      const alias=alias_email.toLowerCase(),primary=primary_email.toLowerCase();
      if(alias===primary)return E('alias_email and primary_email cannot be the same');
      await db.prepare(`INSERT OR REPLACE INTO email_map (alias_email,primary_email) VALUES (?,?)`).bind(alias,primary).run();
      await cascadeEmailMap(db,alias,primary); await recalcPenalties(db); await applyDoubleHeaderBonus(db);
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/email-map\/.+$/) && method === 'DELETE') {
      await db.prepare('DELETE FROM email_map WHERE alias_email=?').bind(decodeURIComponent(path.split('/')[4])).run();
      return R({success:true});
    }

    // ── TEAM ALIASES ──────────────────────────────────────────────────────────
    if (path === '/api/admin/team-aliases' && method === 'GET') {
      return R((await db.prepare(`SELECT * FROM team_aliases ORDER BY primary_name,alias_name`).all()).results);
    }
    if (path === '/api/admin/team-aliases' && method === 'POST') {
      const {alias_name,primary_name} = await req.json();
      if(!alias_name||!primary_name)return E('alias_name and primary_name required');
      await db.prepare(`INSERT OR REPLACE INTO team_aliases (alias_name,primary_name) VALUES (?,?)`).bind(alias_name.trim(),primary_name.trim()).run();
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/team-aliases\/(\d+)$/) && method === 'DELETE') {
      await db.prepare('DELETE FROM team_aliases WHERE id=?').bind(+path.split('/')[4]).run();
      return R({success:true});
    }

    // ── VARIATIONS ────────────────────────────────────────────────────────────
    if (path === '/api/admin/variations' && method === 'GET') {
      return R((await db.prepare(`SELECT * FROM variations ORDER BY created_at DESC`).all()).results);
    }
    if (path === '/api/admin/variations' && method === 'POST') {
      const {name,type,description,applies_to_match_id,details} = await req.json();
      if(!name||!type)return E('name and type required');
      const det=typeof details==='object'?JSON.stringify(details):(details||null);
      const r=await db.prepare(`INSERT INTO variations (name,type,description,applies_to_match_id,details,is_active) VALUES (?,?,?,?,?,1)`).bind(name,type,description||'',applies_to_match_id||null,det).run();
      return R({id:r.meta.last_row_id});
    }
    if (path.match(/^\/api\/admin\/variations\/(\d+)$/) && method === 'PATCH') {
      const id=+path.split('/')[4]; const body=await req.json();
      const f=[],v=[];
      if(body.name!==undefined){f.push('name=?');v.push(body.name);}
      if(body.description!==undefined){f.push('description=?');v.push(body.description);}
      if(body.is_active!==undefined){f.push('is_active=?');v.push(body.is_active?1:0);}
      if(body.details!==undefined){f.push('details=?');v.push(typeof body.details==='object'?JSON.stringify(body.details):body.details);}
      if(!f.length)return E('Nothing to update');
      v.push(id);await db.prepare(`UPDATE variations SET ${f.join(',')} WHERE id=?`).bind(...v).run();
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/variations\/(\d+)$/) && method === 'DELETE') {
      await db.prepare('DELETE FROM variations WHERE id=?').bind(+path.split('/')[4]).run();
      return R({success:true});
    }
    if (path === '/api/admin/variations/recalc' && method === 'POST') {
      await applyDoubleHeaderBonus(db); await recalcPenalties(db);
      return R({success:true});
    }

    // ── EQUALIZER ─────────────────────────────────────────────────────────────
    if (path === '/api/admin/equalizer' && method === 'GET') {
      const rows=await db.prepare(`SELECT ec.*,p.display_name FROM equalizer_configs ec LEFT JOIN players p ON p.primary_email=ec.primary_email ORDER BY ec.from_match_number`).all();
      return R(rows.results.map(r=>({...r,computed_multiplier:r.custom_multiplier||calcEqualizerMultiplier(r.from_match_number)})));
    }
    if (path === '/api/admin/equalizer' && method === 'POST') {
      const {primary_email,from_match_number,custom_multiplier} = await req.json();
      if(!primary_email||!from_match_number)return E('primary_email and from_match_number required');
      const computed=custom_multiplier||calcEqualizerMultiplier(from_match_number);
      await db.prepare(`INSERT INTO equalizer_configs (primary_email,from_match_number,custom_multiplier,is_active) VALUES (?,?,?,1) ON CONFLICT(primary_email) DO UPDATE SET from_match_number=excluded.from_match_number,custom_multiplier=excluded.custom_multiplier,is_active=1`).bind(primary_email.toLowerCase(),+from_match_number,custom_multiplier||null).run();
      const pastMatches=await db.prepare(`SELECT id FROM matches WHERE status IN ('resulted','abandoned') AND match_number>=?`).bind(+from_match_number).all();
      for(const m of pastMatches.results)await applyEqualizerBonus(db,m.id);
      await db.prepare(`INSERT INTO audit_log (action,actor,entity,entity_id,details) VALUES (?,?,?,?,?)`).bind('equalizer_added','admin','player',primary_email,`From M${from_match_number}, multiplier ${computed}`).run();
      return R({success:true,computed_multiplier:computed});
    }
    if (path.match(/^\/api\/admin\/equalizer\/(\d+)$/) && method === 'PATCH') {
      const id=+path.split('/')[4]; const body=await req.json();
      const f=[],v=[];
      if(body.from_match_number!==undefined){f.push('from_match_number=?');v.push(body.from_match_number);}
      if(body.custom_multiplier!==undefined){f.push('custom_multiplier=?');v.push(body.custom_multiplier);}
      if(body.is_active!==undefined){f.push('is_active=?');v.push(body.is_active?1:0);}
      if(!f.length)return E('Nothing to update');
      v.push(id);await db.prepare(`UPDATE equalizer_configs SET ${f.join(',')} WHERE id=?`).bind(...v).run();
      const cfg=await db.prepare(`SELECT * FROM equalizer_configs WHERE id=?`).bind(id).first();
      if(cfg&&cfg.is_active){const pm=await db.prepare(`SELECT id FROM matches WHERE status IN ('resulted','abandoned') AND match_number>=?`).bind(cfg.from_match_number).all();for(const m of pm.results)await applyEqualizerBonus(db,m.id);}
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/equalizer\/(\d+)$/) && method === 'DELETE') {
      const id=+path.split('/')[4];
      const cfg=await db.prepare(`SELECT * FROM equalizer_configs WHERE id=?`).bind(id).first();
      if(cfg)await db.prepare(`DELETE FROM bonus_points WHERE primary_email=? AND reason='equalizer'`).bind(cfg.primary_email).run();
      await db.prepare('DELETE FROM equalizer_configs WHERE id=?').bind(id).run();
      return R({success:true});
    }

    // ── BONUS POINTS ──────────────────────────────────────────────────────────
    if (path === '/api/admin/bonus-points' && method === 'GET') {
      const rows=await db.prepare(`SELECT bp.*,p.display_name,m.match_number FROM bonus_points bp LEFT JOIN players p ON p.primary_email=bp.primary_email LEFT JOIN matches m ON m.id=bp.match_id ORDER BY bp.id DESC`).all();
      return R(rows.results);
    }
    if (path === '/api/admin/bonus-points' && method === 'POST') {
      const {primary_email,match_id,bonus_pts,reason,details} = await req.json();
      if(!primary_email||bonus_pts===undefined)return E('email and pts required');
      await db.prepare(`INSERT INTO bonus_points (primary_email,match_id,bonus_pts,reason,details) VALUES (?,?,?,?,?)`).bind(primary_email,match_id||null,bonus_pts,reason||'manual',details||'').run();
      return R({success:true});
    }
    if (path.match(/^\/api\/admin\/bonus-points\/(\d+)$/) && method === 'DELETE') {
      await db.prepare('DELETE FROM bonus_points WHERE id=?').bind(+path.split('/')[4]).run();
      return R({success:true});
    }

    // ── PENALTIES ─────────────────────────────────────────────────────────────
    if (path === '/api/admin/penalties' && method === 'GET') {
      const rows=await db.prepare(`SELECT pen.*,p.display_name,m.match_number,m.title FROM penalties pen LEFT JOIN players p ON p.primary_email=pen.primary_email LEFT JOIN matches m ON m.id=pen.match_id WHERE pen.reason NOT IN ('missed_vote_exempt') ORDER BY m.match_number,p.display_name`).all();
      return R(rows.results);
    }
    if (path.match(/^\/api\/admin\/penalties\/(\d+)$/) && method === 'DELETE') {
      const id=+path.split('/')[4];
      const pen=await db.prepare('SELECT * FROM penalties WHERE id=?').bind(id).first();
      if(pen&&pen.reason==='missed_vote')await db.prepare(`INSERT OR IGNORE INTO penalties (primary_email,match_id,penalty_pts,reason) VALUES (?,?,0,'missed_vote_exempt')`).bind(pen.primary_email,pen.match_id).run();
      await db.prepare('DELETE FROM penalties WHERE id=?').bind(id).run();
      return R({success:true});
    }
    if (path === '/api/admin/penalties/recalc' && method === 'POST') {
      await recalcPenalties(db); return R({success:true});
    }

    // ── LEADERBOARD (ADMIN) ───────────────────────────────────────────────────
    if (path === '/api/admin/leaderboard' && method === 'GET') {
      return R(await buildLB(db, url.searchParams.get('asOf')||null));
    }

    // ── AUDIT ─────────────────────────────────────────────────────────────────
    if (path === '/api/admin/audit' && method === 'GET') {
      return R((await db.prepare(`SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 500`).all()).results);
    }

    // ── EXPORT ────────────────────────────────────────────────────────────────
    if (path === '/api/admin/export' && method === 'GET') {
      const [m,pr,sc,pe,pl,em,bp,v,ta] = await Promise.all([
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
      return R({exported_at:new Date().toISOString(),leaderboard:await buildLB(db,null),matches:m.results,predictions:pr.results,scores:sc.results,penalties:pe.results,players:pl.results,email_map:em.results,bonus_points:bp.results,variations:v.results,team_aliases:ta.results});
    }

    return E('Not found',404);
  }
};

/**
 * SQLite Database Setup (better-sqlite3 — native, synchronous)
 * v2: 올·실·코·편물 재설계
 *
 * better-sqlite3 장점:
 *   - 파일에 직접 읽기/쓰기 (메모리에 전체 DB 올리지 않음)
 *   - WAL 모드: 읽기와 쓰기 동시 가능
 *   - 동기 API: 더 빠르고 예측 가능
 */
var Database = require('better-sqlite3');
var fs = require('fs');
var path = require('path');

var DB_DIR = path.join(__dirname, 'data');
var DB_PATH = path.join(DB_DIR, 'knitting.db');

var db = null;     // DBCompat wrapper
var rawDb = null;  // better-sqlite3 instance

function generateId(prefix) {
  return (prefix || 'id') + '_' + Date.now().toString(36) + Math.random().toString(36).substring(2, 7);
}

/**
 * sql.js 호환 래퍼
 * 기존 코드에서 db.getDB().run(sql, params), db.getDB().exec(sql, params) 패턴을 유지
 */
function DBCompat(betterDb) {
  this._db = betterDb;
}

/**
 * run: DDL/DML 실행 (INSERT, UPDATE, DELETE, CREATE TABLE 등)
 * sql.js와 동일한 호출 방식: db.run(sql, [param1, param2, ...])
 */
DBCompat.prototype.run = function(sql, params) {
  if (params && params.length) {
    var stmt = this._db.prepare(sql);
    return stmt.run.apply(stmt, params);
  }
  return this._db.exec(sql);
};

/**
 * exec: SELECT 쿼리 실행, sql.js 포맷으로 반환
 * 반환값: [{columns: [...], values: [[...]]}]
 * 기존 rowsToObjects()와 호환
 */
DBCompat.prototype.exec = function(sql, params) {
  try {
    var stmt = this._db.prepare(sql);
    var rows;
    if (params && params.length) {
      rows = stmt.all.apply(stmt, params);
    } else {
      rows = stmt.all();
    }
    if (!rows || !rows.length) return [];
    var columns = Object.keys(rows[0]);
    var values = rows.map(function(r) {
      return columns.map(function(c) { return r[c]; });
    });
    return [{ columns: columns, values: values }];
  } catch (e) {
    // DDL이나 비-SELECT 문은 빈 배열 반환
    return [];
  }
};

async function initDB() {
  if (!fs.existsSync(DB_DIR)) {
    fs.mkdirSync(DB_DIR, { recursive: true });
  }

  rawDb = new Database(DB_PATH);
  rawDb.pragma('journal_mode = WAL');
  rawDb.pragma('foreign_keys = ON');

  db = new DBCompat(rawDb);

  // ── 올 (fiber) ──────────────────────────────────
  db.run("CREATE TABLE IF NOT EXISTS fibers (\n    id TEXT PRIMARY KEY,\n    text TEXT NOT NULL,\n    source TEXT DEFAULT '',\n    source_id TEXT DEFAULT '',\n    source_title TEXT DEFAULT '',\n    tension INTEGER DEFAULT 3 CHECK(tension BETWEEN 1 AND 5),\n    tone TEXT DEFAULT 'resonance',\n    caught_at INTEGER NOT NULL,\n    source_range TEXT DEFAULT NULL,\n    born_from_id TEXT DEFAULT NULL,\n    born_from_type TEXT DEFAULT NULL\n  )");

  // ── 실 (thread) — 올+올 연결 ────────────────────
  db.run("CREATE TABLE IF NOT EXISTS threads (\n    id TEXT PRIMARY KEY,\n    fiber_a_id TEXT NOT NULL,\n    fiber_b_id TEXT NOT NULL,\n    why TEXT DEFAULT '',\n    created_at INTEGER NOT NULL\n  )");

  // ── 코 (stitch) — 실+실 연결 ────────────────────
  db.run("CREATE TABLE IF NOT EXISTS stitches (\n    id TEXT PRIMARY KEY,\n    thread_a_id TEXT NOT NULL,\n    thread_b_id TEXT NOT NULL,\n    why TEXT DEFAULT '',\n    created_at INTEGER NOT NULL\n  )");

  // ── 편물 (fabric) — 코들의 모임 ──────────────────
  db.run("CREATE TABLE IF NOT EXISTS fabrics (\n    id TEXT PRIMARY KEY,\n    title TEXT DEFAULT '',\n    insight TEXT DEFAULT '',\n    created_at INTEGER NOT NULL,\n    updated_at INTEGER NOT NULL\n  )");

  db.run("CREATE TABLE IF NOT EXISTS fabric_stitches (\n    fabric_id TEXT NOT NULL,\n    stitch_id TEXT NOT NULL,\n    added_at INTEGER NOT NULL,\n    PRIMARY KEY (fabric_id, stitch_id)\n  )");

  // ── 교차 연결 — 다른 층위 간 ─────────────────────
  db.run("CREATE TABLE IF NOT EXISTS connections (\n    id TEXT PRIMARY KEY,\n    node_a_id TEXT NOT NULL,\n    node_b_id TEXT NOT NULL,\n    why TEXT DEFAULT '',\n    created_at INTEGER NOT NULL\n  )");

  // ── 임베딩 (통합) ───────────────────────────────
  db.run("CREATE TABLE IF NOT EXISTS embeddings (\n    node_id TEXT PRIMARY KEY,\n    embedding TEXT NOT NULL\n  )");

  // ── 노트 (소스 문서) ────────────────────────────
  db.run("CREATE TABLE IF NOT EXISTS notes (\n    id TEXT PRIMARY KEY,\n    type TEXT DEFAULT 'blank',\n    title TEXT DEFAULT '',\n    content TEXT DEFAULT '',\n    html_content TEXT DEFAULT '',\n    answers TEXT DEFAULT NULL,\n    bookshelf_id TEXT DEFAULT NULL,\n    created_at INTEGER NOT NULL,\n    updated_at INTEGER NOT NULL\n  )");

  // ── 마이그레이션 ────────────────────────────────
  migrateV2();

  return db;
}

/**
 * v1 → v2 마이그레이션
 * 기존 테이블이 존재하면 데이터를 새 구조로 변환
 */
function migrateV2() {
  // 마이그레이션 이미 완료됐는지 확인
  var migrated = false;
  try {
    var result = db.exec("SELECT name FROM sqlite_master WHERE type='table' AND name='_migration_v2_done'");
    if (result.length > 0 && result[0].values.length > 0) migrated = true;
  } catch (e) { /* ignore */ }
  if (migrated) return;

  // 구 fibers 테이블에 thought 컬럼이 있는지 확인
  var hasOldFibers = false;
  try {
    var cols = db.exec("PRAGMA table_info(fibers)");
    if (cols.length > 0) {
      var colNames = cols[0].values.map(function(r) { return r[1]; });
      hasOldFibers = colNames.indexOf('thought') >= 0;
    }
  } catch (e) { /* ignore */ }

  if (!hasOldFibers) {
    // 신규 설치 — 마이그레이션 불필요
    db.run("CREATE TABLE IF NOT EXISTS _migration_v2_done (done INTEGER)");
    db.run("INSERT INTO _migration_v2_done VALUES (1)");
    return;
  }

  console.log('[migration] v1 → v2 시작...');
  var now = Date.now();

  // ── 1단계: 모든 기존 데이터를 먼저 읽어둔다 ──
  var oldFibers = rowsToObjects(db.exec("SELECT * FROM fibers"));

  var oldReplies = [];
  try {
    oldReplies = rowsToObjects(db.exec("SELECT * FROM fiber_replies"));
  } catch (e) { /* fiber_replies 없으면 스킵 */ }

  var oldStitches = [];
  var hasOldStitchCols = false;
  try {
    var sCols = db.exec("PRAGMA table_info(stitches)");
    if (sCols.length > 0) {
      var sColNames = sCols[0].values.map(function(r) { return r[1]; });
      hasOldStitchCols = sColNames.indexOf('fiber_a_id') >= 0;
    }
    if (hasOldStitchCols) {
      oldStitches = rowsToObjects(db.exec("SELECT * FROM stitches"));
    }
  } catch (e) { /* ignore */ }

  var oldKnots = [];
  var oldKnotStitches = [];
  try {
    oldKnots = rowsToObjects(db.exec("SELECT * FROM knots"));
    oldKnotStitches = rowsToObjects(db.exec("SELECT * FROM knot_stitches"));
  } catch (e) { /* ignore */ }

  var oldEmbeddings = [];
  try {
    oldEmbeddings = rowsToObjects(db.exec("SELECT * FROM fiber_embeddings"));
  } catch (e) { /* ignore */ }

  // ── 2단계: fibers 테이블 재생성 (새 스키마) ──
  try {
    db.run("CREATE TABLE fibers_v2 (id TEXT PRIMARY KEY, text TEXT NOT NULL, source TEXT DEFAULT '', source_id TEXT DEFAULT '', source_title TEXT DEFAULT '', tension INTEGER DEFAULT 3, tone TEXT DEFAULT 'resonance', caught_at INTEGER NOT NULL, source_range TEXT DEFAULT NULL, born_from_id TEXT DEFAULT NULL, born_from_type TEXT DEFAULT NULL)");
    oldFibers.forEach(function(f) {
      db.run(
        "INSERT INTO fibers_v2 (id, text, source, source_id, source_title, tension, tone, caught_at, source_range, born_from_id, born_from_type) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [f.id, f.text, f.source || '', f.source_note_id || '', f.source_note_title || '', f.tension || 3, f.tone || 'resonance', f.caught_at, f.source_range || null, null, null]
      );
    });
    db.run("DROP TABLE fibers");
    db.run("ALTER TABLE fibers_v2 RENAME TO fibers");
  } catch (e) { console.log('[migration] fibers 테이블 재생성 실패:', e.message); }

  // ── 3단계: thought → 새 올 + 실 생성 ──
  oldFibers.forEach(function(f) {
    if (f.thought && f.thought.trim()) {
      var thoughtFiberId = generateId('fb');
      db.run(
        "INSERT INTO fibers (id, text, source, source_id, source_title, tension, tone, caught_at, born_from_id, born_from_type) VALUES (?,?,?,?,?,?,?,?,?,?)",
        [thoughtFiberId, f.thought.trim(), 'thought', '', '', f.tension || 3, f.tone || 'resonance', f.spun_at || now, f.id, 'fiber']
      );

      var threadId = generateId('th');
      db.run(
        "INSERT INTO threads (id, fiber_a_id, fiber_b_id, why, created_at) VALUES (?,?,?,?,?)",
        [threadId, f.id, thoughtFiberId, '', f.spun_at || now]
      );
    }
  });

  // ── 4단계: fiber_replies → 새 올 + 실 생성 ──
  oldReplies.forEach(function(r) {
    var replyFiberId = generateId('fb');
    db.run(
      "INSERT INTO fibers (id, text, source, source_id, source_title, tension, tone, caught_at, born_from_id, born_from_type) VALUES (?,?,?,?,?,?,?,?,?,?)",
      [replyFiberId, r.note, 'reply', '', '', 3, 'resonance', r.created_at || now, r.fiber_id, 'fiber']
    );

    var threadId = generateId('th');
    db.run(
      "INSERT INTO threads (id, fiber_a_id, fiber_b_id, why, created_at) VALUES (?,?,?,?,?)",
      [threadId, r.fiber_id, replyFiberId, '', r.created_at || now]
    );
  });

  // ── 5단계: 기존 stitches (올+올) → threads로 이전 ──
  var stitchToThread = {};
  if (hasOldStitchCols && oldStitches.length > 0) {
    oldStitches.forEach(function(s) {
      var threadId = generateId('th');
      stitchToThread[s.id] = threadId;
      db.run(
        "INSERT OR IGNORE INTO threads (id, fiber_a_id, fiber_b_id, why, created_at) VALUES (?,?,?,?,?)",
        [threadId, s.fiber_a_id, s.fiber_b_id, s.why || '', s.created_at || now]
      );
    });
  }

  // ── 6단계: knots → fabrics + connections ──
  oldKnots.forEach(function(k) {
    var fabricId = generateId('fa');
    db.run(
      "INSERT INTO fabrics (id, title, insight, created_at, updated_at) VALUES (?,?,?,?,?)",
      [fabricId, '', k.insight || '', k.created_at || now, k.created_at || now]
    );

    var knotLinks = oldKnotStitches.filter(function(ks) { return ks.knot_id === k.id; });
    knotLinks.forEach(function(ks) {
      var threadId = stitchToThread[ks.stitch_id];
      if (threadId) {
        var connId = generateId('cn');
        db.run(
          "INSERT INTO connections (id, node_a_id, node_b_id, why, created_at) VALUES (?,?,?,?,?)",
          [connId, fabricId, threadId, 'migrated from knot', now]
        );
      }
    });
  });

  // ── 7단계: stitches 테이블 재생성 (thread_a_id/thread_b_id 구조) ──
  if (hasOldStitchCols) {
    db.run("DROP TABLE IF EXISTS stitches");
    db.run("CREATE TABLE stitches (id TEXT PRIMARY KEY, thread_a_id TEXT NOT NULL, thread_b_id TEXT NOT NULL, why TEXT DEFAULT '', created_at INTEGER NOT NULL)");
  }

  // ── 8단계: fiber_embeddings → embeddings 이전 ──
  oldEmbeddings.forEach(function(e) {
    db.run(
      "INSERT OR IGNORE INTO embeddings (node_id, embedding) VALUES (?,?)",
      [e.fiber_id, e.embedding]
    );
  });

  // ── 9단계: 구 테이블 삭제 ──
  db.run("DROP TABLE IF EXISTS fiber_replies");
  db.run("DROP TABLE IF EXISTS fiber_embeddings");
  db.run("DROP TABLE IF EXISTS reply_embeddings");
  db.run("DROP TABLE IF EXISTS knots");
  db.run("DROP TABLE IF EXISTS knot_stitches");

  // 마이그레이션 완료 표시
  db.run("CREATE TABLE IF NOT EXISTS _migration_v2_done (done INTEGER)");
  db.run("INSERT INTO _migration_v2_done VALUES (1)");

  console.log('[migration] v1 → v2 완료');
}

/**
 * persist — better-sqlite3는 디스크에 직접 쓰므로 no-op
 * 기존 호출 코드와의 호환성을 위해 유지
 */
function persist() {
  // No-op: better-sqlite3 writes directly to disk
}

function getDB() {
  return db;
}

/**
 * sql.js 호환 포맷 [{columns, values}] → 객체 배열로 변환
 * 기존 코드(hint.js 등)와의 호환성을 위해 유지
 */
function rowsToObjects(result) {
  if (!result || !result.length) return [];
  var stmt = result[0];
  return stmt.values.map(function(row) {
    var obj = {};
    stmt.columns.forEach(function(col, i) { obj[col] = row[i]; });
    return obj;
  });
}

/**
 * 최적화된 단일 행 조회 — better-sqlite3 네이티브 사용
 */
function getOne(sql, params) {
  var stmt = rawDb.prepare(sql);
  return (params && params.length ? stmt.get.apply(stmt, params) : stmt.get()) || null;
}

/**
 * 최적화된 다중 행 조회 — better-sqlite3 네이티브 사용
 */
function getAll(sql, params) {
  var stmt = rawDb.prepare(sql);
  return params && params.length ? stmt.all.apply(stmt, params) : stmt.all();
}

module.exports = { initDB: initDB, getDB: getDB, persist: persist, generateId: generateId, rowsToObjects: rowsToObjects, getOne: getOne, getAll: getAll };

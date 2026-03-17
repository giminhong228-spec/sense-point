/**
 * Fabric Routes — 편물 CRUD (코들의 모임)
 */
var express = require('express');
var router = express.Router();
var db = require('../db');
var hint = require('../services/hint');

// POST /api/fabrics — 편물 만들기
router.post('/', function(req, res) {
  try {
    var body = req.body;
    var stitch_ids = body.stitch_ids;

    if (!stitch_ids || !Array.isArray(stitch_ids) || stitch_ids.length < 1) {
      return res.status(400).json({ error: 'At least 1 stitch_id is required' });
    }

    // 모든 코 존재 확인
    for (var i = 0; i < stitch_ids.length; i++) {
      var s = db.getOne('SELECT id FROM stitches WHERE id = ?', [stitch_ids[i]]);
      if (!s) return res.status(404).json({ error: 'Stitch not found: ' + stitch_ids[i] });
    }

    var id = db.generateId('fa');
    var now = Date.now();

    db.getDB().run(
      'INSERT INTO fabrics (id, title, insight, created_at, updated_at) VALUES (?,?,?,?,?)',
      [id, body.title || '', body.insight || '', now, now]
    );

    for (var j = 0; j < stitch_ids.length; j++) {
      db.getDB().run(
        'INSERT INTO fabric_stitches (fabric_id, stitch_id, added_at) VALUES (?,?,?)',
        [id, stitch_ids[j], now]
      );
    }
    db.persist();

    var fabric = _getFabricWithDetails(id);
    res.status(201).json(fabric);

    // 편물 임베딩 생성
    var embText = [body.title || '', body.insight || ''].filter(Boolean).join(' ');
    if (embText.trim()) {
      hint.saveEmbedding(id, embText.trim()).catch(function(err) {
        console.error('[embedding] 편물 임베딩 생성 실패:', err.message);
      });
    }
  } catch (err) {
    console.error('POST /api/fabrics error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/fabrics — 편물 목록
router.get('/', function(req, res) {
  try {
    var limit = Math.min(parseInt(req.query.limit) || 100, 500);
    var offset = parseInt(req.query.offset) || 0;
    var fabrics = db.getAll('SELECT * FROM fabrics ORDER BY created_at DESC LIMIT ? OFFSET ?', [limit, offset]);
    fabrics.forEach(function(f) {
      var links = db.getAll('SELECT stitch_id FROM fabric_stitches WHERE fabric_id = ?', [f.id]);
      f.stitch_ids = links.map(function(l) { return l.stitch_id; });
    });
    res.json(fabrics);
  } catch (err) {
    console.error('GET /api/fabrics error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/fabrics/:id — 편물 상세
router.get('/:id', function(req, res) {
  try {
    var fabric = _getFabricWithDetails(req.params.id);
    if (!fabric) return res.status(404).json({ error: 'Not found' });
    res.json(fabric);
  } catch (err) {
    console.error('GET /api/fabrics/:id error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// PATCH /api/fabrics/:id — 편물 수정
router.patch('/:id', function(req, res) {
  try {
    var fabric = db.getOne('SELECT * FROM fabrics WHERE id = ?', [req.params.id]);
    if (!fabric) return res.status(404).json({ error: 'Not found' });

    var updates = [];
    var params = [];

    if (req.body.title !== undefined) {
      updates.push('title = ?');
      params.push(req.body.title);
    }
    if (req.body.insight !== undefined) {
      updates.push('insight = ?');
      params.push(req.body.insight);
    }

    if (updates.length > 0) {
      updates.push('updated_at = ?');
      params.push(Date.now());
      params.push(req.params.id);
      db.getDB().run('UPDATE fabrics SET ' + updates.join(', ') + ' WHERE id = ?', params);
    }

    // 코 연결 업데이트
    if (req.body.stitch_ids && Array.isArray(req.body.stitch_ids)) {
      db.getDB().run('DELETE FROM fabric_stitches WHERE fabric_id = ?', [req.params.id]);
      var now = Date.now();
      req.body.stitch_ids.forEach(function(sid) {
        db.getDB().run(
          'INSERT INTO fabric_stitches (fabric_id, stitch_id, added_at) VALUES (?,?,?)',
          [req.params.id, sid, now]
        );
      });
    }

    db.persist();
    var updated = _getFabricWithDetails(req.params.id);
    res.json(updated);

    // 임베딩 갱신
    var embText = [updated.title || '', updated.insight || ''].filter(Boolean).join(' ');
    if (embText.trim()) {
      hint.saveEmbedding(req.params.id, embText.trim()).catch(function(err) {
        console.error('[embedding] 편물 임베딩 갱신 실패:', err.message);
      });
    }
  } catch (err) {
    console.error('PATCH /api/fabrics/:id error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// DELETE /api/fabrics/:id — 편물 삭제
router.delete('/:id', function(req, res) {
  try {
    var fabric = db.getOne('SELECT id FROM fabrics WHERE id = ?', [req.params.id]);
    if (!fabric) return res.status(404).json({ error: 'Not found' });

    db.getDB().run('DELETE FROM fabric_stitches WHERE fabric_id = ?', [req.params.id]);
    db.getDB().run('DELETE FROM connections WHERE node_a_id = ? OR node_b_id = ?', [req.params.id, req.params.id]);
    hint.deleteEmbedding(req.params.id);
    db.getDB().run('DELETE FROM fabrics WHERE id = ?', [req.params.id]);
    db.persist();

    res.status(204).end();
  } catch (err) {
    console.error('DELETE /api/fabrics/:id error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Helper: fabric with stitches and thread/fiber details
function _getFabricWithDetails(id) {
  var fabric = db.getOne('SELECT * FROM fabrics WHERE id = ?', [id]);
  if (!fabric) return null;

  var stitchLinks = db.getAll('SELECT stitch_id FROM fabric_stitches WHERE fabric_id = ?', [id]);
  fabric.stitches = [];

  stitchLinks.forEach(function(link) {
    var stitch = db.getOne('SELECT * FROM stitches WHERE id = ?', [link.stitch_id]);
    if (!stitch) return;

    stitch.thread_a = db.getOne('SELECT * FROM threads WHERE id = ?', [stitch.thread_a_id]);
    stitch.thread_b = db.getOne('SELECT * FROM threads WHERE id = ?', [stitch.thread_b_id]);
    fabric.stitches.push(stitch);
  });

  return fabric;
}

module.exports = router;

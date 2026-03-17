/**
 * Stitch Routes — 코 CRUD (실+실 연결)
 * v2: thread_a_id/thread_b_id 기반
 */
var express = require('express');
var router = express.Router();
var db = require('../db');

// POST /api/stitches — 코 만들기 (실+실 연결)
router.post('/', function(req, res) {
  try {
    var body = req.body;
    if (!body.thread_a_id || !body.thread_b_id) {
      return res.status(400).json({ error: 'thread_a_id and thread_b_id are required' });
    }
    if (body.thread_a_id === body.thread_b_id) {
      return res.status(400).json({ error: 'Cannot connect a thread to itself' });
    }

    var threadA = db.getOne('SELECT id FROM threads WHERE id = ?', [body.thread_a_id]);
    var threadB = db.getOne('SELECT id FROM threads WHERE id = ?', [body.thread_b_id]);
    if (!threadA || !threadB) {
      return res.status(404).json({ error: 'One or both threads not found' });
    }

    var id = db.generateId('st');
    var now = Date.now();

    db.getDB().run(
      'INSERT INTO stitches (id, thread_a_id, thread_b_id, why, created_at) VALUES (?,?,?,?,?)',
      [id, body.thread_a_id, body.thread_b_id, body.why || '', now]
    );
    db.persist();

    var stitch = db.getOne('SELECT * FROM stitches WHERE id = ?', [id]);
    res.status(201).json(stitch);
  } catch (err) {
    console.error('POST /api/stitches error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/stitches — 코 목록
router.get('/', function(req, res) {
  try {
    var limit = Math.min(parseInt(req.query.limit) || 100, 500);
    var offset = parseInt(req.query.offset) || 0;
    var stitches;
    if (req.query.thread_id) {
      stitches = db.getAll(
        'SELECT * FROM stitches WHERE thread_a_id = ? OR thread_b_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?',
        [req.query.thread_id, req.query.thread_id, limit, offset]
      );
    } else {
      stitches = db.getAll('SELECT * FROM stitches ORDER BY created_at DESC LIMIT ? OFFSET ?', [limit, offset]);
    }
    res.json(stitches);
  } catch (err) {
    console.error('GET /api/stitches error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/stitches/:id — 코 상세
router.get('/:id', function(req, res) {
  try {
    var stitch = db.getOne('SELECT * FROM stitches WHERE id = ?', [req.params.id]);
    if (!stitch) return res.status(404).json({ error: 'Not found' });
    // 양쪽 실 정보 포함
    stitch.thread_a = db.getOne('SELECT * FROM threads WHERE id = ?', [stitch.thread_a_id]);
    stitch.thread_b = db.getOne('SELECT * FROM threads WHERE id = ?', [stitch.thread_b_id]);
    res.json(stitch);
  } catch (err) {
    console.error('GET /api/stitches/:id error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// DELETE /api/stitches/:id — 코 삭제
router.delete('/:id', function(req, res) {
  try {
    var stitch = db.getOne('SELECT id FROM stitches WHERE id = ?', [req.params.id]);
    if (!stitch) return res.status(404).json({ error: 'Not found' });

    // fabric_stitches에서도 삭제
    db.getDB().run('DELETE FROM fabric_stitches WHERE stitch_id = ?', [req.params.id]);
    // 교차 연결 삭제
    db.getDB().run('DELETE FROM connections WHERE node_a_id = ? OR node_b_id = ?', [req.params.id, req.params.id]);
    // 코 삭제
    db.getDB().run('DELETE FROM stitches WHERE id = ?', [req.params.id]);
    db.persist();

    res.status(204).end();
  } catch (err) {
    console.error('DELETE /api/stitches/:id error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;

/**
 * Connection Routes — 교차 연결 CRUD (다른 층위 간 연결)
 */
var express = require('express');
var router = express.Router();
var db = require('../db');

// POST /api/connections — 교차 연결 생성
router.post('/', function(req, res) {
  try {
    var body = req.body;
    if (!body.node_a_id || !body.node_b_id) {
      return res.status(400).json({ error: 'node_a_id and node_b_id are required' });
    }
    if (body.node_a_id === body.node_b_id) {
      return res.status(400).json({ error: 'Cannot connect a node to itself' });
    }

    var id = db.generateId('cn');
    var now = Date.now();

    db.getDB().run(
      'INSERT INTO connections (id, node_a_id, node_b_id, why, created_at) VALUES (?,?,?,?,?)',
      [id, body.node_a_id, body.node_b_id, body.why || '', now]
    );
    db.persist();

    var conn = db.getOne('SELECT * FROM connections WHERE id = ?', [id]);
    res.status(201).json(conn);
  } catch (err) {
    console.error('POST /api/connections error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/connections — 연결 목록
router.get('/', function(req, res) {
  try {
    var limit = Math.min(parseInt(req.query.limit) || 100, 500);
    var offset = parseInt(req.query.offset) || 0;
    var conns;
    if (req.query.node_id) {
      conns = db.getAll(
        'SELECT * FROM connections WHERE node_a_id = ? OR node_b_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?',
        [req.query.node_id, req.query.node_id, limit, offset]
      );
    } else {
      conns = db.getAll('SELECT * FROM connections ORDER BY created_at DESC LIMIT ? OFFSET ?', [limit, offset]);
    }
    res.json(conns);
  } catch (err) {
    console.error('GET /api/connections error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// DELETE /api/connections/:id — 연결 삭제
router.delete('/:id', function(req, res) {
  try {
    var conn = db.getOne('SELECT id FROM connections WHERE id = ?', [req.params.id]);
    if (!conn) return res.status(404).json({ error: 'Not found' });

    db.getDB().run('DELETE FROM connections WHERE id = ?', [req.params.id]);
    db.persist();

    res.status(204).end();
  } catch (err) {
    console.error('DELETE /api/connections/:id error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;

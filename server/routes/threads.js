/**
 * Thread Routes — 실 CRUD (올+올 연결)
 */
var express = require('express');
var router = express.Router();
var db = require('../db');
var hint = require('../services/hint');

// POST /api/threads — 실 잣기 (올+올 연결)
router.post('/', function(req, res) {
  try {
    var body = req.body;
    if (!body.fiber_a_id || !body.fiber_b_id) {
      return res.status(400).json({ error: 'fiber_a_id and fiber_b_id are required' });
    }
    if (body.fiber_a_id === body.fiber_b_id) {
      return res.status(400).json({ error: 'Cannot connect a fiber to itself' });
    }

    var fiberA = db.getOne('SELECT id FROM fibers WHERE id = ?', [body.fiber_a_id]);
    var fiberB = db.getOne('SELECT id FROM fibers WHERE id = ?', [body.fiber_b_id]);
    if (!fiberA || !fiberB) {
      return res.status(404).json({ error: 'One or both fibers not found' });
    }

    var id = db.generateId('th');
    var now = Date.now();

    db.getDB().run(
      'INSERT INTO threads (id, fiber_a_id, fiber_b_id, why, created_at) VALUES (?,?,?,?,?)',
      [id, body.fiber_a_id, body.fiber_b_id, body.why || '', now]
    );
    db.persist();

    var thread = _getThreadWithFibers(id);
    res.status(201).json(thread);

    // 실의 임베딩 생성 (why + 양쪽 올 텍스트 결합)
    var textParts = [];
    if (body.why) textParts.push(body.why);
    var fa = db.getOne('SELECT text FROM fibers WHERE id = ?', [body.fiber_a_id]);
    var fb = db.getOne('SELECT text FROM fibers WHERE id = ?', [body.fiber_b_id]);
    if (fa) textParts.push(fa.text);
    if (fb) textParts.push(fb.text);
    if (textParts.length > 0) {
      hint.saveEmbedding(id, textParts.join(' ')).catch(function(err) {
        console.error('[embedding] 실 임베딩 생성 실패:', err.message);
      });
    }
  } catch (err) {
    console.error('POST /api/threads error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/threads — 실 목록
router.get('/', function(req, res) {
  try {
    var limit = Math.min(parseInt(req.query.limit) || 100, 500);
    var offset = parseInt(req.query.offset) || 0;
    var threads;
    if (req.query.fiber_id) {
      threads = db.getAll(
        'SELECT * FROM threads WHERE fiber_a_id = ? OR fiber_b_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?',
        [req.query.fiber_id, req.query.fiber_id, limit, offset]
      );
    } else {
      threads = db.getAll('SELECT * FROM threads ORDER BY created_at DESC LIMIT ? OFFSET ?', [limit, offset]);
    }
    res.json(threads);
  } catch (err) {
    console.error('GET /api/threads error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/threads/:id — 실 상세 (양쪽 올 포함)
router.get('/:id', function(req, res) {
  try {
    var thread = _getThreadWithFibers(req.params.id);
    if (!thread) return res.status(404).json({ error: 'Not found' });
    res.json(thread);
  } catch (err) {
    console.error('GET /api/threads/:id error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// DELETE /api/threads/:id — 실 삭제
router.delete('/:id', function(req, res) {
  try {
    var thread = db.getOne('SELECT id FROM threads WHERE id = ?', [req.params.id]);
    if (!thread) return res.status(404).json({ error: 'Not found' });

    // 관련 stitches 삭제
    db.getDB().run('DELETE FROM stitches WHERE thread_a_id = ? OR thread_b_id = ?', [req.params.id, req.params.id]);
    // 교차 연결 삭제
    db.getDB().run('DELETE FROM connections WHERE node_a_id = ? OR node_b_id = ?', [req.params.id, req.params.id]);
    // 임베딩 삭제
    hint.deleteEmbedding(req.params.id);
    // 실 삭제
    db.getDB().run('DELETE FROM threads WHERE id = ?', [req.params.id]);
    db.persist();

    res.status(204).end();
  } catch (err) {
    console.error('DELETE /api/threads/:id error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Helper: thread with fiber details
function _getThreadWithFibers(id) {
  var thread = db.getOne('SELECT * FROM threads WHERE id = ?', [id]);
  if (!thread) return null;
  thread.fiber_a = db.getOne('SELECT id, text, tension, tone FROM fibers WHERE id = ?', [thread.fiber_a_id]);
  thread.fiber_b = db.getOne('SELECT id, text, tension, tone FROM fibers WHERE id = ?', [thread.fiber_b_id]);
  return thread;
}

module.exports = router;

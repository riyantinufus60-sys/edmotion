const express = require("express");
const path    = require("path");
const bcrypt  = require("bcrypt");
const session = require("express-session");
const db      = require("./db");
const history = require("./history");

const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

/* ── Session ── */
app.use(session({
  secret: process.env.SESSION_SECRET || "edmotion-secret-key",
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 1000 * 60 * 60 * 8 }
}));

app.use(express.static("public"));

/* =========================
   LIVE SESSION STATE — tetap in-memory untuk real-time SSE
========================= */
const liveState = {
  students: {},
  feed:     [],
  summaries: {}, // persisted summaries (key: studentId)
};

/* Load summaries dari DB saat server start */
function loadSummariesFromDB() {
  if (!db) return;
  db.query("SELECT * FROM session_summary ORDER BY updated_at DESC", (err, rows) => {
    if (err) { console.warn("[startup] Gagal load session_summary:", err.message); return; }
    rows.forEach(row => {
      try {
        liveState.summaries[String(row.student_id)] = {
          studentId:   String(row.student_id),
          studentName: row.student_name,
          dominant:    row.dominant,
          totalDurasi: row.total_durasi,
          videos:      JSON.parse(row.videos_json || "[]"),
          timestamp:   new Date(row.updated_at).getTime(),
        };
      } catch(e) {}
    });
    console.log(`[startup] Loaded ${rows.length} summaries dari DB`);
  });
}
setTimeout(loadSummariesFromDB, 2000);

function pushFeed(entry) {
  liveState.feed.unshift(entry);
  if (liveState.feed.length > 50) liveState.feed.length = 50;
}

function resetLiveState() {
  liveState.students = {};
  liveState.feed     = [];
}

function getAggregateStats() {
  const emoData = { Normal: 0, Bosan: 0, Bingung: 0, Menguap: 0 };
  let totalData = 0;
  Object.values(liveState.students).forEach(s => {
    totalData += s.totalData || 0;
    Object.keys(emoData).forEach(k => { emoData[k] += (s.emoData?.[k] || 0); });
  });
  return { emoData, totalData };
}

function normalizeEmotion(raw) {
  const s = String(raw || "").trim().toLowerCase().replace(/[^\w\s]/g, "").trim();
  if (s.includes("bosan")   || s.includes("bored"))                           return "Bosan";
  if (s.includes("bingung") || s.includes("confus") || s.includes("fearful")) return "Bingung";
  if (s.includes("menguap") || s.includes("sleep")  || s.includes("surpris")) return "Menguap";
  return "Normal";
}

/* =========================
   HELPER: Hitung summary per siswa dari tabel detection
========================= */
function buildSummariesFromDetection(callback) {
  history.query(
    `SELECT
       d.student_id,
       u.name        AS student_name,
       d.video_type,
       d.expression,
       d.duration
     FROM detection d
     LEFT JOIN users u ON u.id = d.student_id
     WHERE d.student_id IS NOT NULL
     ORDER BY d.student_id, d.video_type, d.id ASC`,
    (err, rows) => {
      if (err) return callback(err, null);

      const byStudent = {};

      rows.forEach(row => {
        const sid   = String(row.student_id);
        const sname = row.student_name
                      || liveState.students[sid]?.name
                      || "Siswa";
        const vark  = row.video_type || "unknown";
        const emo   = normalizeEmotion(row.expression);
        const dur   = parseFloat(row.duration) || 0;

        if (!byStudent[sid]) {
          byStudent[sid] = { studentId: sid, studentName: sname, videos: {}, totalDurasi: 0 };
        }
        if (row.student_name) byStudent[sid].studentName = row.student_name;

        if (!byStudent[sid].videos[vark]) {
          byStudent[sid].videos[vark] = {
            varkType: vark,
            durasi: 0,
            counts: { Normal: 0, Bosan: 0, Bingung: 0, Menguap: 0 },
          };
        }

        byStudent[sid].videos[vark].durasi += dur;
        byStudent[sid].videos[vark].counts[emo] = (byStudent[sid].videos[vark].counts[emo] || 0) + 1;
        byStudent[sid].totalDurasi += dur;
      });

      const summaries = Object.values(byStudent).map(s => {
        const videosArr = Object.values(s.videos).map(v => {
          const total = Object.values(v.counts).reduce((a, b) => a + b, 0);
          const pct   = k => total > 0 ? Math.round(v.counts[k] / total * 100) : 0;
          const dominant = total > 0
            ? Object.entries(v.counts).reduce((a, b) => a[1] > b[1] ? a : b)[0]
            : "Normal";
          return {
            varkType:   v.varkType,
            durasi:     Math.round(v.durasi),
            dominant,
            pctNormal:  pct("Normal"),
            pctBosan:   pct("Bosan"),
            pctBingung: pct("Bingung"),
            pctMenguap: pct("Menguap"),
            rawCounts:  { ...v.counts },
          };
        });

        const totalCounts = { Normal: 0, Bosan: 0, Bingung: 0, Menguap: 0 };
        Object.values(s.videos).forEach(v => {
          Object.keys(totalCounts).forEach(k => { totalCounts[k] += v.counts[k] || 0; });
        });
        const grandTotal = Object.values(totalCounts).reduce((a, b) => a + b, 0);
        const dominant   = grandTotal > 0
          ? Object.entries(totalCounts).reduce((a, b) => a[1] > b[1] ? a : b)[0]
          : "Normal";

        return {
          studentId:   s.studentId,
          studentName: s.studentName,
          dominant,
          totalDurasi: Math.round(s.totalDurasi),
          videos:      videosArr,
          timestamp:   Date.now(),
        };
      });

      callback(null, summaries);
    }
  );
}

/* =========================
   SSE CLIENTS
========================= */
const sseClients = new Set();

function broadcast(eventType, data) {
  const payload = JSON.stringify({ type: eventType, data });
  for (const client of sseClients) {
    client.write(`data: ${payload}\n\n`);
  }
}

/* =========================
   MIDDLEWARE
========================= */
function requireLogin(req, res, next) {
  if (!req.session.user) {
    if (req.path.startsWith("/api/")) return res.status(401).json({ error: "unauthorized" });
    return res.redirect("/");
  }
  next();
}

/* =========================
   ROUTE HTML
========================= */
app.get("/", (req, res) => res.sendFile(path.join(__dirname, "views", "index.html")));
app.get("/dashboard",      requireLogin, (req, res) => res.sendFile(path.join(__dirname, "views", "dashboard.html")));
app.get("/dashboard-siswa",requireLogin, (req, res) => res.sendFile(path.join(__dirname, "views", "dashboard_siswa.html")));
app.get("/history",        requireLogin, (req, res) => res.sendFile(path.join(__dirname, "views", "history.html")));
app.get("/live-report",    requireLogin, (req, res) => res.sendFile(path.join(__dirname, "views", "live-report.html")));
app.get("/users",          requireLogin, (req, res) => res.sendFile(path.join(__dirname, "views", "users.html")));

/* =========================
   LOGIN API
========================= */
app.post("/api/login", (req, res) => {
  const { email, password, role } = req.body;
  if (!email || !password) return res.send("invalid_input");

  db.query("SELECT * FROM users WHERE email = ? AND role = ?", [email, role], async (err, results) => {
    if (err) { console.error(err); return res.send("server_error"); }
    if (results.length === 0) return res.send("user_not_found");

    const user  = results[0];
    const match = await bcrypt.compare(password, user.password);
    if (!match) return res.send("wrong_password");

    req.session.user = { id: user.id, name: user.name, role: user.role };
    return res.send("success");
  });
});

/* =========================
   REGISTER API
========================= */
app.post("/api/register", async (req, res) => {
  const { name, email, password, confirm, role } = req.body;
  if (!name || !email || !password || !confirm || !role) return res.send("invalid_input");
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))        return res.send("invalid_email");
  if (role === "siswa" && !email.includes("@student"))   return res.send("email_student_required");
  if (role === "admin" && email.includes("@student"))    return res.send("admin_cannot_use_student_email");
  if (password.length < 8)  return res.send("password_too_short");
  if (password !== confirm)  return res.send("password_tidak_cocok");

  db.query("SELECT id FROM users WHERE email = ?", [email], async (err, results) => {
    if (err) { console.error(err); return res.send("server_error"); }
    if (results.length > 0) return res.send("email_sudah_ada");

    const hashed = await bcrypt.hash(password, 10);
    db.query(
      "INSERT INTO users (name, email, password, role) VALUES (?, ?, ?, ?)",
      [name, email, hashed, role],
      (err) => {
        if (err) { console.error(err); return res.send("server_error"); }
        return res.send("success");
      }
    );
  });
});

/* =========================
   ME / LOGOUT API
========================= */
app.get("/api/me", (req, res) => {
  if (!req.session.user) return res.status(401).json({ error: "unauthorized" });
  res.json(req.session.user);
});

app.post("/api/logout", (req, res) => {
  req.session.destroy(err => {
    if (err) { console.error(err); return res.status(500).send("logout_failed"); }
    res.send("ok");
  });
});

/* =========================
   USERS API
========================= */
app.get("/api/users", requireLogin, (req, res) => {
  db.query(
    "SELECT id, name, email, password, role, created_at FROM users ORDER BY id ASC",
    (err, rows) => {
      if (err) { console.error(err); return res.status(500).json({ error: "db_error" }); }
      res.json({ users: rows });
    }
  );
});

app.delete("/api/users/:id", requireLogin, (req, res) => {
  const targetId = parseInt(req.params.id);
  if (isNaN(targetId))             return res.status(400).json({ success: false, message: "ID tidak valid." });
  if (targetId === req.session.user.id) return res.status(403).json({ success: false, message: "Tidak bisa menghapus akun sendiri." });

  db.query("DELETE FROM users WHERE id = ?", [targetId], (err, result) => {
    if (err)                        return res.status(500).json({ success: false, message: "Database error." });
    if (result.affectedRows === 0)  return res.status(404).json({ success: false, message: "User tidak ditemukan." });

    history.query("DELETE FROM detection WHERE student_id = ?", [targetId], (err2) => {
      if (err2) console.warn("[DB] Gagal hapus detection untuk user", targetId, err2);
    });

    res.json({ success: true });
  });
});

/* =========================
   DETECTION API
========================= */
app.post("/api/detection", requireLogin, (req, res) => {
  const { face_detection, expression, duration, waktu, student_id, student_name, vark_type } = req.body;

  const dur = parseFloat(duration);
  if (!dur || dur < 1.5) return res.status(400).json({ success: false, error: "Duration too short" });

  const faceDetect = face_detection ? 1 : 0;
  const expr       = expression || "Unknown";
  const vark       = vark_type  || null;
  const sid        = student_id ? parseInt(student_id) : null;
  const localTime  = waktu
    ? new Date(waktu).toISOString().slice(0, 19).replace("T", " ")
    : new Date().toISOString().slice(0, 19).replace("T", " ");
  const createdAt  = new Date().toISOString().slice(0, 19).replace("T", " ");

  history.query(
    `INSERT INTO detection (face_detection, expression, duration, local_time, created_at, video_type, student_id)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [faceDetect, expr, dur, localTime, createdAt, vark, sid],
    (err, result) => {
      if (err) { console.error("Detection insert error:", err); return res.status(500).json({ success: false, error: "db_error" }); }

      const sidStr  = String(student_id   || req.session.user?.id   || "unknown");
      const sname   = String(student_name || req.session.user?.name || "Siswa");
      const varkStr = String(vark_type    || "-");
      const emotion = normalizeEmotion(expr);
      const timeStr = new Date().toLocaleTimeString("id-ID");

      if (!liveState.students[sidStr]) {
        liveState.students[sidStr] = {
          name: sname, lastEmotion: emotion, varkType: varkStr,
          lastSeen: Date.now(), totalData: 0,
          liveDurasi: 0,
          emoData: { Normal: 0, Bosan: 0, Bingung: 0, Menguap: 0 },
        };
      } else {
        Object.assign(liveState.students[sidStr], { name: sname, lastEmotion: emotion, varkType: varkStr, lastSeen: Date.now() });
      }

      liveState.students[sidStr].totalData++;
      liveState.students[sidStr].liveDurasi = (liveState.students[sidStr].liveDurasi || 0) + dur;
      liveState.students[sidStr].emoData[emotion] = (liveState.students[sidStr].emoData[emotion] || 0) + 1;

      const feedEntry = {
        icon: { Normal:"🙂", Bosan:"😐", Bingung:"🤔", Menguap:"😮" }[emotion] || "🙂",
        main: sname + " — " + emotion,
        sub:  "VARK: " + varkStr,
        dur:  dur.toFixed(1) + "s",
        time: timeStr,
        student_id: sidStr,
      };
      pushFeed(feedEntry);

      const agg = getAggregateStats();
      broadcast("detection", {
        id: result.insertId, student_id: sidStr, student_name: sname,
        vark_type: varkStr, expression: expr, duration: dur, local_time: localTime,
        live_durasi: liveState.students[sidStr].liveDurasi,
        feed: feedEntry, agg_total: agg.totalData, agg_emoData: agg.emoData,
      });

      res.json({ success: true, id: result.insertId });
    }
  );
});

/* =========================
   FEED API — ✅ BARU
   Ambil feed dari DB per siswa, dipisah berdasarkan student_id.
   Tidak ada pencampuran data antar siswa.
========================= */
app.get("/api/feed", requireLogin, (req, res) => {
  const sid = req.query.student_id ? String(req.query.student_id) : null;

  // Kalau ada student_id → ambil SEMUA data siswa itu (tidak dibatasi 50)
  // Kalau tidak ada → ambil 100 terbaru dari semua siswa
  const sql = sid
    ? `SELECT d.student_id, u.name AS student_name, d.video_type, d.expression, d.local_time
       FROM detection d
       LEFT JOIN users u ON u.id = d.student_id
       WHERE d.student_id = ?
       ORDER BY d.id DESC`
    : `SELECT d.student_id, u.name AS student_name, d.video_type, d.expression, d.local_time
       FROM detection d
       LEFT JOIN users u ON u.id = d.student_id
       WHERE d.student_id IS NOT NULL
       ORDER BY d.id DESC
       LIMIT 100`;

  const params = sid ? [sid] : [];

  history.query(sql, params, (err, rows) => {
    if (err) { console.error("[feed] DB error:", err); return res.status(500).json({ error: "db_error" }); }

    const emojiMap = { Normal:"🙂", Bosan:"😐", Bingung:"🤔", Menguap:"😮" };

    const feed = rows.map(row => {
      const emotion = normalizeEmotion(row.expression);
      const time    = row.local_time
        ? new Date(row.local_time).toLocaleTimeString("id-ID")
        : "--:--:--";
      return {
        icon:       emojiMap[emotion] || "🙂",
        main:       (row.student_name || "Siswa") + " — " + emotion,
        sub:        "VARK: " + (row.video_type || "-"),
        time,
        student_id: String(row.student_id),
      };
    });

    res.json({ feed });
  });
});

/* =========================
   HISTORY API
========================= */
app.get("/api/history", requireLogin, (req, res) => {
  history.query("SELECT * FROM detection ORDER BY id ASC", (err, rows) => {
    if (err) { console.error(err); return res.status(500).json({ error: "db_error" }); }
    const maxId = rows.length > 0 ? rows[rows.length - 1].id : 0;
    res.json({ rows, maxId, total: rows.length });
  });
});

app.get("/api/history/latest", requireLogin, (req, res) => {
  const since = parseInt(req.query.since) || 0;
  history.query("SELECT * FROM detection WHERE id > ? ORDER BY id ASC", [since], (err, newRows) => {
    if (err) { console.error(err); return res.status(500).json({ error: "db_error" }); }
    history.query("SELECT COUNT(*) AS total, MAX(id) AS maxId FROM detection", (err2, totals) => {
      if (err2) { console.error(err2); return res.status(500).json({ error: "db_error" }); }
      res.json({ rows: newRows, maxId: totals[0].maxId || since, total: totals[0].total || 0 });
    });
  });
});

app.post("/api/history/clear", requireLogin, (req, res) => {
  history.query("DELETE FROM detection", (err) => {
    if (err) { console.error(err); return res.status(500).json({ success: false }); }
    history.query("ALTER TABLE detection AUTO_INCREMENT = 1", (err2) => {
      if (err2) console.warn("Could not reset AUTO_INCREMENT:", err2);
      res.json({ success: true });
    });
  });
});

/* =========================
   LIVE STATE API
========================= */
app.get("/api/live-state", requireLogin, (req, res) => {
  const agg = getAggregateStats();
  res.json({ students: liveState.students, feed: liveState.feed, emoData: agg.emoData, totalData: agg.totalData });
});

app.post("/api/live-state/reset", requireLogin, (req, res) => {
  resetLiveState();
  broadcast("reset", {});
  res.json({ success: true });
});

/* =========================
   SESSION SUMMARY API — SAVE (POST)
========================= */
app.post("/api/session-summary/save", requireLogin, (req, res) => {
  const data = req.body;
  if (!data || !data.studentId) return res.status(400).json({ success: false, error: "Data tidak valid" });

  const sid = String(data.studentId);

  history.query(
    `SELECT d.student_id, u.name AS student_name, d.video_type, d.expression, d.duration
     FROM detection d
     LEFT JOIN users u ON u.id = d.student_id
     WHERE d.student_id = ?
     ORDER BY d.id ASC`,
    [sid],
    (err, rows) => {
      if (err) {
        console.error("[summary/save] DB error:", err);
        broadcast("session-summary", {
          studentId:   sid,
          studentName: data.studentName || liveState.students[sid]?.name || "Siswa",
          dominant:    data.dominant    || "Normal",
          totalDurasi: data.totalDurasi || 0,
          videos:      Array.isArray(data.videos) ? data.videos : [],
          timestamp:   Date.now(),
        });
        return res.json({ success: true });
      }

      const videoMap = {};
      let totalDurasi = 0;

      rows.forEach(row => {
        const vark = row.video_type || "unknown";
        const emo  = normalizeEmotion(row.expression);
        const dur  = parseFloat(row.duration) || 0;

        if (!videoMap[vark]) {
          videoMap[vark] = { varkType: vark, durasi: 0, counts: { Normal:0, Bosan:0, Bingung:0, Menguap:0 } };
        }
        videoMap[vark].durasi += dur;
        videoMap[vark].counts[emo]++;
        totalDurasi += dur;
      });

      const videosArr = Object.values(videoMap).map(v => {
        const total    = Object.values(v.counts).reduce((a, b) => a + b, 0);
        const pct      = k => total > 0 ? Math.round(v.counts[k] / total * 100) : 0;
        const dominant = total > 0
          ? Object.entries(v.counts).reduce((a, b) => a[1] > b[1] ? a : b)[0]
          : "Normal";
        return {
          varkType:   v.varkType,
          durasi:     Math.round(v.durasi),
          dominant,
          pctNormal:  pct("Normal"),
          pctBosan:   pct("Bosan"),
          pctBingung: pct("Bingung"),
          pctMenguap: pct("Menguap"),
          rawCounts:  { ...v.counts },
        };
      });

      const totalCounts = { Normal:0, Bosan:0, Bingung:0, Menguap:0 };
      Object.values(videoMap).forEach(v => {
        Object.keys(totalCounts).forEach(k => { totalCounts[k] += v.counts[k] || 0; });
      });
      const grandTotal = Object.values(totalCounts).reduce((a, b) => a + b, 0);
      const dominant   = grandTotal > 0
        ? Object.entries(totalCounts).reduce((a, b) => a[1] > b[1] ? a : b)[0]
        : "Normal";

      const sname = (rows[0]?.student_name) || liveState.students[sid]?.name || data.studentName || "Siswa";

      const summary = {
        studentId:   sid,
        studentName: sname,
        dominant,
        totalDurasi: Math.round(totalDurasi),
        videos:      videosArr,
        timestamp:   Date.now(),
      };

      // Simpan ke DB agar tidak hilang saat restart
      db.query(
        `INSERT INTO session_summary (student_id, student_name, dominant, total_durasi, videos_json, updated_at)
         VALUES (?, ?, ?, ?, ?, NOW())
         ON DUPLICATE KEY UPDATE
           student_name=VALUES(student_name),
           dominant=VALUES(dominant),
           total_durasi=VALUES(total_durasi),
           videos_json=VALUES(videos_json),
           updated_at=NOW()`,
        [sid, sname, dominant, Math.round(totalDurasi), JSON.stringify(videosArr)],
        (dbErr) => { if (dbErr) console.warn("[summary/save] Gagal simpan ke session_summary:", dbErr.message); }
      );
      // Simpan ke memory juga
      liveState.summaries[sid] = summary;

      broadcast("session-summary", summary);
      console.log(`[session-summary] sid=${sid}, dominant=${dominant}, durasi=${Math.round(totalDurasi)}s`);
      res.json({ success: true });
    }
  );
});

/* =========================
   SESSION SUMMARY API — GET
========================= */
app.get("/api/session-summary", requireLogin, (req, res) => {
  buildSummariesFromDetection((err, summaries) => {
    if (err) { console.error("[summary/get] DB error:", err); return res.status(500).json({ error: "db_error" }); }
    res.json({ summaries });
  });
});

/* =========================
   SESSION SUMMARY API — CLEAR ALL
========================= */
app.post("/api/session-summary/clear", requireLogin, (req, res) => {
  history.query("DELETE FROM detection WHERE student_id IS NOT NULL", (err) => {
    if (err) { console.error(err); return res.status(500).json({ success: false }); }
    // Hapus juga dari session_summary dan memory
    db.query("DELETE FROM session_summary", () => {});
    liveState.summaries = {};
    res.json({ success: true });
  });
});

/* =========================
   SESSION SUMMARY API — RESET PER SISWA
========================= */
app.post("/api/session-summary/reset-student", requireLogin, (req, res) => {
  const sid = String(req.session.user?.id || "");
  if (!sid) return res.status(400).json({ error: "invalid" });

  history.query("DELETE FROM detection WHERE student_id = ?", [sid], (err) => {
    if (err) { console.error("[reset-student] DB error:", err); return res.status(500).json({ success: false }); }

    if (liveState.students[sid]) {
      liveState.students[sid].totalData  = 0;
      liveState.students[sid].liveDurasi = 0;
      liveState.students[sid].emoData    = { Normal: 0, Bosan: 0, Bingung: 0, Menguap: 0 };
    }

    broadcast("student-session-reset", { studentId: sid });

    const agg = getAggregateStats();
    broadcast("stats-update", { agg_total: agg.totalData, agg_emoData: agg.emoData });

    console.log(`[session-reset] Student ${sid} reset — detection dihapus dari DB`);
    res.json({ success: true });
  });
});

/* =========================
   SSE STREAM API
========================= */
app.get("/api/stream", requireLogin, (req, res) => {
  res.setHeader("Content-Type",      "text/event-stream");
  res.setHeader("Cache-Control",     "no-cache");
  res.setHeader("Connection",        "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders();

  const agg      = getAggregateStats();
  const snapshot = JSON.stringify({
    type: "snapshot",
    data: {
      students:  liveState.students,
      feed:      liveState.feed,
      emoData:   agg.emoData,
      totalData: agg.totalData,
    }
  });
  res.write(`data: ${snapshot}\n\n`);

  // Kirim summaries yang sudah tersimpan (dari DB via liveState.summaries)
  Object.values(liveState.summaries).forEach(summary => {
    const payload = JSON.stringify({ type: "session-summary", data: summary });
    res.write(`data: ${payload}\n\n`);
  });

  const heartbeat = setInterval(() => res.write(": heartbeat\n\n"), 25000);
  sseClients.add(res);
  req.on("close", () => { clearInterval(heartbeat); sseClients.delete(res); });
});

/* =========================
   SERVER
========================= */

/* =============================================================
   ACTUAL LABELS — Input manual guru/admin
   ============================================================= */

// GET semua actual_labels (untuk history page)
app.get("/api/actual-labels", requireLogin, (req, res) => {
  history.query(
    "SELECT detection_id, actual_label FROM actual_labels",
    (err, rows) => {
      if (err) return res.status(500).json({ error: "db_error" });
      const map = {};
      rows.forEach(r => { map[r.detection_id] = r.actual_label; });
      res.json({ labels: map });
    }
  );
});

// POST simpan/update actual_label untuk satu detection row
app.post("/api/actual-labels", requireLogin, (req, res) => {
  const { detection_id, student_id, actual_label } = req.body;
  if (!detection_id || !actual_label) return res.status(400).json({ error: "invalid" });
  const adminId = req.session.user?.id || null;
  history.query(
    `INSERT INTO actual_labels (detection_id, student_id, actual_label, labeled_by)
     VALUES (?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE actual_label=VALUES(actual_label), labeled_by=VALUES(labeled_by)`,
    [detection_id, student_id || 0, actual_label, adminId],
    (err) => {
      if (err) { console.error("[actual-labels] save error:", err); return res.status(500).json({ error: "db_error" }); }
      res.json({ success: true });
    }
  );
});

// GET confusion matrix untuk student_id tertentu (atau semua)
app.get("/api/confusion-matrix", requireLogin, (req, res) => {
  const sid = req.query.student_id ? parseInt(req.query.student_id) : null;
  const whereClause = sid ? "WHERE d.student_id = ?" : "";
  const params = sid ? [sid] : [];

  const sql = `SELECT d.expression AS predicted, al.actual_label AS actual
     FROM detection d
     INNER JOIN actual_labels al ON al.detection_id = d.id
     ${whereClause}`;

  history.query(
    sql,
    params,
    (err, rows) => {
      if (err) return res.status(500).json({ error: "db_error" });

      const labels = ["Normal", "Bosan", "Bingung", "Menguap"];
      // Inisialisasi matrix 4x4
      const matrix = {};
      labels.forEach(a => {
        matrix[a] = {};
        labels.forEach(p => { matrix[a][p] = 0; });
      });

      let total = 0;
      rows.forEach(({ predicted, actual }) => {
        const norm = (e) => {
          e = String(e || "").toLowerCase();
          if (e.includes("bos") || e.includes("bored")) return "Bosan";
          if (e.includes("con") || e.includes("fear") || e.includes("bing")) return "Bingung";
          if (e.includes("yaw") || e.includes("sle") || e.includes("mengu")) return "Menguap";
          return "Normal";
        };
        const a = norm(actual);
        const p = norm(predicted);
        if (matrix[a] && matrix[a][p] !== undefined) {
          matrix[a][p]++;
          total++;
        }
      });

      // Hitung metrik per kelas
      const metrics = {};
      labels.forEach(lbl => {
        const tp = matrix[lbl][lbl];
        const fp = labels.reduce((s, a) => s + (a !== lbl ? matrix[a][lbl] : 0), 0);
        const fn = labels.reduce((s, p) => s + (p !== lbl ? matrix[lbl][p] : 0), 0);
        const precision = (tp + fp) > 0 ? tp / (tp + fp) : 0;
        const recall    = (tp + fn) > 0 ? tp / (tp + fn) : 0;
        const f1        = (precision + recall) > 0 ? 2 * precision * recall / (precision + recall) : 0;
        metrics[lbl] = { tp, fp, fn, precision, recall, f1 };
      });

      const correct = labels.reduce((s, l) => s + matrix[l][l], 0);
      const accuracy = total > 0 ? correct / total : 0;

      res.json({ matrix, labels, metrics, accuracy, total });
    }
  );
});

// ══════════════════════════════════════════════════════
// POST /api/actual-labels/generate
// Otomatis isi actual_label berdasarkan ekspresi mayoritas
// per (student_id, video_type) — tanpa perlu input manual
// ══════════════════════════════════════════════════════
app.post("/api/actual-labels/generate", requireLogin, (req, res) => {
  const sid = req.body.student_id ? parseInt(req.body.student_id) : null;
  const where = sid ? "WHERE d.student_id = ?" : "";
  const params = sid ? [sid] : [];

  // Step 1: ambil semua detection, hitung mayoritas ekspresi per (student_id, video_type)
  history.query(
    `SELECT d.id AS detection_id, d.student_id, d.video_type, d.expression
     FROM detection d
     WHERE d.student_id IS NOT NULL
     ${sid ? "AND d.student_id = ?" : ""}
     ORDER BY d.student_id, d.video_type, d.id`,
    sid ? [sid] : [],
    (err, rows) => {
      if (err) return res.status(500).json({ error: "db_error", detail: err.message });

      const normalizeEmo = (e) => {
        e = String(e || "").toLowerCase();
        if (e.includes("bos") || e.includes("bored"))              return "Bosan";
        if (e.includes("con") || e.includes("fear") || e.includes("bing")) return "Bingung";
        if (e.includes("yaw") || e.includes("sle") || e.includes("mengu")) return "Menguap";
        return "Normal";
      };

      // Hitung frekuensi per (student_id, video_type)
      const groups = {};
      rows.forEach(r => {
        const key = `${r.student_id}||${r.video_type || "unknown"}`;
        if (!groups[key]) groups[key] = { counts: {}, ids: [] };
        const emo = normalizeEmo(r.expression);
        groups[key].counts[emo] = (groups[key].counts[emo] || 0) + 1;
        groups[key].ids.push(r.detection_id);
      });

      // Tentukan mayoritas per group
      const insertRows = [];
      Object.values(groups).forEach(g => {
        const dominant = Object.entries(g.counts)
          .sort((a, b) => b[1] - a[1])[0][0];
        g.ids.forEach(did => insertRows.push([did, dominant]));
      });

      if (insertRows.length === 0)
        return res.json({ success: true, generated: 0, message: "Tidak ada data untuk di-generate" });

      // Step 2: bulk insert/update actual_labels
      const adminId = req.session.user?.id || null;
      let done = 0;
      const batchSize = 100;

      const insertBatch = (batch) => {
        const placeholders = batch.map(() => "(?, 0, ?, ?)").join(",");
        const vals = batch.flatMap(([did, lbl]) => [did, lbl, adminId]);
        history.query(
          `INSERT INTO actual_labels (detection_id, student_id, actual_label, labeled_by)
           VALUES ${placeholders}
           ON DUPLICATE KEY UPDATE actual_label=VALUES(actual_label), labeled_by=VALUES(labeled_by)`,
          vals,
          (e2) => {
            if (e2) console.warn("[generate] batch error:", e2.message);
            done += batch.length;
            if (done >= insertRows.length) {
              console.log(`[generate] Selesai: ${done} actual_labels di-generate`);
              res.json({ success: true, generated: done });
            }
          }
        );
      };

      // Proses per batch 100
      for (let i = 0; i < insertRows.length; i += batchSize) {
        insertBatch(insertRows.slice(i, i + batchSize));
      }
    }
  );
});

/* =========================
   404 HANDLER
========================= */
app.use((req, res) => res.status(404).send("404 — Halaman tidak ditemukan."));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`[SERVER] Running on port ${PORT}`));
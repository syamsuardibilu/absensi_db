// server.unified.js — single source for Local & Render
require("dotenv").config();

const express = require("express");
const mysql = require("mysql2");
const cors = require("cors");
const fs = require("fs");
const path = require("path");

const app = express();

// ===== Env (works on Render + Local) =====
const NODE_ENV = process.env.NODE_ENV || "development";
const PORT = Number(process.env.PORT || 3000);
const INTERNAL_BASE_URL = `http://127.0.0.1:${PORT}`;
const BASE_URL =
  process.env.BASE_URL || process.env.RENDER_EXTERNAL_URL || INTERNAL_BASE_URL;

// Polyfill fetch for Node < 18
const fetch = global.fetch
  ? global.fetch
  : (...args) => import("node-fetch").then(({ default: f }) => f(...args));

// CORS
const extraOrigins = (process.env.CORS_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
app.use(
  cors({
    origin: (origin, cb) => {
      const whitelist = new Set([
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://daengbilu.com",
        "https://www.daengbilu.com",
        "http://daengbilu.com",
        "http://www.daengbilu.com",
        ...extraOrigins,
      ]);
      if (!origin || whitelist.has(origin)) return cb(null, true);
      if (NODE_ENV !== "production") return cb(null, true);
      return cb(null, false);
    },
    credentials: true,
  })
);

app.use(express.json({ limit: "100mb" }));

// DB Pool
const conn = mysql.createPool({
  host: process.env.DB_HOST || "127.0.0.1",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "",
  database: process.env.DB_NAME || "absensi_db",
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Health + DB Ping
app.get("/api/health", (_req, res) => res.json({ ok: true }));
app.get("/api/ping-db", (_req, res) => {
  conn.query("SELECT NOW() AS waktu", (err, rows) => {
    if (err)
      return res
        .status(500)
        .json({ ok: false, error: err.message, code: err.code });
    res.json({ ok: true, waktu: rows[0].waktu });
  });
});

// === Date utilities (single source) ===
function formatTanggalSafe(input) {
  if (!input) return null;
  const d = new Date(input);
  if (isNaN(d.getTime())) return null;
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}
const formatTanggal = formatTanggalSafe;

// Health check
app.get("/api/health", (_req, res) => res.json({ ok: true }));

// Ping DB cepat
app.get("/api/ping-db", (_req, res) => {
  conn.query("SELECT NOW() AS waktu", (err, rows) => {
    if (err) return res.status(500).json({ ok: false, error: err.message });
    res.json({ ok: true, waktu: rows[0].waktu });
  });
});

app.use(
  cors({
    origin: [
      "https://daengbilu.com",
      "https://www.daengbilu.com",
      "http://daengbilu.com",
      "http://www.daengbilu.com",
    ],
  })
);

// // Helper function to convert seconds to HH:MM:SS
function toTimeString(seconds) {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;
  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(
    2,
    "0"
  )}:${String(secs).padStart(2, "0")}`;
}

// Koneksi DB via ENV (Hostinger + Render)
app.post("/update-perner-tanggal", (req, res) => {
  const { perners, startDate, endDate } = req.body;

  if (!perners || !Array.isArray(perners) || perners.length === 0) {
    return res.status(400).json({ message: "❌ Data perner tidak valid." });
  }

  const start = new Date(startDate);
  const end = new Date(endDate);
  const dates = [];

  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    const isoDate = new Date(d).toISOString().slice(0, 10);
    dates.push(isoDate);
  }

  let insertedCount = 0;
  const tasks = [];

  perners.forEach((perner) => {
    dates.forEach((tanggal) => {
      tasks.push(
        new Promise((resolve, reject) => {
          const tgl = new Date(tanggal);
          const isJumat = tgl.getDay() === 5 ? "jumat" : "bukan jumat";

          const hariList = [
            "minggu",
            "senin",
            "selasa",
            "rabu",
            "kamis",
            "jumat",
            "sabtu",
          ];
          const namaHari = hariList[tgl.getDay()];

          const sql = `
          INSERT INTO olah_absensi (perner, tanggal, is_jumat, nama_hari)
          VALUES (?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE 
            is_jumat = VALUES(is_jumat),
            nama_hari = VALUES(nama_hari)
        `;

          conn.query(
            sql,
            [perner, tanggal, isJumat, namaHari],
            (err, result) => {
              if (err) return reject(err);
              insertedCount += result.affectedRows;
              resolve();
            }
          );
        })
      );
    });
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        insertedCount,
        message: `✅ Berhasil menyisipkan ${insertedCount} data (perner + tanggal).`,
      });
    })
    .catch((err) => {
      console.error("❌ Database error:", err);
      res
        .status(500)
        .json({ message: "❌ Gagal menyimpan ke database.", error: err });
    });
});

app.post("/hapus-semua-data", (req, res) => {
  const sql = "DELETE FROM olah_absensi";

  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal menghapus semua data:", err);
      return res
        .status(500)
        .json({ message: "❌ Gagal menghapus semua data." });
    }
    res.json({ message: `✅ ${result.affectedRows} baris berhasil dihapus.` });
  });
});

app.post("/update-hari-libur", (req, res) => {
  const liburData = req.body.data;
  if (!Array.isArray(liburData))
    return res.status(400).json({ message: "❌ Data tidak valid" });

  const tasks = [];
  const tanggalLiburSet = new Set(liburData.map((d) => d.tanggal)); // untuk pengecekan "bukan libur"

  // Update tanggal libur
  liburData.forEach((item) => {
    const sql = `UPDATE olah_absensi SET jenis_hari = ? WHERE tanggal = ?`;
    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(sql, [item.jenis_hari, item.tanggal], (err, result) => {
          if (err) return reject(err);
          resolve();
        });
      })
    );
  });

  // Tanggal lain → "HARI KERJA"
  const sqlHariKerja = `
    UPDATE olah_absensi
    SET jenis_hari = 'HARI KERJA'
    WHERE (jenis_hari IS NULL OR jenis_hari = '')
      AND tanggal IS NOT NULL
  `;

  tasks.push(
    new Promise((resolve, reject) => {
      conn.query(sqlHariKerja, (err, result) => {
        if (err) return reject(err);
        resolve();
      });
    })
  );

  Promise.all(tasks)
    .then(() => {
      res.json({ message: `✅ Hari libur dan hari kerja berhasil diproses.` });
    })
    .catch((err) => {
      console.error("❌ Error hari libur:", err);
      res.status(500).json({ message: "❌ Gagal memproses hari libur." });
    });
});

app.post("/update-ci-co", (req, res) => {
  const { data } = req.body;
  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  const tasks = [];

  data.forEach(({ perner, tanggal, waktu, tipe, correction }) => {
    let timeColumn = "";
    let correctionColumn = "";

    // ✨ MODIFIED: Map both time and correction columns
    if (tipe.toUpperCase() === "CLOCK_IN") {
      timeColumn = "daily_in";
      correctionColumn = "correction_in";
    } else if (tipe.toUpperCase() === "CLOCK_OUT") {
      timeColumn = "daily_out";
      correctionColumn = "correction_out";
    } else {
      return; // Skip invalid tipe
    }

    // ✨ MODIFIED: Update both time and correction fields
    const sql = `
      UPDATE olah_absensi
      SET ${timeColumn} = ?, ${correctionColumn} = ?
      WHERE perner = ? AND tanggal = ?
    `;

    tasks.push(
      new Promise((resolve, reject) => {
        // ✨ MODIFIED: Include correction in parameters
        conn.query(sql, [waktu, correction, perner, tanggal], (err, result) => {
          if (err) return reject(err);
          resolve();
        });
      })
    );
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        message: `✅ Berhasil memproses ${data.length} data CI/CO dengan status koreksi.`,
      });
    })
    .catch((err) => {
      console.error("❌ Gagal update CI/CO:", err);
      res.status(500).json({ message: "❌ Gagal update data CI/CO." });
    });
});

app.get("/getAllData", (req, res) => {
  conn.query(
    "SELECT * FROM olah_absensi ORDER BY tanggal DESC",
    (err, result) => {
      if (err) return res.status(500).json({ error: "Gagal mengambil data" });
      res.json(result);
    }
  );
});

app.post("/hapus-data-db", (req, res) => {
  const { jenis } = req.body;
  let kolom = "";

  switch (jenis) {
    case "data_pegawai":
      kolom = "is_jumat = NULL, nama_hari = NULL";
      break;
    case "data_hari_libur":
      kolom = "jenis_hari = NULL";
      break;
    case "ci_co_daily":
      kolom = "daily_in = NULL, daily_out = NULL";
      break;
    case "att_abs_daily":
      kolom = "att_daily = NULL, abs_daily = NULL";
      break;
    case "att_sap":
      kolom = "att_sap = NULL";
      break;
    case "abs_sap":
      kolom = "abs_sap = NULL";
      break;
    case "sppd_umum":
      kolom = "sppd_umum = NULL";
      break;
    case "work_schedule":
      kolom = "ws_rule = NULL";
      break;
    case "substitution_daily":
      kolom = "jenis_jam_kerja_shift_daily = NULL";
      break;
    case "substitution_sap":
      kolom = "jenis_jam_kerja_shift_sap = NULL";
      break;
    default:
      return res.status(400).json({ message: "❌ Jenis data tidak dikenal." });
  }

  if (!kolom) {
    return res
      .status(400)
      .json({ message: "⚠️ Kolom belum ditentukan untuk jenis ini." });
  }

  const sql = `UPDATE olah_absensi SET ${kolom}`;
  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal hapus data:", err);
      return res
        .status(500)
        .json({ message: "❌ Gagal menghapus data dari DB." });
    }
    res.json({
      message: `✅ ${result.affectedRows} baris berhasil dikosongkan.`,
    });
  });
});

app.post("/update-att-abs-daily", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  const tasks = [];

  data.forEach(({ perner, tanggal, tipe_text, kategori }) => {
    const kolom =
      kategori === "att"
        ? "att_daily"
        : kategori === "abs"
        ? "abs_daily"
        : null;
    if (!kolom) return;

    const sql = `UPDATE olah_absensi SET ${kolom} = ? WHERE perner = ? AND tanggal = ?`;

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(sql, [tipe_text, perner, tanggal], (err) => {
          if (err) return reject(err);
          resolve();
        });
      })
    );
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        message: `✅ Berhasil memproses ${data.length} data ATT ABS Daily.`,
      });
    })
    .catch((err) => {
      console.error("❌ Gagal update ATT ABS:", err);
      res.status(500).json({ message: "❌ Gagal update ATT ABS Daily." });
    });
});

app.post("/update-att-sap", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  const tasks = [];

  data.forEach(({ perner, tanggal, tipe_text }) => {
    const sql = `
      UPDATE olah_absensi
      SET att_sap = ?
      WHERE perner = ? AND tanggal = ?
    `;

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(sql, [tipe_text, perner, tanggal], (err) => {
          if (err) return reject(err);
          resolve();
        });
      })
    );
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        message: `✅ Berhasil memproses ${data.length} data ATT ABS.`,
      });
    })
    .catch((err) => {
      console.error("❌ Gagal update ATT ABS:", err);
      res.status(500).json({ message: "❌ Gagal update ATT ABS." });
    });
});

app.post("/update-abs-sap", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  const tasks = [];

  data.forEach(({ perner, tanggal, tipe_text }) => {
    const sql = `
      UPDATE olah_absensi
      SET abs_sap = ?
      WHERE perner = ? AND tanggal = ?
    `;

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(sql, [tipe_text, perner, tanggal], (err) => {
          if (err) return reject(err);
          resolve();
        });
      })
    );
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        message: `✅ Berhasil memproses ${data.length} data ABS SAP.`,
      });
    })
    .catch((err) => {
      console.error("❌ Gagal update ABS SAP:", err);
      res.status(500).json({ message: "❌ Gagal update ABS SAP." });
    });
});

app.get("/get-lastdate", (req, res) => {
  const sql = "SELECT MAX(tanggal) AS lastdate FROM olah_absensi";

  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal mengambil lastdate:", err);
      return res.status(500).json({ message: "❌ Gagal mengambil lastdate." });
    }

    if (!result || !result[0] || !result[0].lastdate) {
      return res.status(200).json({ lastdate: null });
    }

    res.json({ lastdate: result[0].lastdate });
  });
});

app.post("/update-sppd-umum", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  const tasks = [];

  data.forEach(({ perner, tanggal, keterangan }) => {
    const sql = `
      UPDATE olah_absensi
      SET sppd_umum = ?
      WHERE perner = ? AND tanggal = ?
    `;

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(sql, [keterangan, perner, tanggal], (err) => {
          if (err) return reject(err);
          resolve();
        });
      })
    );
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        message: `✅ Berhasil mengisi ${data.length} baris SPPD Umum.`,
      });
    })
    .catch((err) => {
      console.error("❌ Gagal update SPPD Umum:", err);
      res.status(500).json({ message: "❌ Gagal update SPPD Umum." });
    });
});

app.post("/update-work-schedule", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  const tasks = [];

  data.forEach(({ perner, tanggal, ws_rule }) => {
    const sql = `
      UPDATE olah_absensi
      SET ws_rule = ?
      WHERE perner = ? AND tanggal = ?
    `;

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(sql, [ws_rule, perner, tanggal], (err) => {
          if (err) return reject(err);
          resolve();
        });
      })
    );
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        message: `✅ Berhasil memproses ${data.length} data Work Schedule.`,
      });
    })
    .catch((err) => {
      console.error("❌ Gagal update Work Schedule:", err);
      res.status(500).json({ message: "❌ Gagal update Work Schedule." });
    });
});

app.post("/update-substitution-daily", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  const tasks = [];

  data.forEach(({ perner, tanggal, jenis_shift }) => {
    const sql = `
      UPDATE olah_absensi
      SET jenis_jam_kerja_shift_daily = ?
      WHERE perner = ? AND tanggal = ?
    `;

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(sql, [jenis_shift, perner, tanggal], (err) => {
          if (err) return reject(err);
          resolve();
        });
      })
    );
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        message: `✅ Berhasil memproses ${data.length} data Substitution Daily.`,
      });
    })
    .catch((err) => {
      console.error("❌ Gagal update Substitution Daily:", err);
      res.status(500).json({ message: "❌ Gagal update Substitution Daily." });
    });
});

app.post("/update-substitution-sap", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  const tasks = [];

  data.forEach(({ perner, tanggal, jenis_shift }) => {
    const sql = `
      UPDATE olah_absensi
      SET jenis_jam_kerja_shift_sap = ?
      WHERE perner = ? AND tanggal = ?
    `;

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(sql, [jenis_shift, perner, tanggal], (err) => {
          if (err) return reject(err);
          resolve();
        });
      })
    );
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        message: `✅ Berhasil memproses ${data.length} data Substitution SAP.`,
      });
    })
    .catch((err) => {
      console.error("❌ Gagal update Substitution SAP:", err);
      res.status(500).json({ message: "❌ Gagal update Substitution SAP." });
    });
});

app.get("/get-ganda", (req, res) => {
  const sql = `

    SELECT perner, tanggal, 
       att_daily, abs_daily, att_sap, abs_sap, sppd_umum,
       att_daily_new, abs_daily_new, att_sap_new, abs_sap_new, sppd_umum_new


    FROM olah_absensi
    WHERE status_ganda_att_abs = 'Ganda'
  `;
  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal ambil data ganda:", err);
      return res.status(500).json({ message: "❌ Gagal ambil data ganda." });
    }
    res.json(result);
  });
});

app.post("/update-status-ganda", (req, res) => {
  const ambilSQL = `
    SELECT perner, tanggal,
           att_daily, abs_daily, att_sap, abs_sap, sppd_umum,
           jenis_jam_kerja_shift_daily, jenis_jam_kerja_shift_sap,
           ws_rule, jenis_hari
    FROM olah_absensi
    WHERE tanggal IS NOT NULL
  `;

  const resetSQL = `
    UPDATE olah_absensi
        SET
          status_ganda_att_abs = NULL,
          status_ganda_ws_rule = NULL,
          att_daily_new = NULL,
          abs_daily_new = NULL,
          att_sap_new = NULL,
          abs_sap_new = NULL,
          sppd_umum_new = NULL,
          value_att_abs = NULL,
          is_att_abs = NULL,
          jenis_jam_kerja_shift_daily_new = NULL,
          jenis_jam_kerja_shift_sap_new = NULL,
          value_shift_daily_sap = NULL,
          status_jam_kerja = NULL,
          is_shift_daily_sap = NULL,
          kategori_jam_kerja = NULL,
          komponen_perhitungan_jkp = NULL,
          status_absen = NULL,
          status_in_out = NULL,
          ket_in_out = NULL,
          kategori_hit_jkp = NULL,
          jam_kerja_pegawai = NULL,
          jam_kerja_pegawai_cleansing = NULL,
          jam_kerja_seharusnya = NULL
        `;

  conn.query(resetSQL, (err) => {
    if (err) {
      console.error("❌ Gagal mereset data:", err);
      return res.status(500).json({ message: "❌ Gagal mereset data." });
    }

    conn.query(ambilSQL, (err, rows) => {
      if (err) {
        console.error("❌ Gagal mengambil data:", err);
        return res.status(500).json({ message: "❌ Gagal mengambil data." });
      }

      const updateTasks = [];

      rows.forEach((row) => {
        const {
          perner,
          tanggal,
          att_daily,
          abs_daily,
          att_sap,
          abs_sap,
          sppd_umum,
          jenis_jam_kerja_shift_daily,
          jenis_jam_kerja_shift_sap,
          ws_rule,
          jenis_hari,
        } = row;

        // 1. Evaluasi status_ganda_att_abs
        const nilaiIsi = [att_daily, abs_daily, att_sap, abs_sap, sppd_umum]
          .filter((v) => v && v.trim() !== "")
          .map((v) => v.trim());

        let status = "Normal";
        if (nilaiIsi.length > 1) {
          const unik = new Set(nilaiIsi);
          if (unik.size > 1) status = "Ganda";
        }

        // 2. Evaluasi status_ganda_ws_rule
        const isiDaily = (jenis_jam_kerja_shift_daily || "").trim();
        const isiSAP = (jenis_jam_kerja_shift_sap || "").trim();

        let status_ws = "Normal";
        const keduanyaTerisi = isiDaily && isiSAP;
        const isSama = isiDaily === isiSAP;
        const adaDoubleBar = isiDaily.includes("||") || isiSAP.includes("||");

        if ((keduanyaTerisi && !isSama) || adaDoubleBar) {
          status_ws = "Ganda";
        }

        // 3. Penyalinan *_new absensi
        let att_daily_new = null;
        let abs_daily_new = null;
        let att_sap_new = null;
        let abs_sap_new = null;
        let sppd_umum_new = null;
        let value_att_abs = null;

        if (status === "Normal") {
          if (att_sap) {
            att_sap_new = att_sap;
            value_att_abs = `att_sap_new => ${att_sap}`;
          } else if (abs_sap) {
            abs_sap_new = abs_sap;
            value_att_abs = `abs_sap_new => ${abs_sap}`;
          } else if (att_daily) {
            att_daily_new = att_daily;
            value_att_abs = `att_daily_new => ${att_daily}`;
          } else if (abs_daily) {
            abs_daily_new = abs_daily;
            value_att_abs = `abs_daily_new => ${abs_daily}`;
          } else if (sppd_umum) {
            sppd_umum_new = sppd_umum;
            value_att_abs = `sppd_umum_new => ${sppd_umum}`;
          }
        }

        // 4. Penyalinan shift *_new
        let jenis_jam_kerja_shift_daily_new = null;
        let jenis_jam_kerja_shift_sap_new = null;
        let value_shift_daily_sap = null;
        let is_shift_daily_sap = "false";

        if (status_ws === "Normal") {
          if (isiSAP) {
            jenis_jam_kerja_shift_sap_new = isiSAP;
          } else if (isiDaily) {
            jenis_jam_kerja_shift_daily_new = isiDaily;
          }
        }

        if (jenis_jam_kerja_shift_sap_new) {
          value_shift_daily_sap = `shift_sap => ${jenis_jam_kerja_shift_sap_new}`;
        } else if (jenis_jam_kerja_shift_daily_new) {
          value_shift_daily_sap = `shift_daily => ${jenis_jam_kerja_shift_daily_new}`;
        }

        if (value_shift_daily_sap && value_shift_daily_sap.trim() !== "") {
          is_shift_daily_sap = "true";
        }

        const is_att_abs =
          value_att_abs !== null &&
          value_att_abs !== undefined &&
          value_att_abs.trim() !== ""
            ? "true"
            : "false";

        // 5. Parse wsin-wsout dari ws_rule
        let wsin = "-",
          wsout = "-";
        if (ws_rule && ws_rule.includes("~")) {
          const [inRaw, outRaw] = ws_rule.split("~");
          wsin = inRaw.replace(/\./g, ":");
          wsout = outRaw.replace(/\./g, ":");
        }

        // 6. Tentukan status_jam_kerja
        const shiftValue = value_shift_daily_sap?.toLowerCase() || "";
        let status_jam_kerja = "-";

        if (shiftValue.includes("pdkb")) {
          status_jam_kerja = `Normal => PDKB (${wsin}-${wsout})`;
        } else if (shiftValue.includes("piket")) {
          status_jam_kerja = `Normal => PIKET (${wsin}-${wsout})`;
        } else if (shiftValue.includes("shift2-malam")) {
          status_jam_kerja = "Shift => Malam (00:00-08:00)";
        } else if (shiftValue.includes("shift2-siang")) {
          status_jam_kerja = "Shift => Siang (16:00-24:00)";
        } else if (shiftValue.includes("shift2-pagi")) {
          status_jam_kerja = "Shift => Pagi (08:00-16:00)";
        } else if (shiftValue.includes("off")) {
          status_jam_kerja = "Shift => OFF";
        } else {
          status_jam_kerja = `Normal => (${wsin}-${wsout})`;
        }

        // 7. Tentukan kategori_jam_kerja dan komponen perhitungan jkp
        let kategori_jam_kerja = "Normal"; // default
        if (shiftValue.includes("pdkb")) {
          kategori_jam_kerja = "PDKB";
        } else if (shiftValue.includes("piket")) {
          kategori_jam_kerja = "PIKET";
        } else if (
          shiftValue.includes("shift2-malam") ||
          shiftValue.includes("shift2-siang") ||
          shiftValue.includes("shift2-pagi") ||
          shiftValue.includes("off")
        ) {
          kategori_jam_kerja = "Shift";
        }

        // Penentuan komponen_perhitungan_jkp sesuai instruksi (DIPERBAIKI)
        let komponen_perhitungan_jkp = false;

        // Cek apakah jenis_hari mengandung "kerja" (hari kerja)
        const isHariKerja =
          jenis_hari && jenis_hari.toLowerCase().includes("kerja");

        if (kategori_jam_kerja === "Shift") {
          komponen_perhitungan_jkp = true;
        } else if (
          kategori_jam_kerja &&
          kategori_jam_kerja !== "Shift" &&
          isHariKerja
        ) {
          komponen_perhitungan_jkp = true;
        } else {
          komponen_perhitungan_jkp = false;
        }

        // 8. Update ke database
        const sqlUpdate = `
            UPDATE olah_absensi
            SET
              status_ganda_att_abs = ?,
              status_ganda_ws_rule = ?,
              att_daily_new = ?,
              abs_daily_new = ?,
              att_sap_new = ?,
              abs_sap_new = ?,
              sppd_umum_new = ?,
              jenis_jam_kerja_shift_daily_new = ?,
              jenis_jam_kerja_shift_sap_new = ?,
              value_att_abs = ?,
              is_att_abs = ?,
              value_shift_daily_sap = ?,
              is_shift_daily_sap = ?,
              status_jam_kerja = ?,
              kategori_jam_kerja = ?,
              komponen_perhitungan_jkp = ?
            WHERE perner = ? AND tanggal = ?
          `;

        const params = [
          status,
          status_ws,
          att_daily_new,
          abs_daily_new,
          att_sap_new,
          abs_sap_new,
          sppd_umum_new,
          jenis_jam_kerja_shift_daily_new,
          jenis_jam_kerja_shift_sap_new,
          value_att_abs,
          is_att_abs,
          value_shift_daily_sap,
          is_shift_daily_sap,
          status_jam_kerja,
          kategori_jam_kerja,
          komponen_perhitungan_jkp,
          perner,
          tanggal,
        ];

        updateTasks.push(
          new Promise((resolve, reject) => {
            conn.query(sqlUpdate, params, (err, result) => {
              if (err) return reject(err);
              resolve(result);
            });
          })
        );
      });

      Promise.all(updateTasks)
        .then(() => {
          res.json({
            message: `✅ Status Ganda/Normal & status_jam_kerja diperbarui untuk ${rows.length} baris.`,
          });
        })
        .catch((err) => {
          console.error("❌ Gagal update status:", err);
          res.status(500).json({ message: "❌ Gagal mengupdate status." });
        });
    });
  });
});

app.post("/reset-pilihan-new", (req, res) => {
  const kolomNew = [
    "att_daily_new",
    "abs_daily_new",
    "att_sap_new",
    "abs_sap_new",
    "sppd_umum_new",
  ];

  const setClause = kolomNew.map((k) => `${k} = NULL`).join(", ");
  const sql = `UPDATE olah_absensi SET ${setClause}`;

  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal reset kolom *_new:", err);
      return res.status(500).json({ message: "❌ Gagal reset kolom *_new." });
    }

    res.json({
      message: `✅ Berhasil mereset ${result.affectedRows} baris kolom *_new.`,
    });
  });
});

app.post("/update-ganda-pilihan", (req, res) => {
  const { data } = req.body;
  if (!Array.isArray(data) || data.length === 0) {
    return res
      .status(400)
      .json({ message: "❌ Data kosong atau tidak valid." });
  }

  const tasks = data.map(({ perner, tanggal, field, nilai }) => {
    const kolomBaru = `${field}_new`;
    const valueFormatted = `${kolomBaru} => ${nilai}`;
    const is_att_abs = "true";

    const sql = `
      UPDATE olah_absensi
      SET ${kolomBaru} = ?, value_att_abs = ?, is_att_abs = ?
      WHERE perner = ? AND tanggal = ?
    `;

    return new Promise((resolve, reject) => {
      conn.query(
        sql,
        [nilai, valueFormatted, is_att_abs, perner, tanggal],
        (err) => {
          if (err) return reject(err);
          resolve();
        }
      );
    });
  });

  Promise.all(tasks)
    .then(() => {
      res.json({
        message: `✅ Berhasil menyimpan ${data.length} pilihan dan mengisi value_att_abs serta is_att_abs.`,
      });
    })
    .catch((err) => {
      console.error("❌ Gagal simpan pilihan:", err);
      res.status(500).json({ message: "❌ Gagal menyimpan data." });
    });
});

app.get("/get-ganda-shift", (req, res) => {
  const sql = `
    SELECT perner, tanggal,
           jenis_jam_kerja_shift_daily,
           jenis_jam_kerja_shift_sap,
           jenis_jam_kerja_shift_daily_new,
           jenis_jam_kerja_shift_sap_new
    FROM olah_absensi
    WHERE status_ganda_ws_rule = 'Ganda'
    ORDER BY perner, tanggal
  `;

  conn.query(sql, (err, results) => {
    if (err) {
      console.error("❌ Gagal mengambil data shift ganda:", err);
      return res
        .status(500)
        .json({ message: "❌ Gagal mengambil data shift ganda." });
    }

    res.json(results);
  });
});

app.post("/update-ganda-shift-pilihan", async (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res
      .status(400)
      .json({ message: "❌ Data kosong atau tidak valid." });
  }

  // Helper function untuk mendapatkan jenis_hari dari database
  const getJenisHari = (perner, tanggal) => {
    return new Promise((resolve, reject) => {
      const sql =
        "SELECT jenis_hari FROM olah_absensi WHERE perner = ? AND tanggal = ? LIMIT 1";
      conn.query(sql, [perner, tanggal], (err, results) => {
        if (err) return reject(err);

        if (results && results.length > 0 && results[0].jenis_hari) {
          resolve(results[0].jenis_hari);
        } else {
          resolve(null); // Jika tidak ditemukan
        }
      });
    });
  };

  try {
    // Ambil jenis_hari untuk semua data terlebih dahulu
    const jenisHariPromises = data.map((item) =>
      getJenisHari(item.perner, item.tanggal).then((jenis_hari) => ({
        ...item,
        jenis_hari,
      }))
    );

    const dataWithJenisHari = await Promise.all(jenisHariPromises);

    const tasks = dataWithJenisHari.map(
      ({ perner, tanggal, nilai, pilihan, jenis_hari }) => {
        let kolomDaily = null;
        let kolomSAP = null;
        let value_shift_daily_sap = null;
        let is_shift_daily_sap = "false";
        let status_jam_kerja = null;
        let kategori_jam_kerja = null;
        let komponen_perhitungan_jkp = false;

        // 1. Tentukan sumber nilai shift
        if (pilihan === "daily") {
          kolomDaily = nilai;
          value_shift_daily_sap = `shift_daily => ${nilai}`;
        } else if (pilihan === "sap") {
          kolomSAP = nilai;
          value_shift_daily_sap = `shift_sap => ${nilai}`;
        }

        if (value_shift_daily_sap && value_shift_daily_sap.trim() !== "") {
          is_shift_daily_sap = "true";

          // 2. Mapping status & kategori jam kerja berdasarkan value_shift_daily_sap
          const shiftVal = value_shift_daily_sap.toLowerCase();

          if (shiftVal.includes("pdkb")) {
            status_jam_kerja = "Normal => PDKB (08:00-17:00)";
            kategori_jam_kerja = "PDKB";
          } else if (shiftVal.includes("piket")) {
            status_jam_kerja = "Normal => PIKET (08:00-17:00)";
            kategori_jam_kerja = "PIKET";
          } else if (shiftVal.includes("shift2-malam")) {
            status_jam_kerja = "Shift => Malam (00:00-08:00)";
            kategori_jam_kerja = "Shift";
          } else if (shiftVal.includes("shift2-siang")) {
            status_jam_kerja = "Shift => Siang (16:00-24:00)";
            kategori_jam_kerja = "Shift";
          } else if (shiftVal.includes("shift2-pagi")) {
            status_jam_kerja = "Shift => Pagi (08:00-16:00)";
            kategori_jam_kerja = "Shift";
          } else if (shiftVal.includes("off")) {
            status_jam_kerja = "Shift => OFF";
            kategori_jam_kerja = "Shift";
          }
        }

        // 3. Penentuan komponen_perhitungan_jkp sesuai instruksi
        // Cek apakah jenis_hari mengandung "kerja" (hari kerja)
        const isHariKerja =
          jenis_hari && jenis_hari.toLowerCase().includes("kerja");

        if (kategori_jam_kerja === "Shift") {
          komponen_perhitungan_jkp = true;
        } else if (
          kategori_jam_kerja &&
          kategori_jam_kerja !== "Shift" &&
          isHariKerja
        ) {
          komponen_perhitungan_jkp = true;
        } else {
          komponen_perhitungan_jkp = false;
        }

        // console.log(
        //   `Debug - komponen_perhitungan_jkp: ${komponen_perhitungan_jkp}`
        // );
        // console.log("---");

        // 4. Siapkan query dan parameter dinamis
        const kolomSet = [
          "jenis_jam_kerja_shift_daily_new = ?",
          "jenis_jam_kerja_shift_sap_new = ?",
          "value_shift_daily_sap = ?",
          "is_shift_daily_sap = ?",
          "komponen_perhitungan_jkp = ?",
        ];
        const paramSet = [
          kolomDaily,
          kolomSAP,
          value_shift_daily_sap,
          is_shift_daily_sap,
          komponen_perhitungan_jkp,
        ];

        // Tambahkan status dan kategori jam kerja jika tersedia
        if (status_jam_kerja !== null && kategori_jam_kerja !== null) {
          kolomSet.push("status_jam_kerja = ?");
          kolomSet.push("kategori_jam_kerja = ?");
          paramSet.push(status_jam_kerja);
          paramSet.push(kategori_jam_kerja);
        }

        // WHERE clause
        const sql = `
      UPDATE olah_absensi
      SET ${kolomSet.join(", ")}
      WHERE perner = ? AND tanggal = ?
    `;
        paramSet.push(perner, tanggal);

        return new Promise((resolve, reject) => {
          conn.query(sql, paramSet, (err, result) => {
            if (err) return reject(err);
            resolve(result);
          });
        });
      }
    );

    Promise.all(tasks)
      .then(() => {
        res.json({
          message: `✅ Berhasil menyimpan ${data.length} pilihan shift ganda (termasuk value_shift_daily_sap, status_jam_kerja, dan kategori_jam_kerja).`,
        });
      })
      .catch((err) => {
        console.error("❌ Gagal update pilihan shift ganda:", err);
        res
          .status(500)
          .json({ message: "❌ Gagal menyimpan pilihan shift ganda." });
      });
  } catch (error) {
    console.error("❌ Error mengambil jenis_hari:", error);
    res
      .status(500)
      .json({ message: "❌ Gagal mengambil data jenis_hari dari database." });
  }
});

app.post("/reset-shift-ganda", (req, res) => {
  const kolomShift = [
    "jenis_jam_kerja_shift_daily_new",
    "jenis_jam_kerja_shift_sap_new",
  ];

  const setClause = kolomShift.map((k) => `${k} = NULL`).join(", ");
  const sql = `UPDATE olah_absensi SET ${setClause} WHERE status_ganda_ws_rule = 'Ganda'`;

  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal reset shift *_new:", err);
      return res.status(500).json({ message: "❌ Gagal mereset data shift." });
    }

    res.json({
      message: `✅ Berhasil mereset ${result.affectedRows} baris shift *_new.`,
    });
  });
});

app.post("/ambil-data-absensi-untuk-jkp", (req, res) => {
  const sql = `
    SELECT perner, tanggal, 
           att_daily_new, abs_daily_new, att_sap_new, abs_sap_new, sppd_umum_new,
           jenis_hari, ws_rule, is_jumat,
           daily_in, daily_out,
           jenis_jam_kerja_shift_daily_new, jenis_jam_kerja_shift_sap_new
    FROM olah_absensi
  `;
  console.log("SQL Query untuk ambil-data-absensi-untuk-jkp:", sql);

  conn.query(sql, (err, results) => {
    if (err) {
      console.error("❌ Gagal mengambil data absensi:", err);
      return res.status(500).json({ message: "❌ Gagal ambil data absensi." });
    }

    res.json(results);
  });
});

app.post("/reset-jkp", (req, res) => {
  const sql = "UPDATE olah_absensi SET jam_kerja_pegawai = NULL";

  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal reset JKP:", err);
      return res.status(500).json({ message: "❌ Gagal reset JKP." });
    }

    res.json({
      message: `✅ Kolom jam_kerja_pegawai berhasil dikosongkan (${result.affectedRows} baris).`,
    });
  });
});

app.post("/update-jkp", (req, res) => {
  const { data } = req.body;
  if (!Array.isArray(data)) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  const sql = `
    UPDATE olah_absensi
    SET jam_kerja_pegawai = ?
    WHERE perner = ? AND tanggal = ?
  `;

  let updated = 0;
  const tasks = data.map((item) => {
    return new Promise((resolve) => {
      conn.query(sql, [item.jkp, item.perner, item.tanggal], (err) => {
        if (!err) updated++;
        resolve();
      });
    });
  });

  Promise.all(tasks).then(() => {
    res.json({ message: `✅ JKP berhasil disimpan ke ${updated} baris.` });
  });
});

// const { hitungJKPShift } = require("./fungsiJKP");

app.post("/proses-kalkulasi-jkp-backend", async (req, res) => {
  try {
    // Kirim parameter filter
    const ambil = await fetch(`${BASE_URL}/ambil-data-absensi-untuk-jkp`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filterValue: "shift_daily => Shift2-Malam~00.00~08.00", // <<=== Kriteria yang ingin kamu proses
      }),
    });

    if (!ambil.ok) {
      throw new Error(`HTTP error! status: ${ambil.status}`);
    }

    const data = await ambil.json();

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(400).json({
        message: "❌ Data tidak valid atau kosong",
      });
    }

    const hasilUpdate = [];
    const logJKP = [];
    const logSQLDebug = [];

    data.forEach((row, index) => {
      try {
        if (!row.perner || !row.tanggal) {
          console.warn(`⚠️ Data tidak lengkap pada index ${index}:`, row);
          return;
        }

        const hasilJKP = hitungJKPFinal(row);
        const hasilJKPShift = hitungJKPShift(row);

        if (!hasilJKP) {
          console.warn(
            `⚠️ hitungJKPFinal mengembalikan null pada index ${index}`
          );
          return;
        }

        const rawDurasi = Number(hasilJKP.durasi ?? 0);
        const rawDurasiCleansing = Number(
          hasilJKPShift.durasi_cleansing_c ??
            hasilJKPShift.durasi_cleansing ??
            hasilJKP.durasi ??
            0
        );

        const daily_in_raw = hasilJKPShift.daily_in_cleansing_c ?? null;
        const daily_out_raw =
          hasilJKP.daily_out_cleansing_c ??
          hasilJKP.daily_out_cleansing ??
          hasilJKP.jpr ??
          row.daily_out ??
          null;

        const hasil = {
          perner: row.perner,
          tanggal: formatTanggal(row.tanggal),
          jkp: !isNaN(rawDurasi) ? parseFloat(rawDurasi.toFixed(3)) : 0,
          daily_in_cleansing:
            daily_in_raw && daily_in_raw !== "-"
              ? daily_in_raw.replace(/\./g, ":")
              : null,
          daily_out_cleansing:
            daily_out_raw && daily_out_raw !== "-"
              ? daily_out_raw.replace(/\./g, ":")
              : null,
          durasi_cleansing: !isNaN(rawDurasiCleansing)
            ? parseFloat(rawDurasiCleansing.toFixed(3))
            : 0,
        };

        hasilUpdate.push(hasil);

        logJKP.push(
          row.__logJKP !== undefined
            ? row.__logJKP
            : `❌ Log tidak tersedia untuk ${row.perner} - ${formatTanggal(
                row.tanggal
              )}`
        );
      } catch (error) {
        console.error(`❌ Error processing row ${index}:`, error);
        logJKP.push(`❌ Error processing ${row.perner}: ${error.message}`);
      }
    });

    if (hasilUpdate.length === 0) {
      return res.status(400).json({
        message: "❌ Tidak ada data valid untuk diproses",
      });
    }

    const sql = `
      UPDATE olah_absensi
      SET jam_kerja_pegawai = ?, 
          daily_in_cleansing = ?, 
          daily_out_cleansing = ?, 
          jam_kerja_pegawai_cleansing = ?
      WHERE perner = ? AND tanggal = ?
    `;

    const tasks = hasilUpdate.map((item, index) => {
      const {
        perner,
        tanggal,
        jkp,
        daily_in_cleansing,
        daily_out_cleansing,
        durasi_cleansing,
      } = item;

      const values = [
        jkp,
        daily_in_cleansing,
        daily_out_cleansing,
        durasi_cleansing,
        perner,
        tanggal,
      ];

      const logBaris = `
📤 [${index + 1}] Update SQL:
Perner: ${perner}
Tanggal: ${tanggal}
JKP: ${jkp}
In Cleansing: ${daily_in_cleansing}
Out Cleansing: ${daily_out_cleansing}
Durasi Cleansing: ${durasi_cleansing}
Values: ${JSON.stringify(values)}
      `.trim();

      console.log(logBaris);
      logSQLDebug.push(logBaris);

      return new Promise((resolve, reject) => {
        const cekQuery = `
          SELECT perner, tanggal FROM olah_absensi WHERE perner = ? AND tanggal = ?
        `;

        conn.query(cekQuery, [perner, tanggal], (cekErr, cekRows) => {
          if (cekErr) {
            console.error(
              `❌ ERROR saat cek data: ${perner} - ${tanggal}`,
              cekErr.message
            );
            return reject(new Error(`Cek data gagal: ${cekErr.message}`));
          }

          if (cekRows.length === 0) {
            logSQLDebug.push(
              `⚠️ SKIP UPDATE: Data tidak ditemukan untuk ${perner} - ${tanggal}`
            );
            return resolve({
              perner,
              tanggal,
              affectedRows: 0,
              status: "skip - not found",
            });
          }

          conn.query(sql, values, (err, result) => {
            if (err) {
              console.error(`❌ SQL ERROR for ${perner}:`, err.message);
              return reject(
                new Error(`SQL Error for ${perner}: ${err.message}`)
              );
            }

            logSQLDebug.push(
              result.affectedRows > 0
                ? `✅ UPDATE OK [${result.affectedRows}] → ${perner} - ${tanggal}`
                : `⚠️ UPDATE GAGAL: Tidak ada baris berubah untuk ${perner} - ${tanggal}`
            );

            resolve({
              perner,
              tanggal,
              affectedRows: result.affectedRows,
              status: result.affectedRows > 0 ? "updated" : "not-updated",
            });
          });
        });
      });
    });

    const results = await Promise.allSettled(tasks);
    const successes = results.filter((r) => r.status === "fulfilled").length;
    const failures = results.filter((r) => r.status === "rejected");

    try {
      const headerLog = [
        `📊 Total Data Processed: ${data.length}`,
        `✅ Successful Updates: ${successes}`,
        `❌ Failed Updates: ${failures.length}`,
        `📄 Log Entries: ${logJKP.length}`,
        `🕒 Processed at: ${new Date().toISOString()}`,
        `==============================`,
        ``,
      ];

      const isiLogJKP = headerLog.concat(logJKP).join("\n");
      const logJKPPath = path.join(__dirname, "log_jkp.txt");
      fs.writeFileSync(logJKPPath, isiLogJKP, "utf8");

      const logSQLPath = path.join(__dirname, "log_sql_debug.txt");
      fs.writeFileSync(logSQLPath, logSQLDebug.join("\n\n"), "utf8");

      console.log(`📝 Logs saved: ${logJKPPath}, ${logSQLPath}`);
    } catch (logError) {
      console.error("⚠️ Failed to save logs:", logError);
    }

    res.json({
      message: `✅ Processing completed. ${successes}/${hasilUpdate.length} records updated successfully.`,
      summary: {
        total_data: data.length,
        processed: hasilUpdate.length,
        successful_updates: successes,
        failed_updates: failures.length,
      },
      log_file: "log_jkp.txt",
      sql_log_file: "log_sql_debug.txt",
    });
  } catch (err) {
    console.error("❌ Fatal error in JKP calculation:", err);
    res.status(500).json({
      message: "❌ Fatal error during JKP calculation process.",
      error: err.message,
    });
  }
});

app.post("/proses-kalkulasi-jkp-backend-selective", async (req, res) => {
  try {
    const { filterValue, targetRows } = req.body;

    const hardcodedTargetRows = [
      // { perner: "92143410", tanggal: "2025-02-28" }
    ];

    const selectedTargetRows =
      targetRows ||
      (hardcodedTargetRows.length > 0 ? hardcodedTargetRows : null);

    const isProcessAllData =
      !selectedTargetRows || selectedTargetRows.length === 0;

    console.log(
      `🔍 Processing mode: ${isProcessAllData ? "ALL_DATA" : "SELECTIVE"}`
    );
    if (!isProcessAllData) {
      console.log(`🎯 Target rows: ${selectedTargetRows.length}`);
    }

    const ambil = await fetch(`${BASE_URL}/ambil-data-absensi-untuk-jkp`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        filterValue: filterValue || "shift_daily => Shift2-Malam~00.00~08.00",
      }),
    });

    if (!ambil.ok) {
      throw new Error(`HTTP error! status: ${ambil.status}`);
    }

    const allData = await ambil.json();

    if (!Array.isArray(allData) || allData.length === 0) {
      return res.status(400).json({
        message: "❌ Data tidak valid atau kosong",
      });
    }

    let filteredData = [];
    let foundTargets = [];
    let missingTargets = [];

    if (isProcessAllData) {
      filteredData = allData;
      foundTargets = allData.map((row) => ({
        perner: row.perner,
        tanggal: formatTanggalSafe(row.tanggal),
        originalTarget: "ALL_DATA",
        foundRowDate: row.tanggal,
        status: "found",
      }));
    } else {
      if (selectedTargetRows && Array.isArray(selectedTargetRows)) {
        selectedTargetRows.forEach((target) => {
          if (!target || !target.perner || !target.tanggal) return;

          const targetDate = formatTanggalSafe(target.tanggal);
          const foundRow = allData.find((row) => {
            if (!row || !row.perner || !row.tanggal) return false;

            const rowDate = formatTanggalSafe(row.tanggal);
            const penerMatch = row.perner === target.perner;

            let dateMatch = isSameDate(targetDate, rowDate);

            if (
              !dateMatch &&
              row.tanggal &&
              typeof row.tanggal === "string" &&
              row.tanggal.includes("T")
            ) {
              const utcDate = new Date(row.tanggal);
              const localDate = new Date(
                utcDate.getTime() + 7 * 60 * 60 * 1000
              );
              const localDateStr = formatTanggalSafe(localDate);
              if (targetDate === localDateStr) {
                dateMatch = true;
              }
            }

            return penerMatch && dateMatch;
          });

          if (foundRow) {
            filteredData.push(foundRow);
            foundTargets.push({
              perner: target.perner,
              tanggal: targetDate,
              originalTarget: target.tanggal,
              foundRowDate: foundRow.tanggal,
              status: "found",
            });
          } else {
            missingTargets.push({
              perner: target.perner,
              tanggal: targetDate,
              originalTarget: target.tanggal,
              status: "not_found",
            });
          }
        });
      }
    }

    if (filteredData.length === 0) {
      return res.status(400).json({
        message: isProcessAllData
          ? "❌ Tidak ada data dalam database untuk diproses"
          : "❌ Tidak ada target rows yang ditemukan dalam dataset",
        targeting: isProcessAllData
          ? { mode: "ALL_DATA", total_data: allData.length, filtered_data: 0 }
          : {
              total_targets: selectedTargetRows ? selectedTargetRows.length : 0,
              found: foundTargets,
              missing: missingTargets,
            },
      });
    }

    const hasilUpdate = [];
    const logJKP = [];

    filteredData.forEach((row) => {
      try {
        if (!row.perner || !row.tanggal) return;

        const hasilJKP = hitungJKPFinal(row);
        if (!hasilJKP) return;

        const hasilJKPShift = hitungJKPShift(row);

        const rawDurasi = Number(hasilJKP.durasi ?? 0);

        // Pilih sumber ket_in_out sesuai kategori
        const ket_in_out_final =
          hasilJKP.ket === "JKP Shift"
            ? hasilJKPShift?.ket_in_out ?? hasilJKP?.ket_in_out ?? null
            : hasilJKP?.ket_in_out ?? hasilJKPShift?.ket_in_out ?? null;

        // ✨ FITUR BARU: Ekstrak durasi_seharusnya
        const durasi_seharusnya_final =
          hasilJKP.ket === "JKP Shift"
            ? hasilJKPShift?.durasi_seharusnya ??
              hasilJKP?.durasi_seharusnya ??
              null
            : hasilJKP?.durasi_seharusnya ??
              hasilJKPShift?.durasi_seharusnya ??
              null;

        let rawDurasiCleansing;
        let daily_in_raw;
        let daily_out_raw;

        if (hasilJKP.ket === "JKP Shift") {
          rawDurasiCleansing = Number(
            hasilJKPShift.durasi_cleansing_c ??
              hasilJKPShift.durasi_cleansing ??
              hasilJKP.durasi_cleansing_c ??
              hasilJKP.durasi ??
              0
          );
          daily_in_raw =
            hasilJKPShift.daily_in_cleansing_c ??
            hasilJKP.daily_in_cleansing_c ??
            null;
          daily_out_raw =
            hasilJKPShift.daily_out_cleansing_c ??
            hasilJKP.daily_out_cleansing_c ??
            hasilJKP.jpr ??
            row.daily_out ??
            null;
        } else {
          rawDurasiCleansing = Number(
            hasilJKP.durasi_cleansing_c ??
              hasilJKP.durasi_cleansing ??
              hasilJKP.durasi ??
              0
          );
          daily_in_raw =
            hasilJKP.daily_in_cleansing_c ??
            hasilJKP.daily_in_cleansing ??
            hasilJKP.jmr ??
            row.daily_in ??
            null;
          daily_out_raw =
            hasilJKP.daily_out_cleansing_c ??
            hasilJKP.daily_out_cleansing ??
            hasilJKP.jpr ??
            row.daily_out ??
            null;
        }

        const finalTanggal = formatTanggalSafe(row.tanggal);

        // ✨ FITUR BARU: Perhitungan field-field untuk database
        const jam_kerja_pegawai_cleansing = !isNaN(rawDurasiCleansing)
          ? parseFloat(rawDurasiCleansing.toFixed(3))
          : 0;

        const jam_kerja_seharusnya =
          durasi_seharusnya_final !== null &&
          !isNaN(Number(durasi_seharusnya_final))
            ? parseFloat(Number(durasi_seharusnya_final).toFixed(3))
            : null;

        // ✨ FITUR BARU: Hitung persentase pemenuhan jam kerja
        let persentase = null;
        if (jam_kerja_seharusnya !== null && jam_kerja_seharusnya > 0) {
          const persentase_raw =
            (jam_kerja_pegawai_cleansing / jam_kerja_seharusnya) * 100;
          persentase = parseFloat(persentase_raw.toFixed(2));
        }

        const hasil = {
          perner: row.perner,
          tanggal: finalTanggal,
          jkp: !isNaN(rawDurasi) ? parseFloat(rawDurasi.toFixed(3)) : 0,
          daily_in_cleansing:
            daily_in_raw && daily_in_raw !== "-"
              ? daily_in_raw.replace(/\./g, ":")
              : null,
          daily_out_cleansing:
            daily_out_raw && daily_out_raw !== "-"
              ? daily_out_raw.replace(/\./g, ":")
              : null,
          durasi_cleansing: jam_kerja_pegawai_cleansing,
          kategori_hit_jkp: hasilJKP.ket || null,
          ket_in_out: ket_in_out_final || null,
          jam_kerja_seharusnya: jam_kerja_seharusnya,
          persentase: persentase,
        };

        hasilUpdate.push(hasil);

        logJKP.push(
          row.__logJKP !== undefined
            ? row.__logJKP
            : `❌ Log tidak tersedia untuk ${row.perner} - ${formatTanggalSafe(
                row.tanggal
              )}`
        );
      } catch (error) {
        logJKP.push(
          `❌ Error processing target ${row.perner}: ${error.message}`
        );
      }
    });

    if (hasilUpdate.length === 0) {
      return res.status(400).json({
        message: isProcessAllData
          ? "❌ Tidak ada data yang berhasil diproses dari database"
          : "❌ Tidak ada target rows yang berhasil diproses",
      });
    }

    // ✨ FITUR BARU: Update SQL query dengan jam_kerja_seharusnya dan persentase
    const sql = `
      UPDATE olah_absensi
      SET jam_kerja_pegawai = ?, 
          daily_in_cleansing = ?, 
          daily_out_cleansing = ?, 
          jam_kerja_pegawai_cleansing = ?,
          kategori_hit_jkp = ?,
          ket_in_out = ?,
          jam_kerja_seharusnya = ?,
          persentase = ?
      WHERE perner = ? AND (
        DATE(tanggal) = ? OR 
        DATE(CONVERT_TZ(tanggal, '+00:00', '+07:00')) = ? OR
        tanggal = ?
      )
    `;

    const tasks = hasilUpdate.map((item) => {
      const {
        perner,
        tanggal,
        jkp,
        daily_in_cleansing,
        daily_out_cleansing,
        durasi_cleansing,
        kategori_hit_jkp,
        ket_in_out,
        jam_kerja_seharusnya,
        persentase,
      } = item;

      const values = [
        jkp,
        daily_in_cleansing,
        daily_out_cleansing,
        durasi_cleansing,
        kategori_hit_jkp,
        ket_in_out,
        jam_kerja_seharusnya,
        persentase,
        perner,
        tanggal,
        tanggal,
        tanggal,
      ];

      return new Promise((resolve, reject) => {
        const cekQuery = `
          SELECT perner, tanggal, jam_kerja_pegawai, daily_in_cleansing,
                 daily_out_cleansing, jam_kerja_pegawai_cleansing,
                 kategori_hit_jkp, ket_in_out, jam_kerja_seharusnya, persentase
          FROM olah_absensi
          WHERE perner = ? AND (
            DATE(tanggal) = ? OR 
            DATE(CONVERT_TZ(tanggal, '+00:00', '+07:00')) = ? OR
            tanggal = ?
          )
        `;

        conn.query(
          cekQuery,
          [perner, tanggal, tanggal, tanggal],
          (cekErr, cekRows) => {
            if (cekErr) {
              return reject(new Error(`Cek data gagal: ${cekErr.message}`));
            }
            if (cekRows.length === 0) {
              return resolve({
                perner,
                tanggal,
                affectedRows: 0,
                status: "skip - not found",
              });
            }
            conn.query(sql, values, (err, result) => {
              if (err) {
                return reject(
                  new Error(`SQL Error for target ${perner}: ${err.message}`)
                );
              }
              resolve({
                perner,
                tanggal,
                affectedRows: result.affectedRows,
                status: result.affectedRows > 0 ? "updated" : "not-updated",
                beforeUpdate: cekRows[0],
                updateValues: values,
              });
            });
          }
        );
      });
    });

    await Promise.allSettled(tasks);

    // ✨ FITUR BARU: Enhanced response message dengan statistik persentase
    const updatedRowsCount = hasilUpdate.length;
    const jamKerjaSeharusnyaCount = hasilUpdate.filter(
      (item) => item.jam_kerja_seharusnya !== null
    ).length;
    const persentaseCount = hasilUpdate.filter(
      (item) => item.persentase !== null
    ).length;

    // Hitung statistik persentase
    const persentaseData = hasilUpdate.filter(
      (item) => item.persentase !== null
    );
    let avgPersentase = null;
    let minPersentase = null;
    let maxPersentase = null;

    if (persentaseData.length > 0) {
      const persentaseValues = persentaseData.map((item) => item.persentase);
      avgPersentase = parseFloat(
        (
          persentaseValues.reduce((a, b) => a + b, 0) / persentaseValues.length
        ).toFixed(2)
      );
      minPersentase = Math.min(...persentaseValues);
      maxPersentase = Math.max(...persentaseValues);
    }

    res.json({
      message: `✅ Processing completed. ${updatedRowsCount} records processed. ${persentaseCount} records with persentase data (avg: ${avgPersentase}%).`,
      details: {
        total_processed: updatedRowsCount,
        with_jam_kerja_seharusnya: jamKerjaSeharusnyaCount,
        with_persentase: persentaseCount,
        without_persentase: updatedRowsCount - persentaseCount,
        persentase_stats: {
          average: avgPersentase,
          minimum: minPersentase,
          maximum: maxPersentase,
          count: persentaseCount,
        },
      },
    });
  } catch (err) {
    console.error("❌ Fatal error in selective JKP calculation:", err);
    res.status(500).json({
      message: "❌ Fatal error during selective JKP calculation process.",
      error: err.message,
    });
  }
});

// ===================================================================
// 🗓️ UNIVERSAL DATE UTILITIES - STANDARD UNTUK SEMUA FUNGSI JKP
// ===================================================================

/**
 * 🎯 MASTER FUNCTION: Format tanggal ke YYYY-MM-DD (timezone safe)
 */
function formatTanggalSafe(input) {
  if (!input) return null;

  let dateObj;

  // Handle berbagai format input
  if (typeof input === "string") {
    // Jika sudah format YYYY-MM-DD, return langsung
    if (input.match(/^\d{4}-\d{2}-\d{2}$/)) {
      return input;
    }

    // Jika format lain (ISO, etc), parse dengan timezone handling
    if (input.includes("T")) {
      // ISO format: 2025-02-14T16:00:00.000Z
      dateObj = new Date(input);
    } else {
      // Simple date string: "2025-02-14"
      // PENTING: Tambah T00:00:00 untuk avoid timezone shift
      dateObj = new Date(input + "T00:00:00");
    }
  } else if (input instanceof Date) {
    dateObj = input;
  } else {
    return null;
  }

  // Validasi date object
  if (isNaN(dateObj.getTime())) {
    return null;
  }

  // TIMEZONE SAFE: Gunakan getFullYear/getMonth/getDate (local time)
  const year = dateObj.getFullYear();
  const month = String(dateObj.getMonth() + 1).padStart(2, "0");
  const day = String(dateObj.getDate()).padStart(2, "0");

  const result = `${year}-${month}-${day}`;
  return result;
}

/**
 * 🎯 COMPARISON FUNCTION: Bandingkan dua tanggal (timezone safe)
 */
function isSameDate(date1, date2) {
  const formatted1 = formatTanggalSafe(date1);
  const formatted2 = formatTanggalSafe(date2);
  return formatted1 === formatted2;
}

// Legacy compatibility
// 🔧 HELPER FUNCTION: Format tanggal untuk konsistensi matching
// 🔧 HELPER FUNCTION: Format tanggal untuk konsistensi matching
const {
  hitungJKPNormal,
  hitungJKPShift,
  hitungJKPFinal,
} = require("./fungsiJKP"); // pastikan path-nya sesuai

app.post("/update-status-absen-cleansing", (req, res) => {
  const ambilSQL = `
    SELECT perner, tanggal, daily_in_cleansing, daily_out_cleansing
    FROM olah_absensi
    WHERE tanggal IS NOT NULL
  `;

  conn.query(ambilSQL, (err, rows) => {
    if (err) {
      console.error("❌ Gagal mengambil data:", err);
      return res
        .status(500)
        .json({ message: "❌ Gagal mengambil data absensi." });
    }

    const updateTasks = [];

    rows.forEach((row) => {
      const { perner, tanggal, daily_in_cleansing, daily_out_cleansing } = row;

      // 1. Evaluasi status_absen
      let status_absen = null;

      const adaIn = daily_in_cleansing !== null;
      const adaOut = daily_out_cleansing !== null;

      if (adaIn && adaOut) {
        status_absen = "Lengkap";
      } else if (!adaIn && adaOut) {
        status_absen = "Tidak lengkap -> in kosong";
      } else if (adaIn && !adaOut) {
        status_absen = "Tidak lengkap -> out kosong";
      } else {
        status_absen = "Tidak lengkap -> tidak absen";
      }

      // 2. Query update per baris
      const sqlUpdate = `
        UPDATE olah_absensi
        SET status_absen = ?
        WHERE perner = ? AND tanggal = ?
      `;

      const params = [status_absen, perner, tanggal];

      updateTasks.push(
        new Promise((resolve, reject) => {
          conn.query(sqlUpdate, params, (err) => {
            if (err) return reject(err);
            resolve();
          });
        })
      );
    });

    Promise.all(updateTasks)
      .then(() => {
        res.json({
          message: `✅ Berhasil mengupdate status_absen untuk ${rows.length} baris.`,
        });
      })
      .catch((err) => {
        console.error("❌ Gagal update status_absen:", err);
        res.status(500).json({ message: "❌ Gagal menyimpan status_absen." });
      });
  });
});

// Helper function: Konversi waktu ke detik
function timeToSeconds(timeString) {
  if (!timeString || timeString === "-" || timeString === null) {
    return null;
  }

  // Handle format HH:MM:SS atau HH:MM
  const parts = timeString.split(":");
  if (parts.length < 2) return null;

  const hours = parseInt(parts[0]) || 0;
  const minutes = parseInt(parts[1]) || 0;
  const seconds = parseInt(parts[2]) || 0;

  return hours * 3600 + minutes * 60 + seconds;
}

// Helper function: Ekstrak jadwal dari status_jam_kerja
function extractSchedule(statusJamKerja) {
  if (!statusJamKerja) {
    return { jms: null, jps: null };
  }

  // Cari pola dalam kurung: (HH:MM-HH:MM)
  const match = statusJamKerja.match(/\((\d{2}:\d{2})-(\d{2}:\d{2})\)/);
  if (match) {
    return {
      jms: match[1] + ":00", // Tambah detik
      jps: match[2] + ":00",
    };
  }

  return { jms: null, jps: null };
}

// Helper function: Tentukan keterangan berdasarkan perbandingan
function getKeterangan(realisasi_seconds, seharusnya_seconds, type) {
  if (realisasi_seconds === null || seharusnya_seconds === null) {
    return "-";
  }

  if (type === "masuk") {
    if (realisasi_seconds > seharusnya_seconds) {
      return "Terlambat masuk";
    } else {
      return "Waktu masuk sesuai";
    }
  } else if (type === "pulang") {
    if (realisasi_seconds < seharusnya_seconds) {
      return "Cepat pulang";
    } else {
      return "Waktu pulang sesuai";
    }
  }

  return "-";
}

app.post("/update-status-in-out", (req, res) => {
  const ambilSQL = `
    SELECT perner, tanggal, 
           daily_in_cleansing, daily_out_cleansing,
           status_jam_kerja, status_absen
    FROM olah_absensi
    WHERE tanggal IS NOT NULL
  `;

  conn.query(ambilSQL, (err, rows) => {
    if (err) {
      console.error("❌ Gagal mengambil data:", err);
      return res
        .status(500)
        .json({ message: "❌ Gagal mengambil data absensi." });
    }

    const updateTasks = [];

    rows.forEach((row) => {
      const {
        perner,
        tanggal,
        daily_in_cleansing,
        daily_out_cleansing,
        status_jam_kerja,
        status_absen,
      } = row;

      let status_in_out = null;

      // 1. Cek kondisi khusus: status_absen != "Lengkap"
      if (status_absen !== "Lengkap") {
        status_in_out = status_absen;
      }
      // 2. Cek kondisi khusus: status_jam_kerja mengandung "OFF"
      else if (status_jam_kerja && status_jam_kerja.includes("OFF")) {
        status_in_out = status_absen;
      }
      // 3. Proses normal: bandingkan waktu
      else {
        // Ekstrak jadwal dari status_jam_kerja
        const { jms, jps } = extractSchedule(status_jam_kerja);

        if (jms && jps) {
          // Definisi variabel
          const jmr = daily_in_cleansing; // jam masuk realisasi
          const jpr = daily_out_cleansing; // jam pulang realisasi

          // Konversi ke detik
          const jmr_seconds = timeToSeconds(jmr);
          const jpr_seconds = timeToSeconds(jpr);
          const jms_seconds = timeToSeconds(jms);
          const jps_seconds = timeToSeconds(jps);

          // Tentukan keterangan
          const ket_in = getKeterangan(jmr_seconds, jms_seconds, "masuk");
          const ket_out = getKeterangan(jpr_seconds, jps_seconds, "pulang");

          // Format output: ket_in + " dan " + ket_out
          if (ket_in !== "-" && ket_out !== "-") {
            status_in_out = `${ket_in} dan ${ket_out}`;
          } else {
            status_in_out = status_absen; // fallback
          }
        } else {
          status_in_out = status_absen; // fallback jika tidak bisa ekstrak jadwal
        }
      }

      // Query update
      const sqlUpdate = `
        UPDATE olah_absensi
        SET status_in_out = ?
        WHERE perner = ? AND tanggal = ?
      `;

      const params = [status_in_out, perner, tanggal];

      updateTasks.push(
        new Promise((resolve, reject) => {
          conn.query(sqlUpdate, params, (err) => {
            if (err) return reject(err);
            resolve();
          });
        })
      );
    });

    Promise.all(updateTasks)
      .then(() => {
        res.json({
          message: `✅ Berhasil mengupdate status_in_out untuk ${rows.length} baris.`,
        });
      })
      .catch((err) => {
        console.error("❌ Gagal update status_in_out:", err);
        res.status(500).json({ message: "❌ Gagal menyimpan status_in_out." });
      });
  });
});

// Endpoint untuk reset status_in_out
app.post("/reset-status-in-out", (req, res) => {
  const sql = "UPDATE olah_absensi SET status_in_out = NULL";

  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal reset status_in_out:", err);
      return res.status(500).json({ message: "❌ Gagal reset status_in_out." });
    }

    res.json({
      message: `✅ Kolom status_in_out berhasil dikosongkan (${result.affectedRows} baris).`,
    });
  });
});

// ===================================================================
// 🔄 ENDPOINT 1: Reset Rekap Absensi
// ===================================================================

app.post("/reset-rekap-absensi", (req, res) => {
  console.log("🔄 Starting reset rekap absensi...");

  const sql = "DELETE FROM rekap_absensi";

  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal reset rekap absensi:", err);
      return res.status(500).json({
        message: "❌ Gagal mereset tabel rekap absensi.",
        error: err.message,
      });
    }

    console.log(`✅ Reset berhasil: ${result.affectedRows} baris dihapus`);
    res.json({
      message: `✅ Berhasil mereset tabel rekap_absensi. ${result.affectedRows} baris dihapus.`,
      details: {
        deleted_rows: result.affectedRows,
        reset_at: new Date().toISOString(),
      },
    });
  });
});

// ===================================================================
// 📊 ENDPOINT 2: View Rekap Stats
// ===================================================================

app.post("/view-rekap-stats", (req, res) => {
  console.log("📊 Getting rekap absensi statistics...");

  const sql = `
    SELECT 
      COUNT(*) as total_pegawai,
      ROUND(AVG(PERSENTASE_JKP), 2) as avg_persentase,
      ROUND(SUM(JUMLAH_JAM_KERJA_REALISASI), 2) as total_jam_realisasi,
      ROUND(SUM(JUMLAH_JAM_KERJA_SEHARUSNYA), 2) as total_jam_seharusnya,
      SUM(JUMLAH_HARI_KERJA) as total_hari_kerja,
      SUM(JUMLAH_HARI_LIBUR) as total_hari_libur,
      SUM(JUMLAH_STATUS_ABSENSI_LENGKAP) as total_absensi_lengkap,
      SUM(JUMLAH_KOREKSI_IN + JUMLAH_KOREKSI_OUT + JUMLAH_KOREKSI_IN_DAN_OUT) as total_koreksi
    FROM rekap_absensi
  `;

  conn.query(sql, (err, result) => {
    if (err) {
      console.error("❌ Gagal mengambil statistik rekap:", err);
      return res.status(500).json({
        message: "❌ Gagal mengambil statistik rekap absensi.",
        error: err.message,
      });
    }

    const stats = result[0];
    console.log(
      `📊 Statistik berhasil diambil: ${stats.total_pegawai} pegawai`
    );

    res.json({
      message: `📊 Statistik rekap absensi berhasil diambil untuk ${stats.total_pegawai} pegawai.`,
      stats: {
        total_pegawai: stats.total_pegawai,
        avg_persentase: stats.avg_persentase,
        total_jam_realisasi: stats.total_jam_realisasi,
        total_jam_seharusnya: stats.total_jam_seharusnya,
        total_hari_kerja: stats.total_hari_kerja,
        total_hari_libur: stats.total_hari_libur,
        total_absensi_lengkap: stats.total_absensi_lengkap,
        total_koreksi: stats.total_koreksi,
      },
      retrieved_at: new Date().toISOString(),
    });
  });
});

// ===================================================================
// 📈 ENDPOINT 3: Generate Rekap Absensi (COMPLEX)
// ===================================================================

app.post("/proses-status-in-out", async (req, res) => {
  try {
    const { filterValue, targetRows, verbose } = req.body || {};

    // ===== DEBUG: hardcoded target rows (opsional) =====
    const hardcodedTargets = [{ perner: "95155200", tanggal: "2025-02-03" }];

    // Prioritas target: request.targetRows > hardcodedTargets > null (ALL)
    const selectedTargets =
      Array.isArray(targetRows) && targetRows.length > 0
        ? targetRows
        : hardcodedTargets.length > 0
        ? hardcodedTargets
        : null;

    const processingMode = selectedTargets ? "SELECTIVE" : "ALL";
    const VERBOSE = verbose === true || verbose === "true";

    // ================== Utils ==================
    const normalizeTime = (t) => {
      if (t === undefined || t === null) return null;
      const s = String(t).trim().replace(/\./g, ":");
      if (!s) return null;
      const m = s.match(/^(\d{1,2})(?::(\d{1,2}))?(?::(\d{1,2}))?$/);
      if (!m) return null;
      const hh = String(m[1]).padStart(2, "0");
      const mm = String(m[2] ?? "00").padStart(2, "0");
      const ss = String(m[3] ?? "00").padStart(2, "0");
      return `${hh}:${mm}:${ss}`;
    };

    const toSeconds = (hms) => {
      if (!hms) return null;
      const [h, m, s] = hms.split(":").map(Number);
      if (h === 24 && (m || s)) return null; // 24:xx bukan format valid
      const hh = h === 24 ? 24 : h;
      return hh * 3600 + m * 60 + s;
    };

    const isShiftOff = (statusJam) =>
      !!statusJam && String(statusJam).toUpperCase().includes("OFF");

    const parseJadwal = (statusJam) => {
      if (!statusJam) return { jms: null, jps: null };
      const m = String(statusJam).match(/\(([^)]+)\)/); // "(08:00-17:00)"
      if (!m) return { jms: null, jps: null };
      const range = m[1].replace(/\s+/g, ""); // "08:00-17:00"
      const [startRaw, endRaw] = range.split("-");
      const jms = normalizeTime(startRaw);
      const jps = normalizeTime(endRaw);
      return { jms, jps };
    };

    const formatTanggalSafeLocal = (v) => {
      const d = new Date(v);
      if (Number.isNaN(d.getTime())) return null;
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const day = String(d.getDate()).padStart(2, "0");
      return `${y}-${m}-${day}`;
    };

    const sameDate = (a, b) => String(a) === String(b);

    // Fungsi utama untuk menghitung status_in_out
    const hitungStatusInOutDetail = ({
      status_absen,
      status_jam_kerja,
      jmr,
      jpr,
    }) => {
      const result = {
        input: {
          status_absen: status_absen ?? null,
          status_jam_kerja: status_jam_kerja ?? null,
          jmr_raw: jmr ?? null,
          jpr_raw: jpr ?? null,
        },
        parsed: { jms: null, jps: null, jmr: null, jpr: null },
        decision: {
          ket_in: null,
          ket_out: null,
          status_in_out: null,
          reason: null,
        },
      };

      // 1) Hanya proses jika status_absen = "Lengkap"
      if (!status_absen || status_absen !== "Lengkap") {
        result.decision.status_in_out = status_absen || "Tidak lengkap";
        result.decision.reason = "status_absen_bukan_Lengkap";
        return result;
      }

      // 2) Jika shift OFF, tidak perlu dibandingkan
      if (isShiftOff(status_jam_kerja)) {
        result.decision.status_in_out = status_absen; // tetap "Lengkap"
        result.decision.reason = "shift_OFF";
        return result;
      }

      // 3) Ambil jms/jps dari kurung dalam status_jam_kerja
      const { jms, jps } = parseJadwal(status_jam_kerja);
      if (!jms || !jps) {
        result.decision.status_in_out = status_absen; // tetap "Lengkap"
        result.decision.reason = "jadwal_tidak_terbaca";
        return result;
      }

      // 4) Normalize realisasi dari cleansing
      const jmrN = normalizeTime(jmr);
      const jprN = normalizeTime(jpr);
      result.parsed.jms = jms;
      result.parsed.jps = jps;
      result.parsed.jmr = jmrN;
      result.parsed.jpr = jprN;

      // Jika salah satu realisasi kosong, tidak bisa dibandingkan
      if (!jmrN || !jprN) {
        result.decision.status_in_out = status_absen; // tetap "Lengkap"
        result.decision.reason = "realisasi_kosong";
        return result;
      }

      // 5) Convert ke detik untuk perbandingan
      const jmrS = toSeconds(jmrN);
      const jprS = toSeconds(jprN);
      const jmsS = toSeconds(jms);
      const jpsS = toSeconds(jps);

      if (jmrS == null || jprS == null || jmsS == null || jpsS == null) {
        result.decision.status_in_out = status_absen; // tetap "Lengkap"
        result.decision.reason = "konversi_detik_gagal";
        return result;
      }

      // 6) Logika perbandingan berdasarkan tabel yang Anda berikan
      let ket_in, ket_out;

      // Untuk jam masuk: jmr vs jms
      if (jmrS > jmsS) {
        ket_in = "Terlambat masuk";
      } else {
        // jmr <= jms (datang tepat waktu atau lebih awal dianggap sesuai)
        ket_in = "Waktu masuk sesuai";
      }

      // Untuk jam pulang: jpr vs jps
      if (jprS < jpsS) {
        ket_out = "Cepat pulang";
      } else {
        // jpr >= jps (pulang tepat waktu atau lembur dianggap sesuai)
        ket_out = "Waktu pulang sesuai";
      }

      result.decision.ket_in = ket_in;
      result.decision.ket_out = ket_out;
      result.decision.status_in_out = `${ket_in} dan ${ket_out}`;
      result.decision.reason = "OK";
      return result;
    };

    // =================================================================================

    console.log("🔧 [/proses-status-in-out] Starting...");
    console.log("🔎 Mode:", processingMode);
    if (processingMode === "SELECTIVE") {
      console.log("🎯 Selected targets:", selectedTargets);
    }
    if (filterValue) {
      console.log("🧪 filterValue diteruskan ke sumber data:", filterValue);
    }

    // Ambil data sumber
    const ambil = await fetch(`${BASE_URL}/ambil-data-absensi-untuk-jkp`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filterValue: filterValue || null }),
    });

    if (!ambil.ok) throw new Error(`HTTP error! status: ${ambil.status}`);

    const allData = await ambil.json();

    console.log("🔍 Raw response from ambil-data-absensi-untuk-jkp:");
    console.log("- Type:", typeof allData);
    console.log("- Is Array:", Array.isArray(allData));
    console.log("- Length:", allData?.length);

    if (!Array.isArray(allData) || allData.length === 0) {
      console.log("❌ Data tidak valid atau kosong");
      return res
        .status(400)
        .json({ message: "❌ Data tidak valid atau kosong" });
    }
    console.log(`📦 Data diterima dari sumber: ${allData.length} baris`);

    // DEBUG: Log data spesifik yang dicari
    if (processingMode === "SELECTIVE") {
      const targets = selectedTargets.map((t) => ({
        perner: t.perner,
        tanggal: formatTanggalSafeLocal(t.tanggal),
      }));

      console.log("🔍 Mencari data untuk targets:", targets);

      targets.forEach((target) => {
        const matchingRows = allData.filter(
          (row) =>
            row.perner === target.perner &&
            sameDate(formatTanggalSafeLocal(row.tanggal), target.tanggal)
        );

        console.log(
          `🎯 Data untuk ${target.perner} ${target.tanggal}:`,
          matchingRows.length,
          "rows"
        );

        if (matchingRows.length > 0) {
          matchingRows.forEach((row, i) => {
            console.log(`  Row ${i + 1} RAW DATA:`, {
              perner: row.perner,
              tanggal: row.tanggal,
              tanggal_formatted: formatTanggalSafeLocal(row.tanggal),
              status_absen: row.status_absen,
              status_absen_type: typeof row.status_absen,
              status_jam_kerja: row.status_jam_kerja,
              daily_in_cleansing: row.daily_in_cleansing,
              daily_out_cleansing: row.daily_out_cleansing,
              all_fields: Object.keys(row),
            });
          });
        } else {
          console.log(
            `  ❌ Tidak ditemukan data untuk ${target.perner} ${target.tanggal}`
          );

          // Cek apakah ada data dengan perner yang sama tapi tanggal berbeda
          const samePernerRows = allData.filter(
            (row) => row.perner === target.perner
          );
          console.log(
            `  📅 Data dengan perner sama (${target.perner}):`,
            samePernerRows.length,
            "rows"
          );
          if (samePernerRows.length > 0) {
            console.log(
              "  📅 Sample tanggal yang ada:",
              samePernerRows.slice(0, 3).map((r) => ({
                tanggal: r.tanggal,
                formatted: formatTanggalSafeLocal(r.tanggal),
              }))
            );
          }
        }
      });
    }

    // DEBUG: Tampilkan sample data untuk debugging
    if (allData.length > 0) {
      console.log("🔍 Sample data structure:", {
        first_row_keys: Object.keys(allData[0]),
        first_row_values: {
          perner: allData[0].perner,
          tanggal: allData[0].tanggal,
          status_absen: allData[0].status_absen,
          status_jam_kerja: allData[0].status_jam_kerja,
          daily_in_cleansing: allData[0].daily_in_cleansing,
          daily_out_cleansing: allData[0].daily_out_cleansing,
        },
      });

      // Cari data dengan status_absen = "Lengkap" untuk sample
      const lengkapData = allData
        .filter((row) => row.status_absen === "Lengkap")
        .slice(0, 2);
      if (lengkapData.length > 0) {
        console.log("📊 Sample data dengan status_absen='Lengkap':");
        lengkapData.forEach((row, i) => {
          console.log(`  Sample ${i + 1}:`, {
            perner: row.perner,
            tanggal: formatTanggalSafeLocal(row.tanggal),
            status_absen: row.status_absen,
            status_jam_kerja: row.status_jam_kerja,
            daily_in_cleansing: row.daily_in_cleansing,
            daily_out_cleansing: row.daily_out_cleansing,
          });
        });
      } else {
        console.log(
          "⚠️ Tidak ada data dengan status_absen='Lengkap' dalam dataset"
        );
      }
    }

    // Pilih dataset: ALL vs SELECTIVE
    let dataset = [];
    let foundTargets = [];
    let missingTargets = [];

    console.log("🔍 Processing mode:", processingMode);
    console.log("🔍 Selected targets:", selectedTargets);

    if (processingMode === "SELECTIVE") {
      const targets = selectedTargets.map((t) => ({
        perner: t.perner,
        tanggal: formatTanggalSafeLocal(t.tanggal),
      }));

      console.log("🎯 Formatted targets:", targets);

      dataset = allData.filter((row) => {
        if (!row?.perner || !row?.tanggal) {
          console.log("⚠️ Row missing perner or tanggal:", {
            perner: row?.perner,
            tanggal: row?.tanggal,
          });
          return false;
        }
        const rowDate = formatTanggalSafeLocal(row.tanggal);
        const match = targets.some(
          (t) => t.perner === row.perner && sameDate(t.tanggal, rowDate)
        );

        if (match) {
          console.log("✅ Found matching row:", {
            perner: row.perner,
            tanggal: row.tanggal,
            rowDate: rowDate,
            status_absen: row.status_absen,
            status_jam_kerja: row.status_jam_kerja,
          });
        }

        return match;
      });

      console.log("📊 Filtered dataset length:", dataset.length);

      // Found/missing untuk debug
      targets.forEach((t) => {
        const hit = dataset.find(
          (r) =>
            r.perner === t.perner &&
            sameDate(formatTanggalSafeLocal(r.tanggal), t.tanggal)
        );
        if (hit) {
          foundTargets.push({
            perner: t.perner,
            tanggal: t.tanggal,
            status: "found",
            raw_data: hit, // Tambahkan data mentah untuk debug
          });
        } else {
          missingTargets.push({
            perner: t.perner,
            tanggal: t.tanggal,
            status: "not_found",
          });
        }
      });
    } else {
      dataset = allData;
    }

    if (dataset.length === 0) {
      return res.status(400).json({
        message: "❌ Tidak ada baris yang cocok untuk diproses",
        debug: {
          mode: processingMode,
          total_in_source: allData.length,
          selectedTargets,
          foundTargets,
          missingTargets,
        },
      });
    }

    // Ringkasan ketersediaan field penting
    const counters = {
      status_absen_missing: 0,
      status_jam_kerja_missing: 0,
      in_cleansing_missing: 0,
      out_cleansing_missing: 0,
    };
    dataset.forEach((r) => {
      if (r.status_absen == null) counters.status_absen_missing++;
      if (r.status_jam_kerja == null) counters.status_jam_kerja_missing++;
      if (r.daily_in_cleansing == null) counters.in_cleansing_missing++;
      if (r.daily_out_cleansing == null) counters.out_cleansing_missing++;
    });
    console.log("🧾 Field availability (on selected dataset):", counters);

    // SQL Update
    const sql = `
      UPDATE olah_absensi
      SET status_in_out = ?
      WHERE perner = ? AND (
        DATE(tanggal) = ? OR
        DATE(CONVERT_TZ(tanggal, '+00:00', '+07:00')) = ? OR
        tanggal = ?
      )
    `;

    const tasks = dataset.map((row) => {
      const tanggal = formatTanggalSafeLocal(row.tanggal);

      // Realisasi dari cleansing (titik -> :)
      const jmr = row.daily_in_cleansing
        ? String(row.daily_in_cleansing).replace(/\./g, ":")
        : null;
      const jpr = row.daily_out_cleansing
        ? String(row.daily_out_cleansing).replace(/\./g, ":")
        : null;

      // Perhitungan menggunakan fungsi yang sudah diperbaiki
      const detail = hitungStatusInOutDetail({
        status_absen: row.status_absen,
        status_jam_kerja: row.status_jam_kerja,
        jmr,
        jpr,
      });

      const status_in_out = detail.decision.status_in_out;
      const params = [status_in_out, row.perner, tanggal, tanggal, tanggal];

      return new Promise((resolve, reject) => {
        // Ambil nilai sebelum update untuk debug
        const cekQuery = `
          SELECT perner, tanggal, status_in_out
          FROM olah_absensi
          WHERE perner = ? AND (
            DATE(tanggal) = ? OR
            DATE(CONVERT_TZ(tanggal, '+00:00', '+07:00')) = ? OR
            tanggal = ?
          )
          LIMIT 1
        `;

        conn.query(
          cekQuery,
          [row.perner, tanggal, tanggal, tanggal],
          (cekErr, cekRows) => {
            if (cekErr)
              return reject(
                new Error(
                  `Cek data gagal untuk ${row.perner} ${tanggal}: ${cekErr.message}`
                )
              );

            conn.query(sql, params, (err, result) => {
              if (err)
                return reject(
                  new Error(
                    `SQL error for ${row.perner} ${tanggal}: ${err.message}`
                  )
                );

              // ======== DEBUG TO TERMINAL (per-baris) ========
              try {
                const before = cekRows?.[0]?.status_in_out ?? null;
                const lines = [
                  "📌 [DEBUG] Updating row:",
                  `    perner         : ${row.perner}`,
                  `    tanggal        : ${tanggal}`,
                  `    status_absen   : ${row.status_absen ?? "-"}`,
                  `    status_jadwal  : ${row.status_jam_kerja ?? "-"}`,
                  `    jmr/jpr (real) : ${detail.parsed.jmr ?? "-"} / ${
                    detail.parsed.jpr ?? "-"
                  }`,
                  `    jms/jps (jadw) : ${detail.parsed.jms ?? "-"} / ${
                    detail.parsed.jps ?? "-"
                  }`,
                  `    ket_in         : ${detail.decision.ket_in ?? "-"}`,
                  `    ket_out        : ${detail.decision.ket_out ?? "-"}`,
                  `    status_in_out  : ${status_in_out}`,
                  `    reason         : ${detail.decision.reason}`,
                  `    before update  : ${before}`,
                  `    affectedRows   : ${result.affectedRows}`,
                ];

                // Log detail jika SELECTIVE mode atau VERBOSE
                if (processingMode === "SELECTIVE" || VERBOSE) {
                  console.log(lines.join("\n"));
                }
              } catch (e) {
                console.warn("⚠️ Gagal menulis debug detail:", e.message);
              }

              resolve({
                perner: row.perner,
                tanggal,
                inputs: {
                  status_absen: row.status_absen ?? null,
                  status_jam_kerja: row.status_jam_kerja ?? null,
                  jmr: detail.parsed.jmr,
                  jpr: detail.parsed.jpr,
                  jms: detail.parsed.jms,
                  jps: detail.parsed.jps,
                },
                decisions: {
                  ket_in: detail.decision.ket_in,
                  ket_out: detail.decision.ket_out,
                  status_in_out: status_in_out,
                  reason: detail.decision.reason,
                },
                beforeUpdate: cekRows?.[0] ?? null,
                affectedRows: result.affectedRows,
              });
            });
          }
        );
      });
    });

    const results = await Promise.allSettled(tasks);
    const successes = results.filter((r) => r.status === "fulfilled").length;
    const failures = results.length - successes;

    // Kumpulkan detail untuk response
    const details = results
      .slice(0, 20)
      .map((r) =>
        r.status === "fulfilled" ? r.value : { error: r.reason.message }
      );

    console.log(
      `✅ Selesai: processed=${results.length}, updated=${successes}, failed=${failures}`
    );

    res.json({
      message: `✅ proses-status-in-out selesai. Updated: ${successes}, Failed: ${failures}`,
      mode: processingMode,
      filterValue: filterValue || null,
      debug: {
        total_in_source: allData.length,
        processed_rows: results.length,
        field_availability: counters,
        selectedTargets,
        foundTargets,
        missingTargets,
        details,
      },
    });
  } catch (err) {
    console.error("❌ Fatal error in /proses-status-in-out:", err);
    res.status(500).json({
      message: "❌ Fatal error during proses-status-in-out.",
      error: err.message,
    });
  }
});

// ============================================
// ENDPOINT: Generate Rekap Absensi
// ============================================
app.post("/generate-rekap-absensi", (req, res) => {
  console.log("📈 Starting generate rekap absensi...");
  const startTime = Date.now();

  // Step 1: Clear existing data (optional)
  const clearSQL = "DELETE FROM rekap_absensi";
  conn.query(clearSQL, (clearErr) => {
    if (clearErr) {
      console.warn("⚠️ Warning clearing rekap_absensi:", clearErr.message);
    }

    // Step 2: Get unique PERNERs
    const getPernersSQL =
      "SELECT DISTINCT perner FROM olah_absensi ORDER BY perner";

    conn.query(getPernersSQL, (err, perners) => {
      if (err) {
        console.error("❌ Gagal mengambil daftar perner:", err);
        return res.status(500).json({
          message: "❌ Gagal mengambil daftar pegawai.",
          error: err.message,
        });
      }

      console.log(`📋 Ditemukan ${perners.length} pegawai untuk diproses`);

      // Step 3: Process each PERNER
      const tasks = [];
      let processedCount = 0;
      let successCount = 0;
      let errorCount = 0;

      perners.forEach((pernerRow) => {
        const perner = pernerRow.perner;

        tasks.push(
          new Promise((resolve, reject) => {
            // Complex SQL untuk calculate semua field sekaligus
            const rekapSQL = `
              SELECT 
                '${perner}' as PERNER,
                
                -- Statistik Hari
                COUNT(*) as TOTAL_HARI,
                COUNT(CASE WHEN jenis_hari LIKE '%HARI KERJA%' THEN 1 END) as HARI_KERJA,
                COUNT(CASE WHEN jenis_hari LIKE '%LIBUR%' THEN 1 END) as HARI_LIBUR,
                
                -- Statistik Koreksi
                COUNT(CASE WHEN (correction_in = 'koreksi' OR correction_out = 'koreksi') THEN 1 END) as TOTAL_HARI_KOREKSI,
                COUNT(CASE WHEN correction_in = 'koreksi' AND (correction_out != 'koreksi' OR correction_out IS NULL) THEN 1 END) as KOREKSI_IN,
                COUNT(CASE WHEN correction_out = 'koreksi' AND (correction_in != 'koreksi' OR correction_in IS NULL) THEN 1 END) as KOREKSI_OUT,
                COUNT(CASE WHEN correction_in = 'koreksi' AND correction_out = 'koreksi' THEN 1 END) as KOREKSI_IN_OUT,
                
                -- Jam Kerja Normal
                COUNT(CASE WHEN status_jam_kerja LIKE '%Normal%' THEN 1 END) as TOTAL_JAM_KERJA_NORMAL,
                COUNT(CASE WHEN status_jam_kerja LIKE '%PIKET%' THEN 1 END) as PIKET,
                COUNT(CASE WHEN status_jam_kerja LIKE '%PDKB%' THEN 1 END) as PDKB,
                COUNT(CASE WHEN status_jam_kerja LIKE '%Normal%' AND status_jam_kerja NOT LIKE '%PIKET%' AND status_jam_kerja NOT LIKE '%PDKB%' THEN 1 END) as REGULER,
                
                -- Jam Kerja Shift
                COUNT(CASE WHEN status_jam_kerja LIKE '%Shift%' THEN 1 END) as TOTAL_JAM_KERJA_SHIFT,
                COUNT(CASE WHEN status_jam_kerja LIKE '%Pagi%' THEN 1 END) as SHIFT_PAGI,
                COUNT(CASE WHEN status_jam_kerja LIKE '%Siang%' THEN 1 END) as SHIFT_SIANG,
                COUNT(CASE WHEN status_jam_kerja LIKE '%Malam%' THEN 1 END) as SHIFT_MALAM,
                COUNT(CASE WHEN status_jam_kerja LIKE '%OFF%' THEN 1 END) as SHIFT_OFF,
                
                -- Status Absensi
                COUNT(CASE WHEN status_absen = 'Lengkap' THEN 1 END) as ABSEN_LENGKAP,
                COUNT(CASE WHEN status_absen LIKE '%tidak absen%' THEN 1 END) as TIDAK_ABSEN,
                COUNT(CASE WHEN status_absen LIKE '%in kosong%' THEN 1 END) as IN_KOSONG,
                COUNT(CASE WHEN status_absen LIKE '%out kosong%' THEN 1 END) as OUT_KOSONG,
                
                -- Pengajuan Ketidakhadiran (Parse value_att_abs)
                COUNT(CASE WHEN SUBSTRING_INDEX(value_att_abs, '_', 1) IN ('att', 'sppd') THEN 1 END) as SPPD_TUGAS_LUAR_DLL,
                COUNT(CASE WHEN SUBSTRING_INDEX(value_att_abs, '_', 1) = 'abs' THEN 1 END) as CUTI_IJIN,
                
                -- Jam Kerja Calculations
                ROUND(COALESCE(SUM(CAST(jam_kerja_pegawai_cleansing AS DECIMAL(10,2))), 0), 2) as JAM_REALISASI,
                ROUND(COALESCE(SUM(CAST(jam_kerja_seharusnya AS DECIMAL(10,2))), 0), 2) as JAM_SEHARUSNYA,
                
                -- Persentase JKP
                CASE 
                  WHEN COALESCE(SUM(CAST(jam_kerja_seharusnya AS DECIMAL(10,2))), 0) = 0 THEN 0.00
                  ELSE ROUND(
                    (COALESCE(SUM(CAST(jam_kerja_pegawai_cleansing AS DECIMAL(10,2))), 0) / 
                     SUM(CAST(jam_kerja_seharusnya AS DECIMAL(10,2)))) * 100, 
                    2
                  )
                END as PERSENTASE_JKP
                
              FROM olah_absensi 
              WHERE perner = ?
            `;

            conn.query(rekapSQL, [perner], (calcErr, calcResult) => {
              if (calcErr) {
                console.error(`❌ Error calculating for ${perner}:`, calcErr);
                errorCount++;
                return reject(calcErr);
              }

              if (calcResult.length === 0) {
                console.warn(`⚠️ No data found for ${perner}`);
                errorCount++;
                return resolve();
              }

              const rekapData = calcResult[0];

              // Insert into rekap_absensi
              const insertSQL = `
                INSERT INTO rekap_absensi (
                  PERNER, TOTAL_HARI, HARI_KERJA, HARI_LIBUR, 
                  TOTAL_HARI_KOREKSI, KOREKSI_IN, KOREKSI_OUT, KOREKSI_IN_OUT,
                  TOTAL_JAM_KERJA_NORMAL, PIKET, PDKB, REGULER,
                  TOTAL_JAM_KERJA_SHIFT, SHIFT_PAGI, SHIFT_SIANG, SHIFT_MALAM, SHIFT_OFF,
                  ABSEN_LENGKAP, TIDAK_ABSEN, IN_KOSONG, OUT_KOSONG,
                  SPPD_TUGAS_LUAR_DLL, CUTI_IJIN,
                  JAM_REALISASI, JAM_SEHARUSNYA, PERSENTASE_JKP
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                  TOTAL_HARI = VALUES(TOTAL_HARI),
                  HARI_KERJA = VALUES(HARI_KERJA),
                  HARI_LIBUR = VALUES(HARI_LIBUR),
                  TOTAL_HARI_KOREKSI = VALUES(TOTAL_HARI_KOREKSI),
                  KOREKSI_IN = VALUES(KOREKSI_IN),
                  KOREKSI_OUT = VALUES(KOREKSI_OUT),
                  KOREKSI_IN_OUT = VALUES(KOREKSI_IN_OUT),
                  TOTAL_JAM_KERJA_NORMAL = VALUES(TOTAL_JAM_KERJA_NORMAL),
                  PIKET = VALUES(PIKET),
                  PDKB = VALUES(PDKB),
                  REGULER = VALUES(REGULER),
                  TOTAL_JAM_KERJA_SHIFT = VALUES(TOTAL_JAM_KERJA_SHIFT),
                  SHIFT_PAGI = VALUES(SHIFT_PAGI),
                  SHIFT_SIANG = VALUES(SHIFT_SIANG),
                  SHIFT_MALAM = VALUES(SHIFT_MALAM),
                  SHIFT_OFF = VALUES(SHIFT_OFF),
                  ABSEN_LENGKAP = VALUES(ABSEN_LENGKAP),
                  TIDAK_ABSEN = VALUES(TIDAK_ABSEN),
                  IN_KOSONG = VALUES(IN_KOSONG),
                  OUT_KOSONG = VALUES(OUT_KOSONG),
                  SPPD_TUGAS_LUAR_DLL = VALUES(SPPD_TUGAS_LUAR_DLL),
                  CUTI_IJIN = VALUES(CUTI_IJIN),
                  JAM_REALISASI = VALUES(JAM_REALISASI),
                  JAM_SEHARUSNYA = VALUES(JAM_SEHARUSNYA),
                  PERSENTASE_JKP = VALUES(PERSENTASE_JKP)
              `;

              const insertValues = [
                rekapData.PERNER,
                rekapData.TOTAL_HARI || 0,
                rekapData.HARI_KERJA || 0,
                rekapData.HARI_LIBUR || 0,
                rekapData.TOTAL_HARI_KOREKSI || 0,
                rekapData.KOREKSI_IN || 0,
                rekapData.KOREKSI_OUT || 0,
                rekapData.KOREKSI_IN_OUT || 0,
                rekapData.TOTAL_JAM_KERJA_NORMAL || 0,
                rekapData.PIKET || 0,
                rekapData.PDKB || 0,
                rekapData.REGULER || 0,
                rekapData.TOTAL_JAM_KERJA_SHIFT || 0,
                rekapData.SHIFT_PAGI || 0,
                rekapData.SHIFT_SIANG || 0,
                rekapData.SHIFT_MALAM || 0,
                rekapData.SHIFT_OFF || 0,
                rekapData.ABSEN_LENGKAP || 0,
                rekapData.TIDAK_ABSEN || 0,
                rekapData.IN_KOSONG || 0,
                rekapData.OUT_KOSONG || 0,
                rekapData.SPPD_TUGAS_LUAR_DLL || 0,
                rekapData.CUTI_IJIN || 0,
                rekapData.JAM_REALISASI || 0.0,
                rekapData.JAM_SEHARUSNYA || 0.0,
                rekapData.PERSENTASE_JKP || 0.0,
              ];

              conn.query(insertSQL, insertValues, (insertErr, insertResult) => {
                if (insertErr) {
                  console.error(`❌ Error inserting ${perner}:`, insertErr);
                  errorCount++;
                  return reject(insertErr);
                }

                processedCount++;
                successCount++;

                // Log progress setiap 10 pegawai
                if (
                  processedCount % 10 === 0 ||
                  processedCount === perners.length
                ) {
                  console.log(
                    `📊 Progress: ${processedCount}/${perners.length} pegawai diproses`
                  );
                }

                resolve(insertResult);
              });
            });
          })
        );
      });

      // Execute all tasks
      Promise.allSettled(tasks)
        .then((results) => {
          const endTime = Date.now();
          const duration = ((endTime - startTime) / 1000).toFixed(2);

          console.log(`✅ Generate rekap selesai dalam ${duration}s`);
          console.log(
            `📊 Summary: ${successCount} sukses, ${errorCount} error dari ${perners.length} pegawai`
          );

          // Get final statistics
          const finalStatsSQL = `
            SELECT 
              COUNT(*) as total_pegawai,
              ROUND(AVG(PERSENTASE_JKP), 2) as avg_persentase,
              ROUND(SUM(JAM_REALISASI), 2) as total_jam_realisasi,
              ROUND(SUM(JAM_SEHARUSNYA), 2) as total_jam_seharusnya,
              ROUND(AVG(TOTAL_HARI), 2) as avg_hari_total,
              MIN(TOTAL_HARI) as min_hari_total,
              MAX(TOTAL_HARI) as max_hari_total,
              COUNT(CASE WHEN TOTAL_HARI < 20 THEN 1 END) as pegawai_data_kurang
            FROM rekap_absensi
          `;

          conn.query(finalStatsSQL, (statsErr, statsResult) => {
            const stats = statsErr ? null : statsResult[0];

            res.json({
              success: true,
              message: `✅ Generate rekap absensi selesai dalam ${duration}s. ${successCount}/${perners.length} pegawai berhasil diproses.`,
              details: {
                total_pegawai: perners.length,
                successful: successCount,
                failed: errorCount,
                processing_time: `${duration}s`,
                generated_at: new Date().toISOString(),
              },
              stats: stats || null,
            });
          });
        })
        .catch((err) => {
          console.error("❌ Fatal error during generate rekap:", err);
          res.status(500).json({
            success: false,
            message: "❌ Fatal error saat generate rekap absensi.",
            error: err.message,
          });
        });
    });
  });
});

// ============================================
// ENDPOINT: Get Rekap Absensi Data
// ============================================
app.get("/get-rekap-absensi", (req, res) => {
  const { limit = 50, offset = 0, search = "" } = req.query;

  let whereClause = "";
  const params = [];

  if (search) {
    whereClause = "WHERE PERNER LIKE ?";
    params.push(`%${search}%`);
  }

  const countSQL = `SELECT COUNT(*) as total FROM rekap_absensi ${whereClause}`;
  const dataSQL = `
    SELECT * FROM rekap_absensi 
    ${whereClause}
    ORDER BY PERNER 
    LIMIT ? OFFSET ?
  `;

  // Get total count
  conn.query(countSQL, params, (countErr, countResult) => {
    if (countErr) {
      return res.status(500).json({
        success: false,
        message: "Error getting count",
        error: countErr.message,
      });
    }

    const total = countResult[0].total;

    // Get data
    conn.query(
      dataSQL,
      [...params, parseInt(limit), parseInt(offset)],
      (dataErr, dataResult) => {
        if (dataErr) {
          return res.status(500).json({
            success: false,
            message: "Error getting data",
            error: dataErr.message,
          });
        }

        res.json({
          success: true,
          data: dataResult,
          pagination: {
            total: total,
            limit: parseInt(limit),
            offset: parseInt(offset),
            totalPages: Math.ceil(total / limit),
          },
        });
      }
    );
  });
});

app.listen(PORT, () => {
  console.log(`✅ Server listening on ${PORT}`);
  console.log(`🔗 Health: http://127.0.0.1:${PORT}/api/health`);
});

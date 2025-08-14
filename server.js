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

// DB Pool - MySQL2 Compatible Configuration
const conn = mysql.createPool({
  host: process.env.DB_HOST || "127.0.0.1",
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASS || "",
  database: process.env.DB_NAME || "absensi_db",
  waitForConnections: true,
  connectionLimit: 20, // ✅ Maximum connections in pool
  queueLimit: 0, // ✅ Unlimited queue
  maxIdle: 10, // ✅ Maximum idle connections
  idleTimeout: 60000, // ✅ Close idle connections after 60s
  enableKeepAlive: true, // ✅ Keep connections alive
  keepAliveInitialDelay: 0, // ✅ No delay for keep-alive
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

  if (liburData.length === 0) {
    return res.status(400).json({ message: "❌ Data hari libur kosong" });
  }

  console.log(
    `🔄 Processing ${liburData.length} hari libur records with BATCH method...`
  );
  const startTime = Date.now();

  const tasks = [];

  // STEP 1: Batch update hari libur
  if (liburData.length > 0) {
    const batchLiburSQL = `
      UPDATE olah_absensi 
      SET jenis_hari = CASE 
        ${liburData.map(() => `WHEN tanggal = ? THEN ?`).join(" ")}
        ELSE jenis_hari
      END
      WHERE tanggal IN (${liburData.map(() => "?").join(", ")})
    `;

    const liburParams = [];
    // Build parameters for CASE statement
    liburData.forEach((item) => {
      liburParams.push(item.tanggal, item.jenis_hari);
    });
    // Build parameters for WHERE clause
    liburData.forEach((item) => {
      liburParams.push(item.tanggal);
    });

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(batchLiburSQL, liburParams, (err, result) => {
          if (err) return reject(err);
          console.log(`✅ Batch LIBUR: ${result.affectedRows} rows updated`);
          resolve({ type: "libur", affectedRows: result.affectedRows });
        });
      })
    );
  }

  // STEP 2: Update tanggal lain jadi "HARI KERJA"
  // Hanya update yang belum ada jenis_hari atau kosong
  const updateHariKerjaSQL = `
    UPDATE olah_absensi
    SET jenis_hari = 'HARI KERJA'
    WHERE (jenis_hari IS NULL OR jenis_hari = '' OR jenis_hari = 'HARI KERJA')
      AND tanggal IS NOT NULL
      AND tanggal NOT IN (${liburData.map(() => "?").join(", ")})
  `;

  const hariKerjaParams = liburData.map((item) => item.tanggal);

  tasks.push(
    new Promise((resolve, reject) => {
      conn.query(updateHariKerjaSQL, hariKerjaParams, (err, result) => {
        if (err) return reject(err);
        console.log(`✅ Batch HARI KERJA: ${result.affectedRows} rows updated`);
        resolve({ type: "hari_kerja", affectedRows: result.affectedRows });
      });
    })
  );

  Promise.all(tasks)
    .then((results) => {
      const endTime = Date.now();
      const duration = endTime - startTime;

      const liburResult = results.find((r) => r.type === "libur");
      const hariKerjaResult = results.find((r) => r.type === "hari_kerja");

      const totalAffected =
        (liburResult?.affectedRows || 0) + (hariKerjaResult?.affectedRows || 0);

      console.log(`⚡ Hari Libur Batch completed in ${duration}ms`);
      console.log(`   - Hari libur: ${liburResult?.affectedRows || 0} rows`);
      console.log(
        `   - Hari kerja: ${hariKerjaResult?.affectedRows || 0} rows`
      );

      res.json({
        message: `✅ Hari libur dan hari kerja berhasil diproses dengan batch method (${duration}ms).`,
        performance: {
          duration_ms: duration,
          libur_records: liburData.length,
          libur_affected: liburResult?.affectedRows || 0,
          hari_kerja_affected: hariKerjaResult?.affectedRows || 0,
          total_affected: totalAffected,
          method: "batch",
        },
      });
    })
    .catch((err) => {
      console.error("❌ Error hari libur batch:", err);
      res.status(500).json({ message: "❌ Gagal memproses hari libur." });
    });
});

app.post("/update-ci-co", (req, res) => {
  const { data } = req.body;
  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  console.log(
    `🔄 Processing ${data.length} CI/CO records with BATCH method...`
  );
  const startTime = Date.now();

  // Group by operation type for batch processing
  const inUpdates = [];
  const outUpdates = [];

  data.forEach(({ perner, tanggal, waktu, tipe, correction }) => {
    const updateData = { perner, tanggal, waktu, correction };

    if (tipe.toUpperCase() === "CLOCK_IN") {
      inUpdates.push(updateData);
    } else if (tipe.toUpperCase() === "CLOCK_OUT") {
      outUpdates.push(updateData);
    }
  });

  const tasks = [];

  // Batch update for CLOCK_IN
  if (inUpdates.length > 0) {
    const batchInSQL = `
      UPDATE olah_absensi 
      SET daily_in = CASE 
        ${inUpdates
          .map(() => `WHEN perner = ? AND tanggal = ? THEN ?`)
          .join(" ")}
      END,
      correction_in = CASE 
        ${inUpdates
          .map(() => `WHEN perner = ? AND tanggal = ? THEN ?`)
          .join(" ")}
      END
      WHERE (perner, tanggal) IN (${inUpdates.map(() => "(?, ?)").join(", ")})
    `;

    const inParams = [];
    // Build parameters for daily_in CASE
    inUpdates.forEach((item) => {
      inParams.push(item.perner, item.tanggal, item.waktu);
    });
    // Build parameters for correction_in CASE
    inUpdates.forEach((item) => {
      inParams.push(item.perner, item.tanggal, item.correction);
    });
    // Build parameters for WHERE clause
    inUpdates.forEach((item) => {
      inParams.push(item.perner, item.tanggal);
    });

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(batchInSQL, inParams, (err, result) => {
          if (err) return reject(err);
          console.log(`✅ Batch IN: ${result.affectedRows} rows updated`);
          resolve(result);
        });
      })
    );
  }

  // Batch update for CLOCK_OUT
  if (outUpdates.length > 0) {
    const batchOutSQL = `
      UPDATE olah_absensi 
      SET daily_out = CASE 
        ${outUpdates
          .map(() => `WHEN perner = ? AND tanggal = ? THEN ?`)
          .join(" ")}
      END,
      correction_out = CASE 
        ${outUpdates
          .map(() => `WHEN perner = ? AND tanggal = ? THEN ?`)
          .join(" ")}
      END
      WHERE (perner, tanggal) IN (${outUpdates.map(() => "(?, ?)").join(", ")})
    `;

    const outParams = [];
    // Build parameters for daily_out CASE
    outUpdates.forEach((item) => {
      outParams.push(item.perner, item.tanggal, item.waktu);
    });
    // Build parameters for correction_out CASE
    outUpdates.forEach((item) => {
      outParams.push(item.perner, item.tanggal, item.correction);
    });
    // Build parameters for WHERE clause
    outUpdates.forEach((item) => {
      outParams.push(item.perner, item.tanggal);
    });

    tasks.push(
      new Promise((resolve, reject) => {
        conn.query(batchOutSQL, outParams, (err, result) => {
          if (err) return reject(err);
          console.log(`✅ Batch OUT: ${result.affectedRows} rows updated`);
          resolve(result);
        });
      })
    );
  }

  Promise.all(tasks)
    .then((results) => {
      const endTime = Date.now();
      const duration = endTime - startTime;
      const totalAffected = results.reduce((sum, r) => sum + r.affectedRows, 0);

      console.log(
        `⚡ CI/CO Batch completed in ${duration}ms, ${totalAffected} rows affected`
      );

      res.json({
        message: `✅ Berhasil memproses ${data.length} data CI/CO dengan batch method (${duration}ms).`,
        performance: {
          duration_ms: duration,
          total_records: data.length,
          affected_rows: totalAffected,
          method: "batch",
        },
      });
    })
    .catch((err) => {
      console.error("❌ Gagal batch update CI/CO:", err);
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

  console.log(
    `🔄 Processing ${data.length} ATT/ABS records with ENHANCED BATCH method...`
  );
  const startTime = Date.now();

  // Separate data by category for optimized batch operations
  const attUpdates = [];
  const absUpdates = [];

  data.forEach(({ perner, tanggal, tipe_text, kategori }) => {
    const updateData = { perner, tanggal, tipe_text };

    if (kategori === "att") {
      attUpdates.push(updateData);
    } else if (kategori === "abs") {
      absUpdates.push(updateData);
    }
  });

  console.log(
    `📊 Data distribution: ATT=${attUpdates.length}, ABS=${absUpdates.length}`
  );

  const tasks = [];

  // BATCH 1: Update ATT (attendance) records
  if (attUpdates.length > 0) {
    // Chunk large datasets to avoid MySQL query limits
    const attChunks = [];
    const chunkSize = 500; // Process 500 records per batch

    for (let i = 0; i < attUpdates.length; i += chunkSize) {
      attChunks.push(attUpdates.slice(i, i + chunkSize));
    }

    attChunks.forEach((chunk, chunkIndex) => {
      const batchAttSQL = `
        UPDATE olah_absensi 
        SET att_daily = CASE 
          ${chunk.map(() => `WHEN perner = ? AND tanggal = ? THEN ?`).join(" ")}
          ELSE att_daily
        END
        WHERE (perner, tanggal) IN (${chunk.map(() => "(?, ?)").join(", ")})
      `;

      const attParams = [];
      // Build parameters for CASE statement
      chunk.forEach((item) => {
        attParams.push(item.perner, item.tanggal, item.tipe_text);
      });
      // Build parameters for WHERE clause
      chunk.forEach((item) => {
        attParams.push(item.perner, item.tanggal);
      });

      tasks.push(
        new Promise((resolve, reject) => {
          conn.query(batchAttSQL, attParams, (err, result) => {
            if (err) {
              console.error(`❌ ATT Batch ${chunkIndex + 1} failed:`, err);
              return reject(err);
            }
            console.log(
              `✅ ATT Batch ${chunkIndex + 1}/${attChunks.length}: ${
                result.affectedRows
              } rows updated`
            );
            resolve({
              type: "att",
              chunk: chunkIndex + 1,
              affectedRows: result.affectedRows,
            });
          });
        })
      );
    });
  }

  // BATCH 2: Update ABS (absence) records
  if (absUpdates.length > 0) {
    // Chunk large datasets
    const absChunks = [];
    const chunkSize = 500;

    for (let i = 0; i < absUpdates.length; i += chunkSize) {
      absChunks.push(absUpdates.slice(i, i + chunkSize));
    }

    absChunks.forEach((chunk, chunkIndex) => {
      const batchAbsSQL = `
        UPDATE olah_absensi 
        SET abs_daily = CASE 
          ${chunk.map(() => `WHEN perner = ? AND tanggal = ? THEN ?`).join(" ")}
          ELSE abs_daily
        END
        WHERE (perner, tanggal) IN (${chunk.map(() => "(?, ?)").join(", ")})
      `;

      const absParams = [];
      // Build parameters for CASE statement
      chunk.forEach((item) => {
        absParams.push(item.perner, item.tanggal, item.tipe_text);
      });
      // Build parameters for WHERE clause
      chunk.forEach((item) => {
        absParams.push(item.perner, item.tanggal);
      });

      tasks.push(
        new Promise((resolve, reject) => {
          conn.query(batchAbsSQL, absParams, (err, result) => {
            if (err) {
              console.error(`❌ ABS Batch ${chunkIndex + 1} failed:`, err);
              return reject(err);
            }
            console.log(
              `✅ ABS Batch ${chunkIndex + 1}/${absChunks.length}: ${
                result.affectedRows
              } rows updated`
            );
            resolve({
              type: "abs",
              chunk: chunkIndex + 1,
              affectedRows: result.affectedRows,
            });
          });
        })
      );
    });
  }

  // Execute all batch operations
  Promise.all(tasks)
    .then((results) => {
      const endTime = Date.now();
      const duration = endTime - startTime;

      // Calculate statistics
      const attResults = results.filter((r) => r.type === "att");
      const absResults = results.filter((r) => r.type === "abs");

      const totalAttAffected = attResults.reduce(
        (sum, r) => sum + r.affectedRows,
        0
      );
      const totalAbsAffected = absResults.reduce(
        (sum, r) => sum + r.affectedRows,
        0
      );
      const totalAffected = totalAttAffected + totalAbsAffected;

      console.log(`⚡ ATT/ABS Enhanced Batch completed in ${duration}ms`);
      console.log(
        `   - ATT batches: ${attResults.length}, affected: ${totalAttAffected} rows`
      );
      console.log(
        `   - ABS batches: ${absResults.length}, affected: ${totalAbsAffected} rows`
      );
      console.log(`   - Total affected: ${totalAffected} rows`);

      res.json({
        message: `✅ Berhasil memproses ${data.length} data ATT ABS Daily dengan enhanced batch method (${duration}ms).`,
        performance: {
          duration_ms: duration,
          total_records: data.length,
          att_records: attUpdates.length,
          abs_records: absUpdates.length,
          att_batches: attResults.length,
          abs_batches: absResults.length,
          att_affected: totalAttAffected,
          abs_affected: totalAbsAffected,
          total_affected: totalAffected,
          method: "enhanced_batch_with_chunking",
          avg_records_per_batch: Math.round(
            (attUpdates.length + absUpdates.length) /
              (attResults.length + absResults.length)
          ),
        },
      });
    })
    .catch((err) => {
      console.error("❌ Gagal enhanced batch update ATT/ABS:", err);
      res.status(500).json({
        message: "❌ Gagal update ATT ABS Daily.",
        error: err.message,
        debug: {
          att_records: attUpdates.length,
          abs_records: absUpdates.length,
          total_tasks: tasks.length,
        },
      });
    });
});

// OPTIMASI ATT SAP
app.post("/update-att-sap", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  console.log(
    `🔄 Processing ${data.length} ATT SAP records with BATCH method...`
  );
  const startTime = Date.now();

  // Chunk untuk dataset besar
  const chunkSize = 500;
  const chunks = [];
  for (let i = 0; i < data.length; i += chunkSize) {
    chunks.push(data.slice(i, i + chunkSize));
  }

  const tasks = chunks.map((chunk, chunkIndex) => {
    const batchSQL = `
      UPDATE olah_absensi 
      SET att_sap = CASE 
        ${chunk.map(() => `WHEN perner = ? AND tanggal = ? THEN ?`).join(" ")}
        ELSE att_sap
      END
      WHERE (perner, tanggal) IN (${chunk.map(() => "(?, ?)").join(", ")})
    `;

    const params = [];
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal, item.tipe_text);
    });
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal);
    });

    return new Promise((resolve, reject) => {
      conn.query(batchSQL, params, (err, result) => {
        if (err) return reject(err);
        console.log(
          `✅ ATT SAP Batch ${chunkIndex + 1}/${chunks.length}: ${
            result.affectedRows
          } rows`
        );
        resolve(result.affectedRows);
      });
    });
  });

  Promise.all(tasks)
    .then((results) => {
      const endTime = Date.now();
      const duration = endTime - startTime;
      const totalAffected = results.reduce((sum, rows) => sum + rows, 0);

      res.json({
        message: `✅ Berhasil memproses ${data.length} data ATT SAP dengan batch method (${duration}ms).`,
        performance: {
          duration_ms: duration,
          total_records: data.length,
          affected_rows: totalAffected,
          batches: chunks.length,
          method: "batch_chunked",
        },
      });
    })
    .catch((err) => {
      console.error("❌ Gagal batch update ATT SAP:", err);
      res.status(500).json({ message: "❌ Gagal update ATT SAP." });
    });
});

// OPTIMASI ABS SAP
app.post("/update-abs-sap", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  console.log(
    `🔄 Processing ${data.length} ABS SAP records with BATCH method...`
  );
  const startTime = Date.now();

  // Chunk untuk dataset besar
  const chunkSize = 500;
  const chunks = [];
  for (let i = 0; i < data.length; i += chunkSize) {
    chunks.push(data.slice(i, i + chunkSize));
  }

  const tasks = chunks.map((chunk, chunkIndex) => {
    const batchSQL = `
      UPDATE olah_absensi 
      SET abs_sap = CASE 
        ${chunk.map(() => `WHEN perner = ? AND tanggal = ? THEN ?`).join(" ")}
        ELSE abs_sap
      END
      WHERE (perner, tanggal) IN (${chunk.map(() => "(?, ?)").join(", ")})
    `;

    const params = [];
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal, item.tipe_text);
    });
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal);
    });

    return new Promise((resolve, reject) => {
      conn.query(batchSQL, params, (err, result) => {
        if (err) return reject(err);
        console.log(
          `✅ ABS SAP Batch ${chunkIndex + 1}/${chunks.length}: ${
            result.affectedRows
          } rows`
        );
        resolve(result.affectedRows);
      });
    });
  });

  Promise.all(tasks)
    .then((results) => {
      const endTime = Date.now();
      const duration = endTime - startTime;
      const totalAffected = results.reduce((sum, rows) => sum + rows, 0);

      res.json({
        message: `✅ Berhasil memproses ${data.length} data ABS SAP dengan batch method (${duration}ms).`,
        performance: {
          duration_ms: duration,
          total_records: data.length,
          affected_rows: totalAffected,
          batches: chunks.length,
          method: "batch_chunked",
        },
      });
    })
    .catch((err) => {
      console.error("❌ Gagal batch update ABS SAP:", err);
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

  console.log(
    `🔄 Processing ${data.length} SPPD Umum records with ENHANCED BATCH method...`
  );
  const startTime = Date.now();

  // STEP 1: Process SPPD data in memory (optimized)
  const processDataStart = Date.now();
  const finalDataMap = new Map(); // Use Map for better performance
  let processedInputs = 0;
  let errorCount = 0;
  let totalDatesGenerated = 0;

  // Process each SPPD record
  data.forEach((item, index) => {
    try {
      const { perner, tanggal, keterangan } = item;

      // Validate required fields
      if (!perner || !tanggal) {
        console.warn(`⚠️ Record ${index + 1}: Missing perner or tanggal`);
        errorCount++;
        return;
      }

      // For SPPD Umum, the 'tanggal' field actually contains date range info
      // But from your original logic, it seems like direct mapping
      const key = `${perner}||${tanggal}`;

      // Handle duplicate entries for same perner+tanggal
      if (finalDataMap.has(key)) {
        const existing = finalDataMap.get(key);
        // Merge keterangan if different
        const existingKet = existing.keterangan;
        const newKet = keterangan || "Perjalanan dinas";

        if (existingKet !== newKet) {
          finalDataMap.set(key, {
            perner,
            tanggal,
            keterangan: `${existingKet} || ${newKet}`,
          });
        }
      } else {
        finalDataMap.set(key, {
          perner,
          tanggal,
          keterangan: keterangan || "Perjalanan dinas",
        });
        totalDatesGenerated++;
      }

      processedInputs++;
    } catch (error) {
      console.error(`❌ Error processing SPPD record ${index + 1}:`, error);
      errorCount++;
    }
  });

  // Alternative: If your input data is actually in raw format (perner, start, end)
  // Uncomment this section if that's the case:
  /*
  data.forEach((row, index) => {
    try {
      const parts = row.split("\t");
      if (parts.length < 3) {
        console.warn(`⚠️ Row ${index + 1}: Insufficient data (${parts.length} parts)`);
        errorCount++;
        return;
      }

      const [perner, start, end] = parts;
      
      // Parse dates
      const [sd, sm, sy] = start.split("/");
      const [ed, em, ey] = end.split("/");
      const startDate = new Date(`${sy}-${sm}-${sd}`);
      const endDate = new Date(`${ey}-${em}-${ed}`);

      // Validate date range
      if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
        console.warn(`⚠️ Row ${index + 1}: Invalid date format`);
        errorCount++;
        return;
      }

      if (endDate < startDate) {
        console.warn(`⚠️ Row ${index + 1}: End date before start date`);
        errorCount++;
        return;
      }

      // Generate date range
      for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
        const tanggal = d.toISOString().slice(0, 10);
        const key = `${perner}||${tanggal}`;

        if (!finalDataMap.has(key)) {
          finalDataMap.set(key, []);
        }
        finalDataMap.get(key).push("Perjalanan dinas");
        totalDatesGenerated++;
      }

      processedInputs++;
    } catch (error) {
      console.error(`❌ Error processing raw SPPD row ${index + 1}:`, error);
      errorCount++;
    }
  });

  // Convert Map to final data structure for raw format
  const finalData = [];
  for (const [key, keteranganArray] of finalDataMap.entries()) {
    const [perner, tanggal] = key.split("||");
    finalData.push({
      perner,
      tanggal,
      keterangan: keteranganArray.join(" || ")
    });
  }
  */

  // Convert Map to Array (for processed format)
  const finalData = Array.from(finalDataMap.values());
  const processDataDuration = Date.now() - processDataStart;

  console.log(`🔧 SPPD Data processing completed:`);
  console.log(`   - Input records: ${data.length}`);
  console.log(`   - Processed records: ${processedInputs}`);
  console.log(`   - Error records: ${errorCount}`);
  console.log(`   - Generated records: ${finalData.length}`);
  console.log(`   - Processing time: ${processDataDuration}ms`);

  if (finalData.length === 0) {
    return res.status(400).json({
      message: "⚠️ Tidak ada data valid untuk SPPD Umum.",
      debug: {
        input_records: data.length,
        processed_records: processedInputs,
        error_count: errorCount,
      },
    });
  }

  // STEP 2: Batch database update (chunked)
  const batchUpdateStart = Date.now();
  const chunkSize = 1000; // Large chunks for SPPD
  const chunks = [];

  for (let i = 0; i < finalData.length; i += chunkSize) {
    chunks.push(finalData.slice(i, i + chunkSize));
  }

  console.log(
    `📦 SPPD data chunked into ${chunks.length} batches (max ${chunkSize} records each)`
  );

  const tasks = chunks.map((chunk, chunkIndex) => {
    const batchSQL = `
      UPDATE olah_absensi 
      SET sppd_umum = CASE 
        ${chunk.map(() => `WHEN perner = ? AND tanggal = ? THEN ?`).join(" ")}
        ELSE sppd_umum
      END
      WHERE (perner, tanggal) IN (${chunk.map(() => "(?, ?)").join(", ")})
    `;

    const params = [];
    // Build parameters for CASE statement
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal, item.keterangan);
    });
    // Build parameters for WHERE clause
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal);
    });

    return new Promise((resolve, reject) => {
      conn.query(batchSQL, params, (err, result) => {
        if (err) {
          console.error(`❌ SPPD Umum Batch ${chunkIndex + 1} failed:`, err);
          return reject(err);
        }

        console.log(
          `✅ SPPD Batch ${chunkIndex + 1}/${chunks.length}: ${
            result.affectedRows
          } rows updated`
        );
        resolve({
          chunk: chunkIndex + 1,
          affectedRows: result.affectedRows,
          chunkSize: chunk.length,
        });
      });
    });
  });

  // Execute all batch operations
  Promise.all(tasks)
    .then((results) => {
      const batchUpdateDuration = Date.now() - batchUpdateStart;
      const overallDuration = Date.now() - startTime;

      const totalAffected = results.reduce((sum, r) => sum + r.affectedRows, 0);
      const avgRowsPerBatch = Math.round(totalAffected / results.length);

      console.log(
        `⚡ SPPD Umum Enhanced Batch completed in ${overallDuration}ms`
      );
      console.log(`   - Data processing: ${processDataDuration}ms`);
      console.log(`   - Batch updates: ${batchUpdateDuration}ms`);
      console.log(
        `   - Total affected: ${totalAffected} rows across ${results.length} batches`
      );

      res.json({
        message: `✅ Berhasil mengisi ${totalAffected} baris SPPD Umum dengan enhanced batch method (${overallDuration}ms).`,
        performance: {
          duration_ms: overallDuration,
          data_processing_ms: processDataDuration,
          batch_update_ms: batchUpdateDuration,
          input_records: data.length,
          processed_records: processedInputs,
          error_records: errorCount,
          generated_records: finalData.length,
          affected_rows: totalAffected,
          batches: results.length,
          avg_rows_per_batch: avgRowsPerBatch,
          chunk_size: chunkSize,
          method: "enhanced_batch_with_deduplication",
        },
        statistics: {
          successful_batches: results.length,
          failed_batches: 0,
          success_rate: Math.round((processedInputs / data.length) * 100) + "%",
          efficiency_ratio:
            Math.round((totalAffected / finalData.length) * 100) + "%",
          duplicate_handling: "merged_with_separator",
        },
      });
    })
    .catch((err) => {
      console.error("❌ Gagal enhanced batch update SPPD Umum:", err);
      res.status(500).json({
        message: "❌ Gagal update SPPD Umum.",
        error: err.message,
        debug: {
          input_records: data.length,
          processed_records: processedInputs,
          error_records: errorCount,
          generated_records: finalData.length,
          total_batches: chunks.length,
        },
      });
    });
});

app.post("/update-work-schedule", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  console.log(
    `🔄 Processing ${data.length} work schedule records with OPTIMIZED BATCH method...`
  );
  const startTime = Date.now();

  // For large datasets, use chunking to avoid MySQL query limits
  const chunkSize = 1500; // Optimal size for work schedule batch updates
  const chunks = [];

  for (let i = 0; i < data.length; i += chunkSize) {
    chunks.push(data.slice(i, i + chunkSize));
  }

  console.log(
    `📦 Data chunked into ${chunks.length} batch(es) (max ${chunkSize} records each)`
  );

  const tasks = chunks.map((chunk, chunkIndex) => {
    const batchSQL = `
      UPDATE olah_absensi 
      SET ws_rule = CASE 
        ${chunk.map(() => `WHEN perner = ? AND tanggal = ? THEN ?`).join(" ")}
        ELSE ws_rule
      END
      WHERE (perner, tanggal) IN (${chunk.map(() => "(?, ?)").join(", ")})
    `;

    const params = [];
    // Build parameters for CASE statement
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal, item.ws_rule);
    });
    // Build parameters for WHERE clause
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal);
    });

    return new Promise((resolve, reject) => {
      conn.query(batchSQL, params, (err, result) => {
        if (err) {
          console.error(
            `❌ Work Schedule Batch ${chunkIndex + 1} failed:`,
            err
          );
          return reject(err);
        }

        console.log(
          `✅ WS Batch ${chunkIndex + 1}/${chunks.length}: ${
            result.affectedRows
          } rows updated`
        );
        resolve({
          chunk: chunkIndex + 1,
          affectedRows: result.affectedRows,
          chunkSize: chunk.length,
        });
      });
    });
  });

  Promise.all(tasks)
    .then((results) => {
      const endTime = Date.now();
      const duration = endTime - startTime;
      const totalAffected = results.reduce((sum, r) => sum + r.affectedRows, 0);

      console.log(
        `⚡ Work Schedule Optimized Batch completed in ${duration}ms`
      );
      console.log(`   - Total records processed: ${data.length}`);
      console.log(`   - Total rows affected: ${totalAffected}`);
      console.log(`   - Batches executed: ${results.length}`);
      console.log(
        `   - Database efficiency: ${Math.round(
          (totalAffected / data.length) * 100
        )}%`
      );

      res.json({
        message: `✅ Berhasil memproses ${data.length} data Work Schedule dengan optimized batch method (${duration}ms).`,
        performance: {
          duration_ms: duration,
          total_records: data.length,
          affected_rows: totalAffected,
          batches: results.length,
          avg_records_per_batch: Math.round(data.length / results.length),
          database_efficiency:
            Math.round((totalAffected / data.length) * 100) + "%",
          method: "optimized_batch_with_chunking",
        },
      });
    })
    .catch((err) => {
      console.error("❌ Gagal optimized batch update Work Schedule:", err);
      res.status(500).json({
        message: "❌ Gagal update Work Schedule.",
        error: err.message,
        debug: {
          total_records: data.length,
          total_batches: chunks.length,
          chunk_size: chunkSize,
        },
      });
    });
});

app.post("/update-substitution-daily", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  console.log(
    `🔄 Processing ${data.length} substitution daily records with BATCH method...`
  );
  const startTime = Date.now();

  // Single batch update
  const batchSQL = `
    UPDATE olah_absensi 
    SET jenis_jam_kerja_shift_daily = CASE 
      ${data.map(() => `WHEN perner = ? AND tanggal = ? THEN ?`).join(" ")}
      ELSE jenis_jam_kerja_shift_daily
    END
    WHERE (perner, tanggal) IN (${data.map(() => "(?, ?)").join(", ")})
  `;

  const params = [];
  // Build parameters for CASE statement
  data.forEach((item) => {
    params.push(item.perner, item.tanggal, item.jenis_shift);
  });
  // Build parameters for WHERE clause
  data.forEach((item) => {
    params.push(item.perner, item.tanggal);
  });

  conn.query(batchSQL, params, (err, result) => {
    if (err) {
      console.error("❌ Gagal batch update Substitution Daily:", err);
      return res
        .status(500)
        .json({ message: "❌ Gagal update Substitution Daily." });
    }

    const endTime = Date.now();
    const duration = endTime - startTime;

    console.log(
      `⚡ Substitution Daily Batch completed in ${duration}ms, ${result.affectedRows} rows affected`
    );

    res.json({
      message: `✅ Berhasil memproses ${data.length} data Substitution Daily dengan batch method (${duration}ms).`,
      performance: {
        duration_ms: duration,
        total_records: data.length,
        affected_rows: result.affectedRows,
        method: "batch",
      },
    });
  });
});

app.post("/update-substitution-sap", (req, res) => {
  const { data } = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).json({ message: "❌ Data tidak valid." });
  }

  console.log(`🔍 DEBUGGING substitution SAP data format...`);
  console.log(`📊 Data length: ${data.length}`);
  console.log(`🔍 First item type: ${typeof data[0]}`);
  console.log(
    `🔍 First item sample:`,
    JSON.stringify(data[0]).substring(0, 200)
  );

  // Check if it's array of strings (raw) or array of objects (processed)
  const isStringFormat = typeof data[0] === "string";
  console.log(
    `📋 Format detected: ${
      isStringFormat ? "STRING (needs parsing)" : "OBJECT (pre-processed)"
    }`
  );

  const startTime = Date.now();
  let finalData = [];

  if (isStringFormat) {
    // === PROCESS RAW STRING DATA ===
    console.log(`🔧 Processing raw string format...`);

    const dataMap = new Map();
    let processedRows = 0;
    let errorCount = 0;
    let barisError = null;

    // Use ORIGINAL LOGIC exactly as in your server.js
    for (let i = 0; i < data.length; i++) {
      try {
        const row = data[i];
        const parts = row.split("\t");

        if (parts.length === 7) {
          parts.push(""); // tambahkan kolom kosong di akhir agar jadi kolom ke-8
        }

        if (parts.length < 8) {
          errorCount++;
          continue;
        }

        const perner = parts[0];
        const tglMulai = parts[2];
        const tglAkhir = parts[3];
        const jamStart = parts[4].trim();
        const jamEnd = parts[5].trim();
        const jadwal = parts[7]?.trim()?.toLowerCase() || "";

        const [sd, sm, sy] = tglMulai.split("/");
        const [ed, em, ey] = tglAkhir.split("/");

        const startDate = new Date(`${sy}-${sm}-${sd}`);
        const endDate = new Date(`${ey}-${em}-${ed}`);

        let tipe = "";
        let jamMasuk = "";
        let jamPulang = "";

        if (jadwal === "free") {
          tipe = "OFF";
          jamMasuk = "00.00";
          jamPulang = "00.00";
        } else {
          // Referensi shift - EXACT SAME AS ORIGINAL
          if (jamStart === "00:00:00" && jamEnd === "08:00:00") {
            tipe = "Shift2-Malam";
            jamMasuk = "00.00";
            jamPulang = "08.00";
          } else if (jamStart === "08:00:00" && jamEnd === "16:00:00") {
            tipe = "Shift2-Pagi";
            jamMasuk = "08.00";
            jamPulang = "16.00";
          } else if (jamStart === "16:00:00" && jamEnd === "00:00:00") {
            tipe = "Shift2-Siang";
            jamMasuk = "16.00";
            jamPulang = "24.00";
          } else {
            barisError = i + 1;
            break;
          }
        }

        const nilai = `${tipe}~${jamMasuk}~${jamPulang}`;

        for (
          let d = new Date(startDate);
          d <= endDate;
          d.setDate(d.getDate() + 1)
        ) {
          const tanggal = d.toISOString().slice(0, 10);
          const key = `${perner}||${tanggal}`;

          if (!dataMap.has(key)) {
            dataMap.set(key, []);
          }
          dataMap.get(key).push(nilai);
        }

        processedRows++;
      } catch (error) {
        console.error(`❌ Error processing row ${i + 1}:`, error);
        errorCount++;
      }
    }

    if (barisError !== null) {
      return res.status(400).json({
        message: `❌ Baris ${barisError} memiliki jam shift yang tidak dikenali.\nProses dibatalkan. Periksa kembali data yang Anda tempel.`,
      });
    }

    // Convert to final format
    for (const [key, nilaiArray] of dataMap.entries()) {
      const [perner, tanggal] = key.split("||");
      const unik = Array.from(new Set(nilaiArray));
      const jenis_shift = unik.length === 1 ? unik[0] : unik.join(" || ");

      finalData.push({ perner, tanggal, jenis_shift });
    }

    console.log(
      `✅ String processing: ${processedRows}/${data.length} rows → ${finalData.length} records`
    );
  } else {
    // === USE PRE-PROCESSED OBJECT DATA ===
    console.log(`🔧 Using pre-processed object format...`);

    // Validate object structure
    const sample = data[0];
    if (
      !sample.hasOwnProperty("perner") ||
      !sample.hasOwnProperty("tanggal") ||
      !sample.hasOwnProperty("jenis_shift")
    ) {
      console.log(
        `❌ Invalid object structure. Expected: {perner, tanggal, jenis_shift}`
      );
      console.log(`❌ Received:`, Object.keys(sample));

      return res.status(400).json({
        message:
          "❌ Invalid object format. Expected: {perner, tanggal, jenis_shift}",
        received_keys: Object.keys(sample),
        sample_data: sample,
      });
    }

    // Data is already processed, use directly
    finalData = data;
    console.log(
      `✅ Object format: Using ${finalData.length} pre-processed records`
    );
  }

  // Check if we have valid data
  if (finalData.length === 0) {
    return res.status(400).json({
      message: "⚠️ Tidak ada data valid untuk Substitution SAP.",
      debug: {
        input_format: isStringFormat ? "string" : "object",
        input_count: data.length,
        output_count: finalData.length,
      },
    });
  }

  // === BATCH PROCESSING (COMMON FOR BOTH FORMATS) ===
  console.log(
    `📦 Starting batch processing for ${finalData.length} records...`
  );

  const chunkSize = 500;
  const chunks = [];
  for (let i = 0; i < finalData.length; i += chunkSize) {
    chunks.push(finalData.slice(i, i + chunkSize));
  }

  console.log(`📦 Data chunked into ${chunks.length} batches`);

  const tasks = chunks.map((chunk, chunkIndex) => {
    const batchSQL = `
      UPDATE olah_absensi 
      SET jenis_jam_kerja_shift_sap = CASE 
        ${chunk.map(() => `WHEN perner = ? AND tanggal = ? THEN ?`).join(" ")}
        ELSE jenis_jam_kerja_shift_sap
      END
      WHERE (perner, tanggal) IN (${chunk.map(() => "(?, ?)").join(", ")})
    `;

    const params = [];
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal, item.jenis_shift);
    });
    chunk.forEach((item) => {
      params.push(item.perner, item.tanggal);
    });

    return new Promise((resolve, reject) => {
      conn.query(batchSQL, params, (err, result) => {
        if (err) {
          console.error(
            `❌ Substitution SAP Batch ${chunkIndex + 1} failed:`,
            err
          );
          return reject(err);
        }

        console.log(
          `✅ Substitution SAP Batch ${chunkIndex + 1}/${chunks.length}: ${
            result.affectedRows
          } rows updated`
        );
        resolve({
          chunk: chunkIndex + 1,
          affectedRows: result.affectedRows,
        });
      });
    });
  });

  Promise.all(tasks)
    .then((results) => {
      const endTime = Date.now();
      const duration = endTime - startTime;
      const totalAffected = results.reduce((sum, r) => sum + r.affectedRows, 0);

      console.log(
        `⚡ Substitution SAP adaptive processing completed in ${duration}ms`
      );
      console.log(
        `   - Format: ${isStringFormat ? "RAW STRING" : "PROCESSED OBJECT"}`
      );
      console.log(`   - Input: ${data.length} records`);
      console.log(`   - Output: ${finalData.length} records`);
      console.log(`   - Database: ${totalAffected} rows affected`);

      res.json({
        message: `✅ Berhasil memproses ${data.length} data Substitution SAP dengan adaptive method (${duration}ms).`,
        performance: {
          duration_ms: duration,
          input_records: data.length,
          processed_records: finalData.length,
          affected_rows: totalAffected,
          batches: results.length,
          format_detected: isStringFormat ? "raw_string" : "processed_object",
          method: "adaptive_batch_processing",
        },
      });
    })
    .catch((err) => {
      console.error("❌ Gagal adaptive batch update Substitution SAP:", err);
      res.status(500).json({
        message: "❌ Gagal update Substitution SAP.",
        error: err.message,
        debug: {
          input_format: isStringFormat ? "string" : "object",
          input_count: data.length,
          final_data_count: finalData.length,
        },
      });
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
  console.log("🔄 Starting SIMPLIFIED status ganda processing...");
  const overallStartTime = Date.now();

  // STEP 1: Reset (same as before)
  const resetSQL = `
    UPDATE olah_absensi SET
      status_ganda_att_abs = NULL, status_ganda_ws_rule = NULL,
      att_daily_new = NULL, abs_daily_new = NULL, att_sap_new = NULL, abs_sap_new = NULL, sppd_umum_new = NULL,
      value_att_abs = NULL, is_att_abs = NULL, value_shift_daily_sap = NULL, is_shift_daily_sap = NULL,
      jenis_jam_kerja_shift_daily_new = NULL, jenis_jam_kerja_shift_sap_new = NULL,
      status_jam_kerja = NULL, kategori_jam_kerja = NULL, komponen_perhitungan_jkp = NULL,
      status_absen = NULL, status_in_out = NULL, ket_in_out = NULL, kategori_hit_jkp = NULL,
      jam_kerja_pegawai = NULL, jam_kerja_pegawai_cleansing = NULL, jam_kerja_seharusnya = NULL
    WHERE tanggal IS NOT NULL
  `;

  conn.query(resetSQL, (err, resetResult) => {
    if (err) {
      console.error("❌ Reset failed:", err);
      return res.status(500).json({ message: "❌ Gagal mereset data." });
    }

    console.log(`✅ Reset: ${resetResult.affectedRows} rows`);

    // STEP 2: Get data
    const ambilSQL = `
      SELECT perner, tanggal, att_daily, abs_daily, att_sap, abs_sap, sppd_umum,
             jenis_jam_kerja_shift_daily, jenis_jam_kerja_shift_sap, ws_rule, jenis_hari
      FROM olah_absensi WHERE tanggal IS NOT NULL ORDER BY perner, tanggal
    `;

    conn.query(ambilSQL, (err, rows) => {
      if (err) {
        console.error("❌ Fetch failed:", err);
        return res.status(500).json({ message: "❌ Gagal mengambil data." });
      }

      console.log(`📊 Fetched: ${rows.length} rows`);

      if (rows.length === 0) {
        return res.json({ message: "⚠️ Tidak ada data untuk diproses." });
      }

      // STEP 3: Process in memory (same business logic)
      console.log("🔧 Processing business logic...");
      const processStart = Date.now();

      const processedData = rows.map((row) => {
        // [Same business logic as before - shortened for brevity]
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

        // 1. Status ganda att/abs
        const nilaiIsi = [att_daily, abs_daily, att_sap, abs_sap, sppd_umum]
          .filter((v) => v && v.trim() !== "")
          .map((v) => v.trim());
        let status = "Normal";
        if (nilaiIsi.length > 1) {
          const unik = new Set(nilaiIsi);
          if (unik.size > 1) status = "Ganda";
        }

        // 2. Status ganda ws_rule
        const isiDaily = (jenis_jam_kerja_shift_daily || "").trim();
        const isiSAP = (jenis_jam_kerja_shift_sap || "").trim();
        let status_ws = "Normal";
        const keduanyaTerisi = isiDaily && isiSAP;
        const isSama = isiDaily === isiSAP;
        const adaDoubleBar = isiDaily.includes("||") || isiSAP.includes("||");
        if ((keduanyaTerisi && !isSama) || adaDoubleBar) {
          status_ws = "Ganda";
        }

        // 3. Copy *_new values (simplified)
        let att_daily_new = null,
          abs_daily_new = null,
          att_sap_new = null,
          abs_sap_new = null,
          sppd_umum_new = null;
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

        // 4. Copy shift *_new
        let jenis_jam_kerja_shift_daily_new = null,
          jenis_jam_kerja_shift_sap_new = null;
        let value_shift_daily_sap = null,
          is_shift_daily_sap = "false";

        if (status_ws === "Normal") {
          if (isiSAP) jenis_jam_kerja_shift_sap_new = isiSAP;
          else if (isiDaily) jenis_jam_kerja_shift_daily_new = isiDaily;
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
          value_att_abs && value_att_abs.trim() !== "" ? "true" : "false";

        // 5. Parse ws_rule
        let wsin = "-",
          wsout = "-";
        if (ws_rule && ws_rule.includes("~")) {
          const [inRaw, outRaw] = ws_rule.split("~");
          wsin = inRaw.replace(/\./g, ":");
          wsout = outRaw.replace(/\./g, ":");
        }

        // 6. Status jam kerja
        const shiftValue = value_shift_daily_sap?.toLowerCase() || "";
        let status_jam_kerja = "-";

        if (shiftValue.includes("pdkb"))
          status_jam_kerja = `Normal => PDKB (${wsin}-${wsout})`;
        else if (shiftValue.includes("piket"))
          status_jam_kerja = `Normal => PIKET (${wsin}-${wsout})`;
        else if (shiftValue.includes("shift2-malam"))
          status_jam_kerja = "Shift => Malam (00:00-08:00)";
        else if (shiftValue.includes("shift2-siang"))
          status_jam_kerja = "Shift => Siang (16:00-24:00)";
        else if (shiftValue.includes("shift2-pagi"))
          status_jam_kerja = "Shift => Pagi (08:00-16:00)";
        else if (shiftValue.includes("off")) status_jam_kerja = "Shift => OFF";
        else status_jam_kerja = `Normal => (${wsin}-${wsout})`;

        // 7. Kategori jam kerja
        let kategori_jam_kerja = "Normal";
        if (shiftValue.includes("pdkb")) kategori_jam_kerja = "PDKB";
        else if (shiftValue.includes("piket")) kategori_jam_kerja = "PIKET";
        else if (shiftValue.includes("shift2-") || shiftValue.includes("off"))
          kategori_jam_kerja = "Shift";

        // 8. Komponen perhitungan jkp
        const isHariKerja =
          jenis_hari && jenis_hari.toLowerCase().includes("kerja");
        let komponen_perhitungan_jkp = false;
        if (kategori_jam_kerja === "Shift") komponen_perhitungan_jkp = true;
        else if (
          kategori_jam_kerja &&
          kategori_jam_kerja !== "Shift" &&
          isHariKerja
        )
          komponen_perhitungan_jkp = true;

        return {
          perner,
          tanggal,
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
        };
      });

      const processedDuration = Date.now() - processStart;
      console.log(
        `✅ Processed: ${processedData.length} records (${processedDuration}ms)`
      );

      // STEP 4: Multiple batch updates (simplified approach)
      console.log("💾 Starting multiple batch updates...");
      const batchStartTime = Date.now();

      const chunkSize = 300;
      const chunks = [];
      for (let i = 0; i < processedData.length; i += chunkSize) {
        chunks.push(processedData.slice(i, i + chunkSize));
      }

      console.log(`📦 ${chunks.length} batches to process`);

      // Update in separate batches by field groups to avoid complex SQL
      const updateFieldGroups = [
        // Group 1: Status fields
        {
          name: "Status Fields",
          fields: [
            "status_ganda_att_abs",
            "status_ganda_ws_rule",
            "is_att_abs",
            "is_shift_daily_sap",
          ],
          dataFields: [
            "status",
            "status_ws",
            "is_att_abs",
            "is_shift_daily_sap",
          ],
        },
        // Group 2: *_new fields
        {
          name: "New Value Fields",
          fields: [
            "att_daily_new",
            "abs_daily_new",
            "att_sap_new",
            "abs_sap_new",
            "sppd_umum_new",
            "jenis_jam_kerja_shift_daily_new",
            "jenis_jam_kerja_shift_sap_new",
          ],
          dataFields: [
            "att_daily_new",
            "abs_daily_new",
            "att_sap_new",
            "abs_sap_new",
            "sppd_umum_new",
            "jenis_jam_kerja_shift_daily_new",
            "jenis_jam_kerja_shift_sap_new",
          ],
        },
        // Group 3: Value/text fields
        {
          name: "Value Fields",
          fields: [
            "value_att_abs",
            "value_shift_daily_sap",
            "status_jam_kerja",
            "kategori_jam_kerja",
          ],
          dataFields: [
            "value_att_abs",
            "value_shift_daily_sap",
            "status_jam_kerja",
            "kategori_jam_kerja",
          ],
        },
        // Group 4: Boolean fields
        {
          name: "Boolean Fields",
          fields: ["komponen_perhitungan_jkp"],
          dataFields: ["komponen_perhitungan_jkp"],
        },
      ];

      const allBatchTasks = [];

      updateFieldGroups.forEach((group, groupIndex) => {
        chunks.forEach((chunk, chunkIndex) => {
          const batchTask = new Promise((resolve, reject) => {
            const setClauses = group.fields.map(() => {
              return `CASE ${chunk
                .map(() => "WHEN perner = ? AND tanggal = ? THEN ?")
                .join(" ")} END`;
            });

            const batchSQL = `
              UPDATE olah_absensi SET
              ${group.fields
                .map((field, i) => `${field} = ${setClauses[i]}`)
                .join(", ")}
              WHERE (perner, tanggal) IN (${chunk
                .map(() => "(?, ?)")
                .join(", ")})
            `;

            const params = [];
            // Add CASE parameters for each field
            group.dataFields.forEach((dataField) => {
              chunk.forEach((item) => {
                params.push(item.perner, item.tanggal, item[dataField]);
              });
            });
            // Add WHERE parameters
            chunk.forEach((item) => {
              params.push(item.perner, item.tanggal);
            });

            const taskStart = Date.now();
            conn.query(batchSQL, params, (err, result) => {
              if (err) {
                console.error(
                  `❌ ${group.name} Batch ${chunkIndex + 1} failed:`,
                  err
                );
                return reject(err);
              }

              const taskDuration = Date.now() - taskStart;
              console.log(
                `✅ ${group.name} Batch ${chunkIndex + 1}/${chunks.length}: ${
                  result.affectedRows
                } rows (${taskDuration}ms)`
              );

              resolve({
                group: group.name,
                chunk: chunkIndex + 1,
                affectedRows: result.affectedRows,
                duration: taskDuration,
              });
            });
          });

          allBatchTasks.push(batchTask);
        });
      });

      // Execute all batch tasks
      Promise.all(allBatchTasks)
        .then((results) => {
          const batchDuration = Date.now() - batchStartTime;
          const overallDuration = Date.now() - overallStartTime;

          // Group results by field group
          const groupStats = updateFieldGroups.map((group) => {
            const groupResults = results.filter((r) => r.group === group.name);
            return {
              name: group.name,
              batches: groupResults.length,
              totalAffected: groupResults.reduce(
                (sum, r) => sum + r.affectedRows,
                0
              ),
              avgDuration: Math.round(
                groupResults.reduce((sum, r) => sum + r.duration, 0) /
                  groupResults.length
              ),
            };
          });

          const totalAffected = results.reduce(
            (sum, r) => sum + r.affectedRows,
            0
          );

          console.log(`⚡ SIMPLIFIED STATUS GANDA COMPLETED!`);
          console.log(`   ⏱️ Overall: ${overallDuration}ms`);
          console.log(`   📊 Processed: ${processedData.length} records`);
          console.log(`   📦 Total batches: ${results.length}`);
          console.log(`   ✅ Total updates: ${totalAffected}`);

          groupStats.forEach((stat) => {
            console.log(
              `   📈 ${stat.name}: ${stat.batches} batches, ${stat.totalAffected} updates, ${stat.avgDuration}ms avg`
            );
          });

          res.json({
            message: `✅ Status Ganda diperbarui dengan simplified batch method (${overallDuration}ms).`,
            performance: {
              overall_duration_ms: overallDuration,
              processing_duration_ms: processedDuration,
              batch_duration_ms: batchDuration,
              total_records: processedData.length,
              total_updates: totalAffected,
              total_batches: results.length,
              field_groups: updateFieldGroups.length,
              chunk_size: chunkSize,
              method: "simplified_multi_group_batch_processing",
            },
            field_group_stats: groupStats,
          });
        })
        .catch((err) => {
          console.error("❌ Simplified batch processing failed:", err);
          res.status(500).json({
            message: "❌ Gagal simplified batch processing.",
            error: err.message,
          });
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
    console.log("🔄 Starting SIMPLIFIED JKP backend selective processing...");
    const overallStartTime = Date.now();

    const { filterValue, targetRows } = req.body;
    const hardcodedTargetRows = [];

    const selectedTargetRows =
      targetRows ||
      (hardcodedTargetRows.length > 0 ? hardcodedTargetRows : null);
    const isProcessAllData =
      !selectedTargetRows || selectedTargetRows.length === 0;

    console.log(`🔍 Mode: ${isProcessAllData ? "ALL_DATA" : "SELECTIVE"}`);

    // STEP 1: Fetch data (with timeout)
    console.log("📊 Fetching source data...");
    const fetchStart = Date.now();

    const ambil = await Promise.race([
      fetch(`${BASE_URL}/ambil-data-absensi-untuk-jkp`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          filterValue: filterValue || "shift_daily => Shift2-Malam~00.00~08.00",
        }),
      }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("Fetch timeout")), 30000)
      ),
    ]);

    if (!ambil.ok) throw new Error(`HTTP error! status: ${ambil.status}`);

    const allData = await ambil.json();
    const fetchDuration = Date.now() - fetchStart;

    console.log(
      `✅ Fetched: ${allData?.length || 0} records (${fetchDuration}ms)`
    );

    if (!Array.isArray(allData) || allData.length === 0) {
      return res.status(400).json({ message: "❌ No data from source" });
    }

    // STEP 2: Filter data efficiently
    let filteredData = [];
    if (isProcessAllData) {
      filteredData = allData;
    } else {
      const targetMap = new Map();
      selectedTargetRows.forEach((target) => {
        if (target?.perner && target?.tanggal) {
          targetMap.set(
            `${target.perner}||${formatTanggalSafe(target.tanggal)}`,
            true
          );
        }
      });

      filteredData = allData.filter((row) => {
        if (!row?.perner || !row?.tanggal) return false;
        return targetMap.has(
          `${row.perner}||${formatTanggalSafe(row.tanggal)}`
        );
      });
    }

    console.log(`🔍 Filtered: ${filteredData.length} records`);

    if (filteredData.length === 0) {
      return res
        .status(400)
        .json({ message: "❌ No filtered data to process" });
    }

    // STEP 3: Process JKP calculations
    console.log("🔧 Processing JKP calculations...");
    const processStart = Date.now();

    const processedResults = [];
    let errorCount = 0;

    for (let i = 0; i < filteredData.length; i++) {
      try {
        const row = filteredData[i];
        if (!row.perner || !row.tanggal) {
          errorCount++;
          continue;
        }

        const hasilJKP = hitungJKPFinal(row);
        if (!hasilJKP) {
          errorCount++;
          continue;
        }

        const hasilJKPShift = hitungJKPShift(row);

        // Simplified logic (keeping only essential calculations)
        const rawDurasi = Number(hasilJKP.durasi ?? 0);
        const ket_in_out_final =
          hasilJKP.ket === "JKP Shift"
            ? hasilJKPShift?.ket_in_out ?? hasilJKP?.ket_in_out ?? null
            : hasilJKP?.ket_in_out ?? hasilJKPShift?.ket_in_out ?? null;

        const durasi_seharusnya_final =
          hasilJKP.ket === "JKP Shift"
            ? hasilJKPShift?.durasi_seharusnya ??
              hasilJKP?.durasi_seharusnya ??
              null
            : hasilJKP?.durasi_seharusnya ??
              hasilJKPShift?.durasi_seharusnya ??
              null;

        let rawDurasiCleansing, daily_in_raw, daily_out_raw;

        if (hasilJKP.ket === "JKP Shift") {
          rawDurasiCleansing = Number(
            hasilJKPShift.durasi_cleansing_c ??
              hasilJKPShift.durasi_cleansing ??
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
            row.daily_in ??
            null;
          daily_out_raw =
            hasilJKP.daily_out_cleansing_c ??
            hasilJKP.daily_out_cleansing ??
            row.daily_out ??
            null;
        }

        const jam_kerja_pegawai_cleansing = !isNaN(rawDurasiCleansing)
          ? parseFloat(rawDurasiCleansing.toFixed(3))
          : 0;
        const jam_kerja_seharusnya =
          durasi_seharusnya_final !== null &&
          !isNaN(Number(durasi_seharusnya_final))
            ? parseFloat(Number(durasi_seharusnya_final).toFixed(3))
            : null;

        let persentase = null;
        if (jam_kerja_seharusnya !== null && jam_kerja_seharusnya > 0) {
          persentase = parseFloat(
            (
              (jam_kerja_pegawai_cleansing / jam_kerja_seharusnya) *
              100
            ).toFixed(2)
          );
        }

        processedResults.push({
          perner: row.perner,
          tanggal: formatTanggalSafe(row.tanggal),
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
        });

        // Progress for large datasets
        if ((i + 1) % 1000 === 0) {
          console.log(`⏳ Processed: ${i + 1}/${filteredData.length}`);
        }
      } catch (error) {
        console.error(`Error processing row ${i + 1}:`, error.message);
        errorCount++;
      }
    }

    const processDuration = Date.now() - processStart;
    console.log(
      `✅ JKP calculations: ${processedResults.length} successful, ${errorCount} errors (${processDuration}ms)`
    );

    if (processedResults.length === 0) {
      return res
        .status(400)
        .json({ message: "❌ No successful JKP calculations" });
    }

    // STEP 4: Simplified batch updates - separate by field groups
    console.log("💾 Starting simplified batch database updates...");
    const batchStart = Date.now();

    const chunkSize = 200;
    const chunks = [];
    for (let i = 0; i < processedResults.length; i += chunkSize) {
      chunks.push(processedResults.slice(i, i + chunkSize));
    }

    console.log(`📦 ${chunks.length} chunks to process`);

    // Group fields for simpler SQL
    const fieldGroups = [
      // Group 1: Primary JKP fields
      {
        name: "Primary JKP",
        updates: [
          { field: "jam_kerja_pegawai", dataField: "jkp" },
          {
            field: "jam_kerja_pegawai_cleansing",
            dataField: "durasi_cleansing",
          },
          { field: "jam_kerja_seharusnya", dataField: "jam_kerja_seharusnya" },
          { field: "persentase", dataField: "persentase" },
        ],
      },
      // Group 2: Cleansing fields
      {
        name: "Cleansing Data",
        updates: [
          { field: "daily_in_cleansing", dataField: "daily_in_cleansing" },
          { field: "daily_out_cleansing", dataField: "daily_out_cleansing" },
          { field: "kategori_hit_jkp", dataField: "kategori_hit_jkp" },
          { field: "ket_in_out", dataField: "ket_in_out" },
        ],
      },
    ];

    const allBatchTasks = [];

    fieldGroups.forEach((group) => {
      chunks.forEach((chunk, chunkIndex) => {
        const task = new Promise((resolve, reject) => {
          // Simple field-by-field update
          const setClauses = group.updates.map((update) => {
            return `${update.field} = CASE ${chunk
              .map(() => "WHEN perner = ? AND DATE(tanggal) = ? THEN ?")
              .join(" ")} ELSE ${update.field} END`;
          });

          const sql = `
            UPDATE olah_absensi SET
            ${setClauses.join(",\n            ")}
            WHERE (perner, DATE(tanggal)) IN (${chunk
              .map(() => "(?, ?)")
              .join(", ")})
          `;

          const params = [];

          // Parameters for each field's CASE statement
          group.updates.forEach((update) => {
            chunk.forEach((item) => {
              params.push(item.perner, item.tanggal, item[update.dataField]);
            });
          });

          // Parameters for WHERE clause
          chunk.forEach((item) => {
            params.push(item.perner, item.tanggal);
          });

          const taskStart = Date.now();
          conn.query(sql, params, (err, result) => {
            if (err) {
              console.error(
                `❌ ${group.name} batch ${chunkIndex + 1} failed:`,
                err
              );
              return reject(err);
            }

            const taskDuration = Date.now() - taskStart;
            console.log(
              `✅ ${group.name} batch ${chunkIndex + 1}/${chunks.length}: ${
                result.affectedRows
              } rows (${taskDuration}ms)`
            );

            resolve({
              group: group.name,
              chunk: chunkIndex + 1,
              affectedRows: result.affectedRows,
              duration: taskDuration,
            });
          });
        });

        allBatchTasks.push(task);
      });
    });

    // Execute all batch tasks
    const batchResults = await Promise.allSettled(allBatchTasks);
    const batchDuration = Date.now() - batchStart;
    const overallDuration = Date.now() - overallStartTime;

    const successfulTasks = batchResults
      .filter((r) => r.status === "fulfilled")
      .map((r) => r.value);
    const failedTasks = batchResults.filter((r) => r.status === "rejected");
    const totalAffected = successfulTasks.reduce(
      (sum, r) => sum + r.affectedRows,
      0
    );

    // Calculate statistics
    const persentaseData = processedResults.filter(
      (item) => item.persentase !== null
    );
    let avgPersentase = null;
    if (persentaseData.length > 0) {
      const values = persentaseData.map((item) => item.persentase);
      avgPersentase = parseFloat(
        (values.reduce((a, b) => a + b, 0) / values.length).toFixed(2)
      );
    }

    console.log(`⚡ SIMPLIFIED JKP PROCESSING COMPLETED!`);
    console.log(`   ⏱️ Overall: ${overallDuration}ms`);
    console.log(`   📊 Fetch: ${fetchDuration}ms`);
    console.log(`   🔧 Process: ${processDuration}ms`);
    console.log(`   💾 Batch: ${batchDuration}ms`);
    console.log(`   ✅ Updates: ${totalAffected}`);
    console.log(
      `   📦 Successful tasks: ${successfulTasks.length}/${allBatchTasks.length}`
    );

    res.json({
      message: `✅ Simplified JKP processing completed. ${processedResults.length} records processed, ${totalAffected} database updates (${overallDuration}ms).`,
      performance: {
        overall_duration_ms: overallDuration,
        fetch_duration_ms: fetchDuration,
        processing_duration_ms: processDuration,
        batch_duration_ms: batchDuration,
        total_processed: processedResults.length,
        total_affected: totalAffected,
        successful_tasks: successfulTasks.length,
        failed_tasks: failedTasks.length,
        total_batches: chunks.length * fieldGroups.length,
        method: "simplified_field_group_batch_processing",
      },
      statistics: {
        error_count: errorCount,
        success_rate: `${Math.round(
          (processedResults.length / filteredData.length) * 100
        )}%`,
        with_persentase: persentaseData.length,
        avg_persentase: avgPersentase,
        processing_rate: `${Math.round(
          processedResults.length / (processDuration / 1000)
        )} calculations/sec`,
        database_rate: `${Math.round(
          totalAffected / (batchDuration / 1000)
        )} updates/sec`,
      },
    });
  } catch (err) {
    console.error("❌ Fatal error in simplified JKP processing:", err);
    res.status(500).json({
      message: "❌ Fatal error during simplified JKP processing.",
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
  console.log("🔄 Starting PARALLEL CHUNKED generate rekap absensi...");
  const overallStartTime = Date.now();

  // STEP 1: Clear existing data
  console.log("🗑️ Clearing existing rekap data...");
  const clearSQL = "DELETE FROM rekap_absensi";

  conn.query(clearSQL, (clearErr, clearResult) => {
    if (clearErr) {
      console.warn("⚠️ Warning clearing rekap_absensi:", clearErr.message);
    }

    console.log(
      `✅ Cleared: ${clearResult?.affectedRows || 0} existing records`
    );

    // STEP 2: Get unique PERNERs efficiently
    console.log("📋 Getting list of employees...");
    const getPernersStartTime = Date.now();

    const getPernersSQL = `
      SELECT DISTINCT perner, COUNT(*) as record_count
      FROM olah_absensi 
      WHERE perner IS NOT NULL 
      GROUP BY perner 
      ORDER BY perner
    `;

    conn.query(getPernersSQL, (err, perners) => {
      if (err) {
        console.error("❌ Failed to get employee list:", err);
        return res.status(500).json({
          success: false,
          message: "❌ Gagal mengambil daftar pegawai.",
          error: err.message,
        });
      }

      const getPernersDate = Date.now() - getPernersStartTime;
      console.log(
        `✅ Found ${perners.length} employees in ${getPernersDate}ms`
      );

      if (perners.length === 0) {
        return res.json({
          success: true,
          message: "⚠️ No employees found to process.",
          details: { total_pegawai: 0 },
        });
      }

      // STEP 3: Parallel chunked processing (OPTIMIZED)
      console.log("⚡ Starting parallel chunked processing...");
      const processStartTime = Date.now();

      const chunkSize = 20; // Process employees in parallel chunks
      const chunks = [];
      for (let i = 0; i < perners.length; i += chunkSize) {
        chunks.push(perners.slice(i, i + chunkSize));
      }

      console.log(
        `📦 Processing ${perners.length} employees in ${chunks.length} parallel chunks (${chunkSize} employees per chunk)`
      );

      // Process chunks in parallel with controlled concurrency
      const maxConcurrentChunks = 5; // Limit concurrent chunks to avoid overwhelming DB
      let processedChunks = 0;
      let totalProcessed = 0;
      let totalErrors = 0;
      const allResults = [];

      const processChunk = (chunk, chunkIndex) => {
        return new Promise((resolve, reject) => {
          console.log(
            `🔧 Processing chunk ${chunkIndex + 1}/${chunks.length} (${
              chunk.length
            } employees)`
          );

          // Build multi-employee query for this chunk
          const multiEmployeeSQL = `
            SELECT 
              perner as PERNER,
              COUNT(*) as TOTAL_HARI,
              COUNT(CASE WHEN jenis_hari LIKE '%HARI KERJA%' THEN 1 END) as HARI_KERJA,
              COUNT(CASE WHEN jenis_hari LIKE '%LIBUR%' THEN 1 END) as HARI_LIBUR,
              COUNT(CASE WHEN (correction_in = 'koreksi' OR correction_out = 'koreksi') THEN 1 END) as TOTAL_HARI_KOREKSI,
              COUNT(CASE WHEN correction_in = 'koreksi' AND (correction_out != 'koreksi' OR correction_out IS NULL) THEN 1 END) as KOREKSI_IN,
              COUNT(CASE WHEN correction_out = 'koreksi' AND (correction_in != 'koreksi' OR correction_in IS NULL) THEN 1 END) as KOREKSI_OUT,
              COUNT(CASE WHEN correction_in = 'koreksi' AND correction_out = 'koreksi' THEN 1 END) as KOREKSI_IN_OUT,
              COUNT(CASE WHEN status_jam_kerja LIKE '%Normal%' THEN 1 END) as TOTAL_JAM_KERJA_NORMAL,
              COUNT(CASE WHEN status_jam_kerja LIKE '%PIKET%' THEN 1 END) as PIKET,
              COUNT(CASE WHEN status_jam_kerja LIKE '%PDKB%' THEN 1 END) as PDKB,
              COUNT(CASE WHEN status_jam_kerja LIKE '%Normal%' AND status_jam_kerja NOT LIKE '%PIKET%' AND status_jam_kerja NOT LIKE '%PDKB%' THEN 1 END) as REGULER,
              COUNT(CASE WHEN status_jam_kerja LIKE '%Shift%' THEN 1 END) as TOTAL_JAM_KERJA_SHIFT,
              COUNT(CASE WHEN status_jam_kerja LIKE '%Pagi%' THEN 1 END) as SHIFT_PAGI,
              COUNT(CASE WHEN status_jam_kerja LIKE '%Siang%' THEN 1 END) as SHIFT_SIANG,
              COUNT(CASE WHEN status_jam_kerja LIKE '%Malam%' THEN 1 END) as SHIFT_MALAM,
              COUNT(CASE WHEN status_jam_kerja LIKE '%OFF%' THEN 1 END) as SHIFT_OFF,
              COUNT(CASE WHEN status_absen = 'Lengkap' THEN 1 END) as ABSEN_LENGKAP,
              COUNT(CASE WHEN status_absen LIKE '%tidak absen%' THEN 1 END) as TIDAK_ABSEN,
              COUNT(CASE WHEN status_absen LIKE '%in kosong%' THEN 1 END) as IN_KOSONG,
              COUNT(CASE WHEN status_absen LIKE '%out kosong%' THEN 1 END) as OUT_KOSONG,
              COUNT(CASE WHEN SUBSTRING_INDEX(value_att_abs, '_', 1) IN ('att', 'sppd') THEN 1 END) as SPPD_TUGAS_LUAR_DLL,
              COUNT(CASE WHEN SUBSTRING_INDEX(value_att_abs, '_', 1) = 'abs' THEN 1 END) as CUTI_IJIN,
              ROUND(COALESCE(SUM(CAST(jam_kerja_pegawai_cleansing AS DECIMAL(10,2))), 0), 2) as JAM_REALISASI,
              ROUND(COALESCE(SUM(CAST(jam_kerja_seharusnya AS DECIMAL(10,2))), 0), 2) as JAM_SEHARUSNYA,
              CASE 
                WHEN COALESCE(SUM(CAST(jam_kerja_seharusnya AS DECIMAL(10,2))), 0) = 0 THEN 0.00
                ELSE ROUND(
                  (COALESCE(SUM(CAST(jam_kerja_pegawai_cleansing AS DECIMAL(10,2))), 0) / 
                   SUM(CAST(jam_kerja_seharusnya AS DECIMAL(10,2)))) * 100, 
                  2
                )
              END as PERSENTASE_JKP
            FROM olah_absensi 
            WHERE perner IN (${chunk.map(() => "?").join(", ")})
            GROUP BY perner
            ORDER BY perner
          `;

          const chunkStartTime = Date.now();
          const pernerList = chunk.map((p) => p.perner);

          conn.query(multiEmployeeSQL, pernerList, (calcErr, chunkResults) => {
            if (calcErr) {
              console.error(
                `❌ Chunk ${chunkIndex + 1} calculation failed:`,
                calcErr
              );
              totalErrors += chunk.length;
              return reject(calcErr);
            }

            if (chunkResults.length === 0) {
              console.warn(`⚠️ Chunk ${chunkIndex + 1}: No data found`);
              totalErrors += chunk.length;
              return resolve([]);
            }

            // Batch insert chunk results
            const insertSQL = `
              INSERT INTO rekap_absensi (
                PERNER, TOTAL_HARI, HARI_KERJA, HARI_LIBUR, 
                TOTAL_HARI_KOREKSI, KOREKSI_IN, KOREKSI_OUT, KOREKSI_IN_OUT,
                TOTAL_JAM_KERJA_NORMAL, PIKET, PDKB, REGULER,
                TOTAL_JAM_KERJA_SHIFT, SHIFT_PAGI, SHIFT_SIANG, SHIFT_MALAM, SHIFT_OFF,
                ABSEN_LENGKAP, TIDAK_ABSEN, IN_KOSONG, OUT_KOSONG,
                SPPD_TUGAS_LUAR_DLL, CUTI_IJIN,
                JAM_REALISASI, JAM_SEHARUSNYA, PERSENTASE_JKP
              ) VALUES ${chunkResults
                .map(
                  () =>
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                )
                .join(", ")}
            `;

            const insertValues = [];
            chunkResults.forEach((rekapData) => {
              insertValues.push(
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
                rekapData.PERSENTASE_JKP || 0.0
              );
            });

            conn.query(insertSQL, insertValues, (insertErr, insertResult) => {
              if (insertErr) {
                console.error(
                  `❌ Chunk ${chunkIndex + 1} insert failed:`,
                  insertErr
                );
                totalErrors += chunk.length;
                return reject(insertErr);
              }

              const chunkDuration = Date.now() - chunkStartTime;
              processedChunks++;
              totalProcessed += chunkResults.length;

              console.log(
                `✅ Chunk ${chunkIndex + 1}/${chunks.length}: ${
                  chunkResults.length
                } employees, ${
                  insertResult.affectedRows
                } records inserted (${chunkDuration}ms)`
              );

              // Progress reporting
              const progressPercent = Math.round(
                (processedChunks / chunks.length) * 100
              );
              console.log(
                `📊 Progress: ${processedChunks}/${chunks.length} chunks (${progressPercent}%), ${totalProcessed}/${perners.length} employees`
              );

              resolve(chunkResults);
            });
          });
        });
      };

      // Process chunks with controlled concurrency
      const processChunksSequentially = async () => {
        const results = [];

        // Process chunks in batches of maxConcurrentChunks
        for (let i = 0; i < chunks.length; i += maxConcurrentChunks) {
          const batchChunks = chunks.slice(i, i + maxConcurrentChunks);
          const batchPromises = batchChunks.map((chunk, index) =>
            processChunk(chunk, i + index)
          );

          console.log(
            `🔄 Processing batch ${
              Math.floor(i / maxConcurrentChunks) + 1
            }/${Math.ceil(chunks.length / maxConcurrentChunks)} (${
              batchPromises.length
            } concurrent chunks)`
          );

          try {
            const batchResults = await Promise.allSettled(batchPromises);

            batchResults.forEach((result) => {
              if (result.status === "fulfilled") {
                results.push(...result.value);
              } else {
                console.error("Batch chunk failed:", result.reason);
              }
            });

            // Small delay between batches to avoid overwhelming the database
            if (i + maxConcurrentChunks < chunks.length) {
              await new Promise((resolve) => setTimeout(resolve, 100));
            }
          } catch (error) {
            console.error("Batch processing error:", error);
          }
        }

        return results;
      };

      // Execute chunked processing
      processChunksSequentially()
        .then(() => {
          const processDuration = Date.now() - processStartTime;
          console.log(
            `✅ Parallel processing completed: ${totalProcessed} successful, ${totalErrors} errors in ${processDuration}ms`
          );

          // STEP 4: Final statistics
          console.log("📈 Getting final statistics...");
          const finalStatsSQL = `
            SELECT 
              COUNT(*) as total_pegawai,
              ROUND(AVG(PERSENTASE_JKP), 2) as avg_persentase,
              ROUND(SUM(JAM_REALISASI), 2) as total_jam_realisasi,
              ROUND(SUM(JAM_SEHARUSNYA), 2) as total_jam_seharusnya,
              MIN(PERSENTASE_JKP) as min_persentase,
              MAX(PERSENTASE_JKP) as max_persentase
            FROM rekap_absensi
          `;

          conn.query(finalStatsSQL, (statsErr, statsResult) => {
            const overallDuration = Date.now() - overallStartTime;
            const stats = statsErr ? null : statsResult[0];

            console.log(`🎉 PARALLEL CHUNKED GENERATE REKAP COMPLETED!`);
            console.log(
              `   ⏱️ Overall: ${overallDuration}ms (${(
                overallDuration / 1000
              ).toFixed(2)}s)`
            );
            console.log(`   👥 Total employees: ${perners.length}`);
            console.log(`   ✅ Processed: ${totalProcessed}`);
            console.log(`   ❌ Errors: ${totalErrors}`);
            console.log(
              `   📦 Chunks: ${chunks.length} (${chunkSize} employees each)`
            );
            console.log(
              `   ⚡ Processing rate: ${Math.round(
                totalProcessed / (processDuration / 1000)
              )} employees/sec`
            );

            res.json({
              success: true,
              message: `⚡ Parallel chunked generate rekap completed in ${(
                overallDuration / 1000
              ).toFixed(2)}s. ${totalProcessed}/${
                perners.length
              } employees processed.`,
              performance: {
                overall_duration_ms: overallDuration,
                processing_duration_ms: processDuration,
                employees_per_second: Math.round(
                  totalProcessed / (processDuration / 1000)
                ),
                chunks_processed: processedChunks,
                chunk_size: chunkSize,
                max_concurrent_chunks: maxConcurrentChunks,
                method: "parallel_chunked_processing",
              },
              details: {
                total_pegawai: perners.length,
                successful: totalProcessed,
                failed: totalErrors,
                success_rate: `${Math.round(
                  (totalProcessed / perners.length) * 100
                )}%`,
                chunks: chunks.length,
                generated_at: new Date().toISOString(),
              },
              stats: stats || null,
            });
          });
        })
        .catch((err) => {
          console.error("❌ Fatal error during parallel processing:", err);
          res.status(500).json({
            success: false,
            message: "❌ Fatal error during parallel chunked processing.",
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

// testcommit
//ters lagi

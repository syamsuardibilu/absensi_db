app.post("/generate-rekap-absensi", (req, res) => {
  const startTime = Date.now();
  console.log("🚀 Starting rekap absensi generation...");

  // Step 1: Check if required tables exist
  const checkTablesSQL = `
    SELECT 
      COUNT(CASE WHEN table_name = 'olah_absensi' THEN 1 END) as olah_exists,
      COUNT(CASE WHEN table_name = 'data_pegawai' THEN 1 END) as pegawai_exists,
      COUNT(CASE WHEN table_name = 'rekap_absensi' THEN 1 END) as rekap_exists
    FROM information_schema.tables 
    WHERE table_schema = DATABASE()
      AND table_name IN ('olah_absensi', 'data_pegawai', 'rekap_absensi')
  `;

  conn.query(checkTablesSQL, (checkErr, checkResults) => {
    if (checkErr) {
      console.error("❌ Error checking tables:", checkErr.message);
      return res.status(500).json({
        success: false,
        message: "❌ Error checking database tables",
        error: checkErr.message,
      });
    }

    const tablesStatus = checkResults[0];

    if (tablesStatus.olah_exists === 0) {
      return res.status(404).json({
        success: false,
        message: "❌ Tabel olah_absensi tidak ditemukan",
      });
    }

    if (tablesStatus.pegawai_exists === 0) {
      return res.status(404).json({
        success: false,
        message: "❌ Tabel data_pegawai tidak ditemukan",
      });
    }

    if (tablesStatus.rekap_exists === 0) {
      return res.status(404).json({
        success: false,
        message: "❌ Tabel rekap_absensi tidak ditemukan",
      });
    }

    console.log("✅ All required tables exist");

    // Step 2: Clear existing rekap data
    const clearSQL = "DELETE FROM rekap_absensi";

    conn.query(clearSQL, (clearErr) => {
      if (clearErr) {
        console.error("❌ Error clearing rekap_absensi:", clearErr.message);
        return res.status(500).json({
          success: false,
          message: "❌ Gagal membersihkan data rekap sebelumnya",
          error: clearErr.message,
        });
      }

      console.log("✅ Previous rekap data cleared");

      // Step 3: Generate new rekap data dengan struktur yang sesuai dan JOIN untuk data pegawai
      const insertSQL = `
        INSERT INTO rekap_absensi (
          PERNER, NAMA, BIDANG_UNIT,
          TOTAL_HARI_REGULER, HARI_KERJA_REGULER, HARI_LIBUR_REGULER,
          TOTAL_HARI_REALISASI, HARI_KERJA_REALISASI, HARI_LIBUR_REALISASI,
          TOTAL_HARI_TANPA_KOREKSI, TOTAL_HARI_KOREKSI,
          KOREKSI_IN, KOREKSI_OUT, KOREKSI_IN_OUT,
          TOTAL_JAM_KERJA_NORMAL, PIKET, PDKB, REGULER,
          TOTAL_JAM_KERJA_SHIFT, SHIFT_PAGI, SHIFT_SIANG, SHIFT_MALAM, SHIFT_OFF,
          ABSEN_LENGKAP, ABSEN_TIDAK_LENGKAP, IN_OUT_KOSONG, IN_KOSONG, OUT_KOSONG,
          SPPD_TUGAS_LUAR_DLL, CUTI_IJIN,
          JAM_REALISASI, JAM_SEHARUSNYA, PERSENTASE_JKP
        )
        SELECT 
          o.perner,
          dp.nama,
          dp.bidang,
          
          -- JUMLAH HARI REGULER
          COUNT(CASE WHEN o.jenis_hari IN ('Kerja', 'Libur') THEN 1 END),
          COUNT(CASE WHEN o.jenis_hari = 'Kerja' THEN 1 END),
          COUNT(CASE WHEN o.jenis_hari = 'Libur' THEN 1 END),
          
          -- JUMLAH HARI REALISASI
          COUNT(CASE WHEN o.jenis_hari_realisasi IN ('Kerja', 'Libur') THEN 1 END),
          COUNT(CASE WHEN o.jenis_hari_realisasi = 'Kerja' THEN 1 END),
          COUNT(CASE WHEN o.jenis_hari_realisasi = 'Libur' THEN 1 END),
          
          -- DATA KOREKSI
          COUNT(CASE WHEN (o.correction_in IS NULL OR o.correction_in = '') 
                     AND (o.correction_out IS NULL OR o.correction_out = '') THEN 1 END),
          COUNT(CASE WHEN (o.correction_in IS NOT NULL AND o.correction_in != '') 
                     OR (o.correction_out IS NOT NULL AND o.correction_out != '') THEN 1 END),
          COUNT(CASE WHEN o.correction_in IS NOT NULL AND o.correction_in != '' 
                     AND (o.correction_out IS NULL OR o.correction_out = '') THEN 1 END),
          COUNT(CASE WHEN o.correction_out IS NOT NULL AND o.correction_out != '' 
                     AND (o.correction_in IS NULL OR o.correction_in = '') THEN 1 END),
          COUNT(CASE WHEN o.correction_in IS NOT NULL AND o.correction_in != '' 
                     AND o.correction_out IS NOT NULL AND o.correction_out != '' THEN 1 END),
          
          -- POLA JAM KERJA NORMAL
          COUNT(CASE WHEN o.status_jam_kerja = 'Normal' THEN 1 END),
          COUNT(CASE WHEN o.status_jam_kerja = 'Normal' AND o.kategori_jam_kerja = 'PIKET' THEN 1 END),
          COUNT(CASE WHEN o.status_jam_kerja = 'Normal' AND o.kategori_jam_kerja = 'PDKB' THEN 1 END),
          COUNT(CASE WHEN o.status_jam_kerja = 'Normal' AND o.kategori_jam_kerja = 'REGULER' THEN 1 END),
          
          -- POLA JAM KERJA SHIFT
          COUNT(CASE WHEN o.status_jam_kerja = 'Shift' THEN 1 END),
          COUNT(CASE WHEN o.jenis_jam_kerja_shift_daily_new = 'SHIFT PAGI' THEN 1 END),
          COUNT(CASE WHEN o.jenis_jam_kerja_shift_daily_new = 'SHIFT SIANG' THEN 1 END),
          COUNT(CASE WHEN o.jenis_jam_kerja_shift_daily_new = 'SHIFT MALAM' THEN 1 END),
          COUNT(CASE WHEN o.jenis_jam_kerja_shift_daily_new = 'SHIFT OFF' THEN 1 END),
          
          -- KETERANGAN ABSEN MASUK DAN PULANG
          COUNT(CASE WHEN o.status_in_out = 'ABSEN LENGKAP' THEN 1 END),
          COUNT(CASE WHEN o.status_in_out != 'ABSEN LENGKAP' THEN 1 END),
          COUNT(CASE WHEN o.status_in_out = 'IN & OUT KOSONG' THEN 1 END),
          COUNT(CASE WHEN o.status_in_out = 'IN KOSONG' THEN 1 END),
          COUNT(CASE WHEN o.status_in_out = 'OUT KOSONG' THEN 1 END),
          
          -- PENGAJUAN KETIDAKHADIRAN
          COUNT(CASE WHEN o.keterangan_kehadiran LIKE '%SPPD%' 
                     OR o.keterangan_kehadiran LIKE '%TUGAS LUAR%' 
                     OR o.keterangan_kehadiran LIKE '%DINAS LUAR%' THEN 1 END),
          COUNT(CASE WHEN o.keterangan_kehadiran LIKE '%CUTI%' 
                     OR o.keterangan_kehadiran LIKE '%IJIN%' 
                     OR o.keterangan_kehadiran LIKE '%SAKIT%' THEN 1 END),
          
          -- PEROLEHAN DURASI JAM KERJA
          COALESCE(SUM(CAST(o.jam_kerja_pegawai_cleansing AS DECIMAL(10,2))), 0.00),
          COALESCE(SUM(CAST(o.jam_kerja_seharusnya AS DECIMAL(10,2))), 0.00),
          CASE 
            WHEN SUM(CAST(o.jam_kerja_seharusnya AS DECIMAL(10,2))) > 0 
            THEN ROUND((SUM(CAST(o.jam_kerja_pegawai_cleansing AS DECIMAL(10,2))) / 
                       SUM(CAST(o.jam_kerja_seharusnya AS DECIMAL(10,2)))) * 100, 2)
            ELSE 0.00 
          END
          
        FROM olah_absensi o
        LEFT JOIN data_pegawai dp ON o.perner = dp.perner
        WHERE o.tanggal IS NOT NULL
          AND dp.perner IS NOT NULL
          ${generateSearchCondition}
        GROUP BY o.perner, dp.nama, dp.bidang
        ORDER BY o.perner ASC
      `;

      conn.query(insertSQL, generateSearchParams, (insertErr, insertResult) => {
        if (insertErr) {
          console.error("❌ Error inserting rekap data:", insertErr.message);
          return res.status(500).json({
            success: false,
            message: "❌ Gagal generate rekap absensi",
            error: insertErr.message,
          });
        }

        const recordsGenerated = insertResult.affectedRows;
        console.log(
          `✅ Rekap generation completed: ${recordsGenerated} records inserted`
        );

        // Step 4: Get summary statistics
        const statsSQL = `
          SELECT 
            COUNT(*) as total_pegawai,
            ROUND(AVG(PERSENTASE_JKP), 2) as avg_persentase,
            ROUND(SUM(JAM_REALISASI), 1) as total_jam_realisasi,
            ROUND(SUM(JAM_SEHARUSNYA), 1) as total_jam_seharusnya
          FROM rekap_absensi
        `;

        conn.query(statsSQL, (statsErr, statsResults) => {
          if (statsErr) {
            console.error("❌ Error getting stats:", statsErr.message);
            // Still return success but without detailed stats
            const endTime = Date.now();
            const duration = endTime - startTime;

            return res.json({
              success: true,
              message: `✅ Rekap absensi berhasil dibuat (${recordsGenerated} pegawai)`,
              records_generated: recordsGenerated,
              duration_ms: duration,
              stats: null,
            });
          }

          const endTime = Date.now();
          const duration = endTime - startTime;

          const stats = statsResults[0];
          console.log(`📊 Generation summary:`, {
            total_pegawai: stats.total_pegawai,
            avg_persentase: `${stats.avg_persentase}%`,
            total_jam_realisasi: stats.total_jam_realisasi,
            total_jam_seharusnya: stats.total_jam_seharusnya,
            duration: `${duration}ms`,
            search_applied:
              search.trim() !== ""
                ? `"${search}" in ${searchFields}`
                : "No filter",
          });

          res.json({
            success: true,
            message: `✅ Rekap absensi berhasil dibuat untuk ${recordsGenerated} pegawai`,
            records_generated: recordsGenerated,
            duration_ms: duration,
            stats: {
              total_pegawai: stats.total_pegawai,
              avg_persentase: stats.avg_persentase,
              total_jam_realisasi: stats.total_jam_realisasi,
              total_jam_seharusnya: stats.total_jam_seharusnya,
            },
            performance: {
              started_at: new Date(startTime).toISOString(),
              completed_at: new Date().toISOString(),
              duration_ms: duration,
            },
          });
        });
      });
    });
  });
});

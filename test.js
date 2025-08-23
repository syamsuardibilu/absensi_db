app.get("/get-rekap-absensi", (req, res) => {
  const startTime = Date.now();
  console.log("🚀 Fetching rekap_absensi data...");

  // Parse query parameters
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  const search = req.query.search || "";
  const searchFields = req.query.search_fields || "PERNER,NIP,NAMA,BIDANG_UNIT";

  console.log("📊 Query params:", {
    limit,
    offset,
    search,
    searchFields: searchFields.split(","),
  });

  // Build search condition - Updated untuk include NIP dari data_pegawai
  let searchCondition = "";
  let searchParams = [];

  if (search.trim()) {
    const fields = searchFields.split(",").map(f => f.trim());
    const validFields = ["PERNER", "NIP", "NAMA", "BIDANG_UNIT"];
    const searchableFields = fields.filter(f => validFields.includes(f));
    
    if (searchableFields.length > 0) {
      const conditions = searchableFields.map(field => {
        // Map field names to correct table references
        switch(field) {
          case "PERNER": return "r.PERNER LIKE ?";
          case "NIP": return "dp.nip LIKE ?";
          case "NAMA": return "r.NAMA LIKE ?";
          case "BIDANG_UNIT": return "r.BIDANG_UNIT LIKE ?";
          default: return null;
        }
      }).filter(Boolean);
      
      if (conditions.length > 0) {
        searchCondition = `WHERE (${conditions.join(" OR ")})`;
        
        // Add search parameter for each valid field
        searchableFields.forEach(() => {
          searchParams.push(`%${search}%`);
        });
      }
    }
  }

  // Step 1: Get total count for pagination with JOIN to data_pegawai
  const countSQL = `
    SELECT COUNT(*) as total 
    FROM rekap_absensi r
    LEFT JOIN data_pegawai dp ON r.PERNER = dp.perner
    ${searchCondition}
  `;

  conn.query(countSQL, searchParams, (countErr, countResults) => {
    if (countErr) {
      console.error("❌ Error getting count:", countErr.message);
      return res.status(500).json({
        success: false,
        message: "❌ Gagal menghitung total data",
        error: countErr.message,
      });
    }

    const totalRecords = countResults[0].total;
    const totalPages = Math.ceil(totalRecords / limit);

    console.log(`📈 Total records: ${totalRecords}, Total pages: ${totalPages}`);

    // Step 2: Get paginated data with JOIN to get NIP from data_pegawai
    const dataSQL = `
      SELECT 
        r.PERNER,
        r.NAMA,
        r.BIDANG_UNIT,
        dp.nip as NIP,
        r.TOTAL_HARI_REGULER,
        r.HARI_KERJA_REGULER,
        r.HARI_LIBUR_REGULER,
        r.TOTAL_HARI_REALISASI,
        r.HARI_KERJA_REALISASI,
        r.HARI_LIBUR_REALISASI,
        r.TOTAL_HARI_TANPA_KOREKSI,
        r.TOTAL_HARI_KOREKSI,
        r.KOREKSI_IN,
        r.KOREKSI_OUT,
        r.KOREKSI_IN_OUT,
        r.TOTAL_JAM_KERJA_NORMAL,
        r.PIKET,
        r.PDKB,
        r.REGULER,
        r.TOTAL_JAM_KERJA_SHIFT,
        r.SHIFT_PAGI,
        r.SHIFT_SIANG,
        r.SHIFT_MALAM,
        r.SHIFT_OFF,
        r.ABSEN_LENGKAP,
        r.ABSEN_TIDAK_LENGKAP,
        r.IN_OUT_KOSONG,
        r.IN_KOSONG,
        r.OUT_KOSONG,
        r.SPPD_TUGAS_LUAR_DLL,
        r.CUTI_IJIN,
        r.JAM_REALISASI,
        r.JAM_SEHARUSNYA,
        r.PERSENTASE_JKP,
        r.RESULT_BLASTING
      FROM rekap_absensi r
      LEFT JOIN data_pegawai dp ON r.PERNER = dp.perner
      ${searchCondition}
      ORDER BY r.PERNER ASC
      LIMIT ? OFFSET ?
    `;

    const dataParams = [...searchParams, limit, offset];

    conn.query(dataSQL, dataParams, (dataErr, dataResults) => {
      if (dataErr) {
        console.error("❌ Error fetching data:", dataErr.message);
        return res.status(500).json({
          success: false,
          message: "❌ Gagal mengambil data rekap absensi",
          error: dataErr.message,
        });
      }

      const endTime = Date.now();
      const duration = endTime - startTime;

      console.log(`✅ Data fetched successfully!`);
      console.log(`   - Records returned: ${dataResults.length}`);
      console.log(`   - Total available: ${totalRecords}`);
      console.log(`   - Page: ${Math.floor(offset / limit) + 1}/${totalPages}`);
      console.log(`   - Duration: ${duration}ms`);

      if (search.trim()) {
        console.log(`🔍 Search applied: "${search}" in fields: ${searchFields} (including NIP from data_pegawai)`);
      }

      // Step 3: Calculate statistics from current page data
      let stats = {
        total_pegawai: dataResults.length,
        total_jam_realisasi: 0,
        total_jam_seharusnya: 0,
        avg_persentase: 0,
      };

      if (dataResults.length > 0) {
        let totalPersentase = 0;
        
        dataResults.forEach(row => {
          stats.total_jam_realisasi += parseFloat(row.JAM_REALISASI || 0);
          stats.total_jam_seharusnya += parseFloat(row.JAM_SEHARUSNYA || 0);
          totalPersentase += parseFloat(row.PERSENTASE_JKP || 0);
        });

        stats.avg_persentase = parseFloat((totalPersentase / dataResults.length).toFixed(2));
      }

      // Response with updated structure
      res.json({
        success: true,
        message: `✅ Data rekap absensi berhasil dimuat (${dataResults.length} records)`,
        data: dataResults,
        stats: stats,
        pagination: {
          total: totalRecords,
          limit: limit,
          offset: offset,
          totalPages: totalPages,
          currentPage: Math.floor(offset / limit) + 1,
          hasNext: offset + limit < totalRecords,
          hasPrev: offset > 0,
        },
        search: {
          query: search,
          fields: searchFields,
          applied: search.trim() !== "",
        },
        performance: {
          duration_ms: duration,
          query_time: new Date().toISOString(),
        },
      });
    });
  });
});

// Generate Rekap Absensi endpoint - HANYA SATU ENDPOINT
app.post("/generate-rekap-absensi", (req, res) => {
  const startTime = Date.now();
  console.log("🚀 Starting rekap absensi generation...");

  // Parse query parameters for optional search filtering during generation
  const search = req.query.search || "";
  const searchFields = req.query.search_fields || "PERNER,NIP,NAMA,BIDANG_UNIT";

  // Build search condition for generate process (optional filtering)
  let generateSearchCondition = "";
  let generateSearchParams = [];

  if (search.trim()) {
    const fields = searchFields.split(",").map(f => f.trim());
    const validFields = ["PERNER", "NIP", "NAMA", "BIDANG_UNIT"];
    const searchableFields = fields.filter(f => validFields.includes(f));
    
    if (searchableFields.length > 0) {
      const conditions = searchableFields.map(field => {
        switch(field) {
          case "PERNER": return "o.perner LIKE ?";
          case "NIP": return "dp.nip LIKE ?";
          case "NAMA": return "dp.nama LIKE ?";
          case "BIDANG_UNIT": return "dp.bidang LIKE ?";
          default: return null;
        }
      }).filter(Boolean);
      
      if (conditions.length > 0) {
        generateSearchCondition = `AND (${conditions.join(" OR ")})`;
        
        searchableFields.forEach(() => {
          generateSearchParams.push(`%${search}%`);
        });
      }
    }
  }

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
        console.log(`✅ Rekap generation completed: ${recordsGenerated} records inserted`);

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
            search_applied: search.trim() !== "" ? `"${search}" in ${searchFields}` : "No filter"
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
            search: {
              query: search,
              fields: searchFields,
              applied: search.trim() !== "",
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
        console.log(`✅ Rekap generation completed: ${recordsGenerated} records inserted`);

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
            search_applied: search.trim() !== "" ? `"${search}" in ${searchFields}` : "No filter"
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

// Updated endpoint untuk get rekap dengan daily details yang sesuai struktur baru
app.get("/getRekapAbsensiWithDaily", (req, res) => {
  const startTime = Date.now();
  console.log("🚀 Fetching rekap_absensi with daily details...");

  // Parse query parameters
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;
  const search = req.query.search || "";
  const searchFields = req.query.search_fields || "PERNER,NIP,NAMA,BIDANG_UNIT";

  // Step 1: Ambil periode bulan dari olah_absensi
  const periodeSQL = `
    SELECT tanggal 
    FROM olah_absensi 
    WHERE tanggal IS NOT NULL 
    ORDER BY tanggal DESC 
    LIMIT 1
  `;

  conn.query(periodeSQL, (periodeErr, periodeResults) => {
    if (periodeErr) {
      console.error("❌ Error getting periode:", periodeErr);
      return res.status(500).json({
        success: false,
        message: "❌ Gagal mengambil periode bulan absensi",
        error: periodeErr.message,
      });
    }

    // Determine periode bulan dan tahun
    let periodeBulan = "Tidak Diketahui";
    let periodeTahun = new Date().getFullYear();
    let startDate = null;
    let endDate = null;

    if (periodeResults.length > 0 && periodeResults[0].tanggal) {
      const tanggalSample = new Date(periodeResults[0].tanggal);
      const monthNames = [
        "Januari", "Februari", "Maret", "April", "Mei", "Juni",
        "Juli", "Agustus", "September", "Oktober", "November", "Desember",
      ];
      periodeBulan = monthNames[tanggalSample.getMonth()];
      periodeTahun = tanggalSample.getFullYear();

      // Set date range for the month
      startDate = new Date(periodeTahun, tanggalSample.getMonth(), 1);
      endDate = new Date(periodeTahun, tanggalSample.getMonth() + 1, 0);
    }

    console.log(`📅 Periode absensi: ${periodeBulan} ${periodeTahun}`);

    // Build search condition for rekap_absensi with JOIN to data_pegawai
    let searchCondition = "";
    let searchParams = [];

    if (search.trim()) {
      const fields = searchFields.split(",").map(f => f.trim());
      const validFields = ["PERNER", "NIP", "NAMA", "BIDANG_UNIT"];
      const searchableFields = fields.filter(f => validFields.includes(f));
      
      if (searchableFields.length > 0) {
        const conditions = searchableFields.map(field => {
          switch(field) {
            case "PERNER": return "r.PERNER LIKE ?";
            case "NIP": return "dp.nip LIKE ?";
            case "NAMA": return "r.NAMA LIKE ?";
            case "BIDANG_UNIT": return "r.BIDANG_UNIT LIKE ?";
            default: return null;
          }
        }).filter(Boolean);
        
        if (conditions.length > 0) {
          searchCondition = `AND (${conditions.join(" OR ")})`;
          
          searchableFields.forEach(() => {
            searchParams.push(`%${search}%`);
          });
        }
      }
    }

    // Step 2: Query dengan 3-way JOIN + daily details - Updated struktur
    const sql = `
      SELECT 
        -- Summary data dari rekap_absensi (struktur baru) dengan NIP dari data_pegawai
        r.PERNER,
        r.NAMA, r.BIDANG_UNIT,
        dp.nip as NIP,
        r.TOTAL_HARI_REGULER, r.HARI_KERJA_REGULER, r.HARI_LIBUR_REGULER,
        r.TOTAL_HARI_REALISASI, r.HARI_KERJA_REALISASI, r.HARI_LIBUR_REALISASI,
        r.TOTAL_HARI_TANPA_KOREKSI, r.TOTAL_HARI_KOREKSI,
        r.KOREKSI_IN, r.KOREKSI_OUT, r.KOREKSI_IN_OUT,
        r.TOTAL_JAM_KERJA_NORMAL, r.PIKET, r.PDKB, r.REGULER,
        r.TOTAL_JAM_KERJA_SHIFT, r.SHIFT_PAGI, r.SHIFT_SIANG, r.SHIFT_MALAM, r.SHIFT_OFF,
        r.ABSEN_LENGKAP, r.ABSEN_TIDAK_LENGKAP, r.IN_OUT_KOSONG, r.IN_KOSONG, r.OUT_KOSONG,
        r.SPPD_TUGAS_LUAR_DLL, r.CUTI_IJIN,
        r.JAM_REALISASI, r.JAM_SEHARUSNYA, r.PERSENTASE_JKP,
        r.RESULT_BLASTING,
        
        -- Data pegawai (NIP sudah diambil di atas)
        dp.no_telp,
        
        -- Daily details dari olah_absensi
        od.tanggal, od.nama_hari, od.jenis_hari, od.status_jam_kerja,
        od.daily_in_cleansing, od.daily_out_cleansing,
        od.correction_in, od.correction_out,
        od.value_att_abs, od.status_absen, od.status_in_out,
        od.jam_kerja_pegawai_cleansing, od.jam_kerja_seharusnya
        
      FROM rekap_absensi r
      LEFT JOIN data_pegawai dp ON r.PERNER = dp.perner
      LEFT JOIN olah_absensi od ON r.PERNER = od.perner
      WHERE od.tanggal IS NOT NULL
        ${searchCondition}
        ${
          startDate && endDate
            ? `AND od.tanggal BETWEEN '${startDate
                .toISOString()
                .slice(0, 10)}' AND '${endDate.toISOString().slice(0, 10)}'`
            : ""
        }
      ORDER BY r.PERNER, od.tanggal ASC
      LIMIT ${limit} OFFSET ${offset}
    `;

    conn.query(sql, searchParams, (err, results) => {
      if (err) {
        console.error("❌ Error fetching data with daily details:", err);
        return res.status(500).json({
          success: false,
          message: "❌ Gagal mengambil data rekap dengan daily details",
          error: err.message,
        });
      }

      const endTime = Date.now();
      const duration = endTime - startTime;

      console.log(
        `✅ Data with daily details fetched: ${results.length} records in ${duration}ms`
      );

      // Step 3: Group data by PERNER (summary + daily_details array)
      const groupedData = {};

      results.forEach((row) => {
        const perner = row.PERNER;

        // Initialize summary data (only once per PERNER)
        if (!groupedData[perner]) {
          groupedData[perner] = {
            // Summary fields - Updated struktur dengan NIP
            PERNER: row.PERNER,
            nama: row.NAMA,
            nip: row.NIP,
            bidang: row.BIDANG_UNIT,
            no_telp: row.no_telp,
            periode_bulan: periodeBulan,
            periode_tahun: periodeTahun,
            
            // Jumlah Hari Reguler
            TOTAL_HARI_REGULER: parseInt(row.TOTAL_HARI_REGULER) || 0,
            HARI_KERJA_REGULER: parseInt(row.HARI_KERJA_REGULER) || 0,
            HARI_LIBUR_REGULER: parseInt(row.HARI_LIBUR_REGULER) || 0,
            
            // Jumlah Hari Realisasi
            TOTAL_HARI_REALISASI: parseInt(row.TOTAL_HARI_REALISASI) || 0,
            HARI_KERJA_REALISASI: parseInt(row.HARI_KERJA_REALISASI) || 0,
            HARI_LIBUR_REALISASI: parseInt(row.HARI_LIBUR_REALISASI) || 0,
            
            // Data Koreksi
            TOTAL_HARI_TANPA_KOREKSI: parseInt(row.TOTAL_HARI_TANPA_KOREKSI) || 0,
            TOTAL_HARI_KOREKSI: parseInt(row.TOTAL_HARI_KOREKSI) || 0,
            KOREKSI_IN: parseInt(row.KOREKSI_IN) || 0,
            KOREKSI_OUT: parseInt(row.KOREKSI_OUT) || 0,
            KOREKSI_IN_OUT: parseInt(row.KOREKSI_IN_OUT) || 0,
            
            // Pola Jam Kerja Normal
            TOTAL_JAM_KERJA_NORMAL: parseInt(row.TOTAL_JAM_KERJA_NORMAL) || 0,
            PIKET: parseInt(row.PIKET) || 0,
            PDKB: parseInt(row.PDKB) || 0,
            REGULER: parseInt(row.REGULER) || 0,
            
            // Pola Jam Kerja Shift
            TOTAL_JAM_KERJA_SHIFT: parseInt(row.TOTAL_JAM_KERJA_SHIFT) || 0,
            SHIFT_PAGI: parseInt(row.SHIFT_PAGI) || 0,
            SHIFT_SIANG: parseInt(row.SHIFT_SIANG) || 0,
            SHIFT_MALAM: parseInt(row.SHIFT_MALAM) || 0,
            SHIFT_OFF: parseInt(row.SHIFT_OFF) || 0,
            
            // Keterangan Absen
            ABSEN_LENGKAP: parseInt(row.ABSEN_LENGKAP) || 0,
            ABSEN_TIDAK_LENGKAP: parseInt(row.ABSEN_TIDAK_LENGKAP) || 0,
            IN_OUT_KOSONG: parseInt(row.IN_OUT_KOSONG) || 0,
            IN_KOSONG: parseInt(row.IN_KOSONG) || 0,
            OUT_KOSONG: parseInt(row.OUT_KOSONG) || 0,
            
            // Pengajuan Ketidakhadiran
            SPPD_TUGAS_LUAR_DLL: parseInt(row.SPPD_TUGAS_LUAR_DLL) || 0,
            CUTI_IJIN: parseInt(row.CUTI_IJIN) || 0,
            
            // Durasi Jam Kerja
            JAM_REALISASI: parseFloat(row.JAM_REALISASI) || 0.0,
            JAM_SEHARUSNYA: parseFloat(row.JAM_SEHARUSNYA) || 0.0,
            PERSENTASE_JKP: parseFloat(row.PERSENTASE_JKP) || 0.0,
            
            RESULT_BLASTING: row.RESULT_BLASTING || null,

            // Daily details array
            daily_details: [],
          };
        }

        // Add daily detail if tanggal exists
        if (row.tanggal) {
          groupedData[perner].daily_details.push({
            tanggal: row.tanggal,
            nama_hari: row.nama_hari,
            jenis_hari: row.jenis_hari,
            status_jam_kerja: row.status_jam_kerja,
            daily_in_cleansing: row.daily_in_cleansing,
            daily_out_cleansing: row.daily_out_cleansing,
            correction_in: row.correction_in,
            correction_out: row.correction_out,
            value_att_abs: row.value_att_abs,
            status_absen: row.status_absen,
            status_in_out: row.status_in_out,
            jam_kerja_pegawai_cleansing:
              parseFloat(row.jam_kerja_pegawai_cleansing) || 0.0,
            jam_kerja_seharusnya: parseFloat(row.jam_kerja_seharusnya) || 0.0,
          });
        }
      });

      // Convert to array format
      const finalResults = Object.values(groupedData);

      // Debug statistics
      if (finalResults.length > 0) {
        const sampleData = finalResults[0];
        const totalDailyRecords = finalResults.reduce(
          (sum, item) => sum + item.daily_details.length,
          0
        );

        console.log(`📊 Grouping Statistics:`);
        console.log(`   - Total PERNER: ${finalResults.length}`);
        console.log(`   - Total daily records: ${totalDailyRecords}`);
        console.log(
          `   - Sample daily details count: ${sampleData.daily_details.length}`
        );
        console.log(
          `   - Sample PERNER: ${sampleData.PERNER} (${sampleData.nama})`
        );
      }

      res.json({
        success: true,
        message: `✅ Data rekap dengan daily details berhasil dimuat (${finalResults.length} pegawai)`,
        data: finalResults,
        periode: {
          bulan: periodeBulan,
          tahun: periodeTahun,
          range: startDate && endDate ? {
            start: startDate.toISOString().slice(0, 10),
            end: endDate.toISOString().slice(0, 10)
          } : null
        },
        pagination: {
          limit: limit,
          offset: offset,
          returned: finalResults.length,
        },
        search: {
          query: search,
          fields: searchFields,
          applied: search.trim() !== "",
        },
        performance: {
          duration_ms: duration,
          query_time: new Date().toISOString(),
        },
      });
    });
  });
});
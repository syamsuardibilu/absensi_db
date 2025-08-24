app.get("/getRekapAbsensiWithDaily", (req, res) => {
  const startTime = Date.now();
  console.log("🚀 Fetching rekap_absensi with daily details...");

  // Util: format YYYY-MM-DD tanpa risiko offset timezone
  const fmtDate = (d) => {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  };

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

    // Tentukan periode bulan & tahun + rentang tanggal
    let periodeBulan = "Tidak Diketahui";
    let periodeTahun = new Date().getFullYear();
    let startDate = null;
    let endDate = null;

    if (periodeResults.length > 0 && periodeResults[0].tanggal) {
      const tanggalSample = new Date(periodeResults[0].tanggal);
      const monthNames = [
        "Januari",
        "Februari",
        "Maret",
        "April",
        "Mei",
        "Juni",
        "Juli",
        "Agustus",
        "September",
        "Oktober",
        "November",
        "Desember",
      ];
      periodeBulan = monthNames[tanggalSample.getMonth()];
      periodeTahun = tanggalSample.getFullYear();

      // 1st day of target month .. last day of (target month + 1)
      startDate = new Date(periodeTahun, tanggalSample.getMonth(), 1);
      endDate = new Date(periodeTahun, tanggalSample.getMonth() + 2, 0);
    }

    console.log(`📅 Periode absensi: ${periodeBulan} ${periodeTahun}`);

    // === Query param pencarian (lebih ketat) ===
    const q = (req.query.q || "").trim();
    const rawTerms = q.split(/\s+/).filter(Boolean);
    const params = [];

    // Clause tanggal
    let dateClause = "";
    if (startDate && endDate) {
      dateClause = "AND od.tanggal BETWEEN ? AND ?";
      params.push(fmtDate(startDate), fmtDate(endDate));
    }

    // Build clause pencarian ketat:
    // - digit: ke PERNER/NIP prefix (dan exact utk PERNER jika panjang ≥ 8)
    // - teks: match di awal kata (awal string atau setelah spasi) di NAMA/BIDANG
    let searchClause = "";
    if (rawTerms.length > 0) {
      const perTermClauses = [];

      rawTerms.forEach((term) => {
        const isNumeric = /^\d+$/.test(term);
        if (isNumeric) {
          // Angka → PERNER/NIP prefix; PERNER exact jika panjang >= 8
          const pieces = [];
          if (term.length >= 8) {
            pieces.push("r.PERNER = ?");
            params.push(term);
          }
          // Prefix (gunakan index)
          pieces.push("r.PERNER LIKE CONCAT(?, '%')");
          pieces.push("dp.nip LIKE CONCAT(?, '%')");
          params.push(term, term);

          perTermClauses.push(`(${pieces.join(" OR ")})`);
        } else {
          // Teks → awal kata pada nama/bidang
          // Pola: LIKE 'term%' (awal string) ATAU LIKE '% term%' (awal kata setelah spasi)
          const pieces = [
            "dp.nama   LIKE CONCAT(?, '%')",
            "dp.nama   LIKE CONCAT('% ', ?, '%')",
            "dp.bidang LIKE CONCAT(?, '%')",
            "dp.bidang LIKE CONCAT('% ', ?, '%')",
          ];
          params.push(term, term, term, term);

          // Tambahkan juga prefix di NIP (alfanumerik) jika user ketik kode seperti '6989'
          pieces.push("dp.nip LIKE CONCAT(?, '%')");
          params.push(term);

          perTermClauses.push(`(${pieces.join(" OR ")})`);
        }
      });

      searchClause = "AND " + perTermClauses.join(" AND ");
    }

    // Step 2: Query dengan 3-way JOIN + daily details
    const sql = `
      SELECT 
        r.PERNER,
        dp.nama   AS nama,
        dp.nip    AS nip,
        dp.bidang AS bidang,
        dp.no_telp,
        r.BIDANG_UNIT,

        r.TOTAL_HARI_REGULER, r.HARI_KERJA_REGULER, r.HARI_LIBUR_REGULER,
        r.TOTAL_HARI_REALISASI, r.HARI_KERJA_REALISASI, r.HARI_LIBUR_REALISASI,
        r.TOTAL_HARI_TANPA_KOREKSI, r.TOTAL_HARI_KOREKSI, r.KOREKSI_IN, r.KOREKSI_OUT, r.KOREKSI_IN_OUT,
        r.TOTAL_JAM_KERJA_NORMAL, r.PIKET, r.PDKB, r.REGULER,
        r.TOTAL_JAM_KERJA_SHIFT, r.SHIFT_PAGI, r.SHIFT_SIANG, r.SHIFT_MALAM, r.SHIFT_OFF,
        r.ABSEN_LENGKAP, r.ABSEN_TIDAK_LENGKAP, r.IN_OUT_KOSONG, r.IN_KOSONG, r.OUT_KOSONG,
        r.SPPD_TUGAS_LUAR_DLL, r.CUTI_IJIN,
        r.JAM_REALISASI, r.JAM_SEHARUSNYA, r.PERSENTASE_JKP,
        r.RESULT_BLASTING,

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
        ${dateClause}
        ${searchClause}
      ORDER BY r.PERNER, od.tanggal ASC
    `;

    conn.query(sql, params, (err, results) => {
      if (err) {
        console.error("❌ Error fetching data with daily details:", err);
        return res.status(500).json({
          success: false,
          message: "❌ Gagal mengambil data rekap dengan daily details",
          error: err.message,
        });
      }

      const duration = Date.now() - startTime;
      console.log(
        `✅ Data with daily details fetched: ${results.length} records in ${duration}ms`
      );

      // Step 3: Group data by PERNER (summary + daily_details array)
      const groupedData = {};
      const to2 = (v) =>
        v == null
          ? "0.00"
          : typeof v === "number"
          ? v.toFixed(2)
          : Number(v).toFixed(2);

      results.forEach((row) => {
        const perner = row.PERNER;

        if (!groupedData[perner]) {
          groupedData[perner] = {
            PERNER: row.PERNER,
            nama: row.NAMA,
            nip: row.NIP,
            bidang: row.BIDANG,
            no_telp: row.no_telp,
            periode_bulan: periodeBulan,
            periode_tahun: periodeTahun,

            // Gunakan *_REALISASI agar tidak 0
            TOTAL_HARI: parseInt(row.TOTAL_HARI_REALISASI) || 0,
            HARI_KERJA: parseInt(row.HARI_KERJA_REALISASI) || 0,
            HARI_LIBUR: parseInt(row.HARI_LIBUR_REALISASI) || 0,

            TOTAL_HARI_KOREKSI: parseInt(row.TOTAL_HARI_KOREKSI) || 0,
            KOREKSI_IN: parseInt(row.KOREKSI_IN) || 0,
            KOREKSI_OUT: parseInt(row.KOREKSI_OUT) || 0,
            KOREKSI_IN_OUT: parseInt(row.KOREKSI_IN_OUT) || 0,

            TOTAL_JAM_KERJA_NORMAL: parseFloat(row.TOTAL_JAM_KERJA_NORMAL) || 0,
            PIKET: parseInt(row.PIKET) || 0,
            PDKB: parseInt(row.PDKB) || 0,
            REGULER: parseInt(row.REGULER) || 0,

            TOTAL_JAM_KERJA_SHIFT: parseFloat(row.TOTAL_JAM_KERJA_SHIFT) || 0,
            SHIFT_PAGI: parseInt(row.SHIFT_PAGI) || 0,
            SHIFT_SIANG: parseInt(row.SHIFT_SIANG) || 0,
            SHIFT_MALAM: parseInt(row.SHIFT_MALAM) || 0,
            SHIFT_OFF: parseInt(row.SHIFT_OFF) || 0,

            ABSEN_LENGKAP: parseInt(row.ABSEN_LENGKAP) || 0,
            ABSEN_TIDAK_LENGKAP: parseInt(row.ABSEN_TIDAK_LENGKAP) || 0,
            IN_KOSONG: parseInt(row.IN_KOSONG) || 0,
            OUT_KOSONG: parseInt(row.OUT_KOSONG) || 0,
            SPPD_TUGAS_LUAR_DLL: parseInt(row.SPPD_TUGAS_LUAR_DLL) || 0,
            CUTI_IJIN: parseInt(row.CUTI_IJIN) || 0,

            JAM_REALISASI: parseFloat(row.JAM_REALISASI) || 0.0,
            JAM_SEHARUSNYA: parseFloat(row.JAM_SEHARUSNYA) || 0.0,

            // String 2 desimal, sama seperti DB
            PERSENTASE_JKP: to2(row.PERSENTASE_JKP),

            RESULT_BLASTING: row.RESULT_BLASTING ?? null,

            daily_details: [],
          };
        }

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

      const finalResults = Object.values(groupedData);

      // Debug
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

      res.json(finalResults);
    });
  });
});

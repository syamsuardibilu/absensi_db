function formatTanggal(tanggalStr) {
  if (!tanggalStr) return "-";
  const d = new Date(tanggalStr);
  if (isNaN(d.getTime())) return "-";
  d.setDate(d.getDate());
  return d.toISOString().split("T")[0];
}

function selisihDetik(jamAkhir, jamAwal) {
  const [ha, ma, sa] = jamAkhir.split(":").map(Number);
  const [hb, mb, sb] = jamAwal.split(":").map(Number);
  const totalA = ha * 3600 + ma * 60 + sa;
  const totalB = hb * 3600 + mb * 60 + sb;
  return totalA - totalB;
}

function parseWSRule(ws_rule) {
  let jms = "-",
    jps = "-";
  if (ws_rule && typeof ws_rule === "string" && ws_rule.includes("~")) {
    [jms, jps] = ws_rule.split("~").map((x) => x.trim().replace(/\./g, ":"));
    if (jms.length === 5) jms += ":00";
    if (jps.length === 5) jps += ":00";
    ws_rule = `${jms}~${jps}`;
  }
  // Tambahkan ketentuan untuk jpr
  if (jps === "00:00:00") jps = "24:00:00";
  return { ws_rule, jms, jps };
}

function generateLogJKPDetail(
  row,
  {
    jms = "-",
    jps = "-",
    jmr = "-",
    jpr = "-",
    jmup = "-",
    jpup = "-",
    durasi = 0,
    ws_rule = "-",
    ket = "JKP",
    daily_in_cleansing_c = "-",
    daily_out_cleansing_c = "-",
    durasi_cleansing_c = 0,
    // Parameter tambahan untuk shift
    jmup_cleansing = "-",
    jpup_cleansing = "-",
  }
) {
  const formatDurasi = (dur) => {
    return isNaN(dur) ? "NaN" : Number(dur).toFixed(3);
  };

  const formatJam = (jam) => {
    return jam && jam !== "-" ? jam : "-";
  };

  return [
    `Perner           : ${row.perner}`,
    `Tanggal          : ${formatTanggal(row.tanggal)}`,
    `Jenis Hari       : ${row.jenis_hari || "-"}`,
    `Is Jumat         : ${row.is_jumat || "-"}`,
    `Daily In         : ${formatJam(row.daily_in)}`,
    `Daily Out        : ${formatJam(row.daily_out)}`,
    `WS Rule          : ${ws_rule}`,
    `att_daily_new    : ${row.att_daily_new || "-"}`,
    `abs_daily_new    : ${row.abs_daily_new || "-"}`,
    `att_sap_new      : ${row.att_sap_new || "-"}`,
    `abs_sap_new      : ${row.abs_sap_new || "-"}`,
    `sppd_umum_new    : ${row.sppd_umum_new || "-"}`,
    `shift_daily_new  : ${row.jenis_jam_kerja_shift_daily_new || "-"}`,
    `shift_sap_new    : ${row.jenis_jam_kerja_shift_sap_new || "-"}`,
    `is_shift_daily_sap: ${row.is_shift_daily_sap || "-"}`,
    `value_shift_daily_sap: ${row.value_shift_daily_sap || "-"}`,
    ``,
    `=== JAM KERJA RAW ===`,
    `JMS (jadwal)     : ${formatJam(jms)}`,
    `JPS (jadwal)     : ${formatJam(jps)}`,
    `JMR (real)       : ${formatJam(jmr)}`,
    `JPR (real)       : ${formatJam(jpr)}`,
    `JMUP (pakai)     : ${formatJam(jmup)}`,
    `JPUP (pakai)     : ${formatJam(jpup)}`,
    `JKP RAW (jam)    : ${formatDurasi(durasi)}`,
    ``,
    `=== JAM KERJA CLEANSING ===`,
    `JMR Cleansing    : ${formatJam(daily_in_cleansing_c)}`,
    `JPR Cleansing    : ${formatJam(daily_out_cleansing_c)}`,
    `JMUP Cleansing   : ${formatJam(jmup_cleansing)}`,
    `JPUP Cleansing   : ${formatJam(jpup_cleansing)}`,
    `JKP Cleansing    : ${formatDurasi(durasi_cleansing_c)}`,
    ``,
    `Keterangan       : ${ket}`,
    `==============================`,
    ``,
  ].join("\n");
}

function jamDalamRentang(jamStr, batasAwal, batasAkhir) {
  try {
    // Validasi input
    if (!jamStr || !batasAwal || !batasAkhir) {
      // console.warn("⚠️ jamDalamRentang: parameter kosong", {
      //   jamStr,
      //   batasAwal,
      //   batasAkhir,
      // });
      return false;
    }

    // Fungsi untuk convert jam ke detik
    const toDetik = (jam) => {
      try {
        const parts = jam.split(":");
        if (parts.length < 2) return 0;

        const h = parseInt(parts[0]) || 0;
        const m = parseInt(parts[1]) || 0;
        const s = parseInt(parts[2]) || 0;

        return h * 3600 + m * 60 + s;
      } catch (error) {
        console.warn("⚠️ Error parsing jam:", jam, error.message);
        return 0;
      }
    };

    const jamDetik = toDetik(jamStr);
    const awalDetik = toDetik(batasAwal);
    const akhirDetik = toDetik(batasAkhir);

    // console.log(
    //   `🔍 jamDalamRentang: ${jamStr}(${jamDetik}) dalam ${batasAwal}(${awalDetik}) - ${batasAkhir}(${akhirDetik})`
    // );

    const result = jamDetik >= awalDetik && jamDetik <= akhirDetik;
    // console.log(`✅ Hasil: ${result}`);

    return result;
  } catch (error) {
    console.error("❌ Error dalam jamDalamRentang:", error.message);
    return false;
  }
}

function JKPHariLibur(row) {
  const jmr = row.daily_in || "-";
  const jpr = row.daily_out || "-";

  const isJumat = row.is_jumat?.toString().toLowerCase() === "jumat";

  const durasi = 0;
  const durasi_cleansing_c = 0; // tetap 0, karena memang tidak dihitung
  const ws_rule = "-";
  const jmup = "-";
  const jpup = "-";
  const jms = "-";
  const jps = "-";
  const diMenit = "-";
  const ket = "JKP Hari Libur";
  const ket_in_out = "tidak diperhitungkan";
  const durasi_seharusnya = 0;

  // 💡 fallback ke nilai asli, supaya tetap bisa disimpan di database
  const daily_in_cleansing_c = jmr;
  const daily_out_cleansing_c = jpr;

  row.__logJKP = generateLogJKPDetail(row, {
    jms,
    jps,
    jmr,
    jpr,
    jmup,
    jpup,
    durasi,
    ws_rule,
    ket,
    ket_in_out,
    durasi_seharusnya,
    daily_in_cleansing_c,
    daily_out_cleansing_c,
    durasi_cleansing_c,
  });

  return {
    durasi,
    jmr,
    jpr,
    jms,
    jps,
    jmup,
    jpup,
    diMenit,
    ket,
    ket_in_out,
    durasi_seharusnya,
    daily_in_cleansing_c,
    daily_out_cleansing_c,
    durasi_cleansing_c,
  };
}

function hitungJKPAbs(row) {
  // Ambil nilai pertama yang tidak kosong dari abs_daily_new atau abs_sap_new
  const valABS =
    [row.abs_daily_new, row.abs_sap_new].find((s) => s && s.trim() !== "") ||
    "";

  const jmr = row.daily_in || "-";
  const jpr = row.daily_out || "-";

  const ws_rule_raw = row.ws_rule || "08.00~16.30";
  const isJumat = row.is_jumat?.toString().toLowerCase() === "jumat";

  let jmup = "-";
  let jpup = "-";
  let jms = "-";
  let jps = "-";
  let diMenit = "-";

  const durasi = 0;
  const durasi_cleansing_c = 0;

  const wsParsed = parseWSRule(ws_rule_raw);
  const ws_rule = wsParsed.ws_rule;
  jms = wsParsed.jms;
  jps = wsParsed.jps;

  const ket = "JKP ABS";
  const ket_in_out = "tidak diperhitungkan";
  const durasi_seharusnya =
    row.jenis_hari && row.jenis_hari.toUpperCase().includes("LIBUR") ? 0 : 8;

  // 💡 fallback cleansing = nilai asli
  const daily_in_cleansing_c = jmr;
  const daily_out_cleansing_c = jpr;

  row.__logJKP = generateLogJKPDetail(row, {
    jms,
    jps,
    jmr,
    jpr,
    jmup,
    jpup,
    durasi,
    ws_rule,
    ket,
    ket_in_out,
    durasi_seharusnya,
    daily_in_cleansing_c,
    daily_out_cleansing_c,
    durasi_cleansing_c,
  });

  return {
    durasi,
    jmr,
    jpr,
    jms,
    jps,
    jmup,
    jpup,
    diMenit,
    ket,
    ket_in_out,
    durasi_seharusnya,
    daily_in_cleansing_c,
    daily_out_cleansing_c,
    durasi_cleansing_c,
  };
}

function hitungJKPAtt(row) {
  const jmr = row.daily_in || "-";
  const jpr = row.daily_out || "-";

  const ws_rule_raw = row.ws_rule || "08.00~16.30";
  const isJumat = row.is_jumat?.toString().toLowerCase() === "jumat";

  // Parsing ws_rule
  const wsParsed = parseWSRule(ws_rule_raw);
  const ws_rule = wsParsed.ws_rule;
  const jms = wsParsed.jms;
  const jps = wsParsed.jps;

  // Nilai default
  const durasi = 8;
  const durasi_cleansing_c = 8;
  const jmup = "-";
  const jpup = "-";
  const diMenit = "-";
  const ket = "JKP Att";
  const ket_in_out = "tidak diperhitungkan";
  const durasi_seharusnya = 8;

  // ✅ Cleansing fallback = daily_in & daily_out
  const daily_in_cleansing_c = jmr;
  const daily_out_cleansing_c = jpr;

  // Generate log
  row.__logJKP = generateLogJKPDetail(row, {
    jms,
    jps,
    jmr,
    jpr,
    jmup,
    jpup,
    durasi,
    ws_rule,
    ket,
    ket_in_out,
    durasi_seharusnya,
    daily_in_cleansing_c,
    daily_out_cleansing_c,
    durasi_cleansing_c,
  });

  return {
    durasi,
    jmr,
    jpr,
    jms,
    jps,
    jmup,
    jpup,
    diMenit,
    ket,
    ket_in_out,
    durasi_seharusnya,
    daily_in_cleansing_c,
    daily_out_cleansing_c,
    durasi_cleansing_c,
  };
}

function hitungJKPNormal(row) {
  const jmr = row.daily_in;
  let jpr = row.daily_out;
  let ws_rule = row.ws_rule;

  const isJumat = row.is_jumat?.toString().toLowerCase() === "jumat";

  let jms = "-";
  let jps = "-";
  let jmup = "-";
  let jpup = "-";
  let diMenit = isJumat ? 60 : 30;
  let durasi = 0;

  // Normalisasi ws_rule
  if (ws_rule && ws_rule.includes("~")) {
    [jms, jps] = ws_rule.split("~").map((x) => x.trim().replace(/\./g, ":"));
    if (jms.length === 5) jms += ":00";
    if (jps.length === 5) jps += ":00";
    ws_rule = `${jms}~${jps}`;
  }

  // Normalisasi jpr
  if (jpr === "00:00:00") jpr = "24:00:00";

  // Perhitungan durasi
  if (jmr && jpr && jms !== "-" && jps !== "-" && jmr <= jpr) {
    jmup = jmr < jms ? jms : jmr;
    jpup = jpr > jps ? jps : jpr;

    // KODE BARU (GANTI):
    const jmi = "12:00:00";
    const jsi = isJumat ? "13:00:00" : "12:30:00";
    const waktuIstirahatMenit = isJumat ? 60 : 30;

    // Logika baru: cek apakah tidak melintasi waktu istirahat
    const keduanyaSebelumIstirahat = jmup < jmi && jpup < jmi;
    const keduanyaSetelahIstirahat = jmup > jsi && jpup > jsi;
    const tidakDipotongIstirahat =
      keduanyaSebelumIstirahat || keduanyaSetelahIstirahat;

    const durasiMentah = selisihDetik(jpup, jmup) / 3600;
    const potonganIstirahat = tidakDipotongIstirahat
      ? 0
      : waktuIstirahatMenit / 60;
    durasi = Math.max(0, durasiMentah - potonganIstirahat);
  } else {
    // Penanganan jam kosong atau tidak valid
    if (!jmr && !jpr) {
      jmup = "00:00:00";
      jpup = "00:00:00";
    } else if (!jmr && jpr) {
      jmup = jpr;
      jpup = jpr;
    } else if (jmr && !jpr) {
      jmup = "00:00:00";
      jpup = "00:00:00";
    } else if (jmr > jpr) {
      jmup = jpr;
      jpup = jpr;
    }

    durasi = 0;
  }

  const durasi_cleansing_c = durasi;
  const ket = "JKP Normal";
  const ket_in_out = "diperhitungkan";
  const durasi_seharusnya = 8;

  // ✅ Fallback cleansing = data asli
  const daily_in_cleansing_c = jmr || "-";
  const daily_out_cleansing_c = jpr || "-";

  row.__logJKP = generateLogJKPDetail(row, {
    jms,
    jps,
    jmr,
    jpr,
    jmup,
    jpup,
    durasi,
    ws_rule,
    ket,
    ket_in_out,
    durasi_seharusnya,
    daily_in_cleansing_c,
    daily_out_cleansing_c,
    durasi_cleansing_c,
  });

  return {
    durasi,
    jmr,
    jpr,
    jms,
    jps,
    jmup,
    jpup,
    diMenit,
    ket,
    ket_in_out,
    durasi_seharusnya,
    daily_in_cleansing_c,
    daily_out_cleansing_c,
    durasi_cleansing_c,
  };
}

function hitungJKPShift(row) {
  const perner = row.perner;
  const jmr = row.daily_in || "-";
  const jpr_original = row.daily_out || "-";
  const ws_rule = row.ws_rule || "-";
  const ket = "JKP Shift";
  const ket_in_out = "diperhitungkan";
  const durasi_seharusnya = 8;
  const shiftRaw =
    row.jenis_jam_kerja_shift_daily_new ||
    row.jenis_jam_kerja_shift_sap_new ||
    "-";

  let jms = "-";
  let jps = "-";

  // =============================
  // 1. PARSING SHIFT RULE
  // =============================
  if (shiftRaw && shiftRaw !== "-" && shiftRaw.includes("~")) {
    const parts = shiftRaw.split("~");
    if (parts.length >= 3) {
      jms = parts[1]?.trim().replace(/\./g, ":") || "-";
      jps = parts[2]?.trim().replace(/\./g, ":") || "-";

      // Tambahkan detik jika belum ada
      if (jms !== "-" && jms.length === 5) jms += ":00";
      if (jps !== "-" && jps.length === 5) jps += ":00";
    }
  }

  // =============================
  // 2. NORMALISASI JPR
  // =============================
  let jpr = jpr_original;
  if (jpr === "00:00:00") {
    jpr = "24:00:00";
  }

  // =============================
  // 3. CLEANSING DATA - VERSI REVISI
  // =============================
  let jmr_c = jmr;
  let jpr_c = jpr;

  // console.log("🔍 === DEBUGGING CLEANSING CONDITIONS ===");
  // console.log(
  //   `   - is_shift_daily_sap: ${
  //     row.is_shift_daily_sap
  //   } (type: ${typeof row.is_shift_daily_sap})`
  // );
  // console.log(
  //   `   - value_shift_daily_sap: ${
  //     row.value_shift_daily_sap
  //   } (type: ${typeof row.value_shift_daily_sap})`
  // );
  // console.log(
  //   `   - jenis_jam_kerja_shift_daily_new: ${row.jenis_jam_kerja_shift_daily_new}`
  // );
  // console.log(
  //   `   - jenis_jam_kerja_shift_sap_new: ${row.jenis_jam_kerja_shift_sap_new}`
  // );
  // console.log(`   - shiftRaw: ${shiftRaw}`);

  // PERBAIKAN: Kondisi shift aktif yang LEBIH ROBUST - menangani berbagai data type
  const isShiftActive =
    row.is_shift_daily_sap === "true" || // String "true"
    row.is_shift_daily_sap === true || // Boolean true
    row.is_shift_daily_sap === 1 || // Number 1
    Boolean(row.jenis_jam_kerja_shift_daily_new) || // Ada shift daily
    Boolean(row.jenis_jam_kerja_shift_sap_new); // Ada shift SAP

  // console.log(`🔍 isShiftActive: ${isShiftActive}`);

  // Ambil data shift dari prioritas sumber yang DIPERBAIKI
  const shiftData =
    row.value_shift_daily_sap || // Prioritas 1: value_shift_daily_sap
    row.jenis_jam_kerja_shift_daily_new || // Prioritas 2: daily_new
    row.jenis_jam_kerja_shift_sap_new; // Prioritas 3: sap_new

  // console.log(`🔍 shiftData selected: ${shiftData}`);

  // CLEANSING LOGIC - DIPERBAIKI dengan kondisi yang lebih robust
  if (isShiftActive && shiftData && shiftData.includes("~")) {
    try {
      // console.log(
      //   `🔧 Memulai proses cleansing dengan data shift: ${shiftData}`
      // );

      const parts = shiftData.split("~");
      if (parts.length >= 3) {
        const jenis = parts[0]?.trim();
        const shiftStart = parts[1]?.trim();
        const shiftEnd = parts[2]?.trim();

        // console.log(
        //   `🔧 Shift parsed: jenis="${jenis}", start="${shiftStart}", end="${shiftEnd}"`
        // );

        // Cleansing untuk Shift Malam - jam masuk mendekati 00:00:00
        if (
          jenis === "Shift2-Malam" &&
          jmr_c &&
          jmr_c !== "-" &&
          jamDalamRentang(jmr_c, "00:00:00", "00:03:00")
        ) {
          // console.log(`🔧 ✅ CLEANSING SHIFT MALAM - IN: ${jmr_c} → 00:00:00`);
          jmr_c = "00:00:00";
        }

        // Cleansing untuk Shift Siang - jam keluar mendekati 24:00:00
        if (
          jenis === "Shift2-Siang" &&
          jpr_c &&
          jpr_c !== "-" &&
          jamDalamRentang(jpr_c, "23:57:00", "23:59:59")
        ) {
          // console.log(`🔧 ✅ CLEANSING SHIFT SIANG - OUT: ${jpr_c} → 24:00:00`);
          jpr_c = "24:00:00";
        }

        // console.log(`🔧 Hasil akhir cleansing: IN=${jmr_c}, OUT=${jpr_c}`);
      } else {
        // console.log(
        //   `⚠️ Format shift data tidak valid (parts < 3): ${shiftData}`
        // );
      }
    } catch (cleansingError) {
      // console.warn("⚠️ Error saat cleansing shift:", cleansingError.message);
      // Tetap lanjut dengan nilai original jika cleansing gagal
    }
  } else {
    // console.log("⚠️ Kondisi cleansing TIDAK terpenuhi:");
    // console.log(`   - isShiftActive: ${isShiftActive}`);
    // console.log(`   - shiftData exists: ${Boolean(shiftData)}`);
    // console.log(
    //   `   - shiftData contains ~: ${Boolean(
    //     shiftData && shiftData.includes("~")
    //   )}`
    // );
  }

  // Normalisasi ulang jpr_c jika masih 00:00:00
  if (jpr_c === "00:00:00") {
    // console.log(`🔧 Normalisasi JPR: ${jpr_c} → 24:00:00`);
    jpr_c = "24:00:00";
  }

  // console.log(`🔧 FINAL cleansing result: jmr_c=${jmr_c}, jpr_c=${jpr_c}`);
  // console.log("🔍 === END DEBUGGING CLEANSING CONDITIONS ===\n");

  // =============================
  // 4. PERHITUNGAN DURASI RAW
  // =============================
  let jmup_raw = "-";
  let jpup_raw = "-";
  let durasi_raw = 0;

  if (
    jmr &&
    jmr !== "-" &&
    jpr &&
    jpr !== "-" &&
    jms !== "-" &&
    jps !== "-" &&
    jmr <= jpr
  ) {
    try {
      // Tentukan jam masuk pakai (jmup) dan jam keluar pakai (jpup)
      jmup_raw = jmr < jms ? jms : jmr;
      jpup_raw = jpr > jps ? jps : jpr;

      // Hitung durasi dalam jam
      const selisihDetikRaw = selisihDetik(jpup_raw, jmup_raw);
      durasi_raw = Math.max(0, selisihDetikRaw / 3600);

      // console.log(
      //   `📊 Durasi RAW: ${jmr} → ${jpr} (${jmup_raw} → ${jpup_raw}) = ${durasi_raw.toFixed(
      //     3
      //   )} jam`
      // );
    } catch (error) {
      // console.warn("⚠️ Error perhitungan durasi RAW:", error.message);
      durasi_raw = 0;
      jmup_raw = "-";
      jpup_raw = "-";
    }
  } else {
    // console.log("📊 Durasi RAW = 0 (data tidak lengkap atau invalid)");
    // console.log(`   - jmr: ${jmr}, jpr: ${jpr}, jms: ${jms}, jps: ${jps}`);
    // console.log(`   - jmr <= jpr: ${jmr <= jpr}`);
  }

  // =============================
  // 5. PERHITUNGAN DURASI CLEANSING
  // =============================
  let jmup_clean = "-";
  let jpup_clean = "-";
  let durasi_clean = 0;

  if (
    jmr_c &&
    jmr_c !== "-" &&
    jpr_c &&
    jpr_c !== "-" &&
    jms !== "-" &&
    jps !== "-" &&
    jmr_c <= jpr_c
  ) {
    try {
      // Tentukan jam masuk pakai (jmup) dan jam keluar pakai (jpup) untuk cleansing
      jmup_clean = jmr_c < jms ? jms : jmr_c;
      jpup_clean = jpr_c > jps ? jps : jpr_c;

      // Hitung durasi cleansing dalam jam
      const selisihDetikClean = selisihDetik(jpup_clean, jmup_clean);
      durasi_clean = Math.max(0, selisihDetikClean / 3600);

      // console.log(
      //   `📊 Durasi CLEANSING: ${jmr_c} → ${jpr_c} (${jmup_clean} → ${jpup_clean}) = ${durasi_clean.toFixed(
      //     3
      //   )} jam`
      // );
    } catch (error) {
      // console.warn("⚠️ Error perhitungan durasi CLEANSING:", error.message);
      durasi_clean = 0;
      jmup_clean = "-";
      jpup_clean = "-";
    }
  } else {
    // console.log("📊 Durasi CLEANSING = 0 (data tidak lengkap atau invalid)");
    // console.log(
    //   `   - jmr_c: ${jmr_c}, jpr_c: ${jpr_c}, jms: ${jms}, jps: ${jps}`
    // );
    // console.log(`   - jmr_c <= jpr_c: ${jmr_c <= jpr_c}`);
  }

  // =============================
  // 6. GENERATE LOG JKP
  // =============================
  row.__logJKP = generateLogJKPDetail(row, {
    jms,
    jps,
    jmr,
    jpr,
    jmup: jmup_raw,
    jpup: jpup_raw,
    durasi: durasi_raw,
    ws_rule,
    ket,
    ket_in_out,
    durasi_seharusnya,
    daily_in_cleansing_c: jmr_c,
    daily_out_cleansing_c: jpr_c,
    durasi_cleansing_c: durasi_clean,
    // Tambahan untuk debugging
    isShiftActive: isShiftActive,
    shiftDataUsed: shiftData,
    cleansingApplied: jmr_c !== jmr || jpr_c !== jpr,
  });

  // =============================
  // 7. RETURN HASIL - SESUAI FORMAT ENDPOINT
  // =============================
  const result = {
    // Data RAW (untuk jam_kerja_pegawai)
    durasi: durasi_raw,
    jmr: jmr,
    jpr: jpr,
    jms: jms,
    jps: jps,
    jmup: jmup_raw,
    jpup: jpup_raw,

    // Data CLEANSING (untuk daily_in_cleansing, daily_out_cleansing, jam_kerja_pegawai_cleansing)
    daily_in_cleansing_c: jmr_c,
    daily_out_cleansing_c: jpr_c,
    durasi_cleansing_c: durasi_clean,

    // Data tambahan untuk compatibility
    jmup_cleansing: jmup_clean,
    jpup_cleansing: jpup_clean,
    durasi_cleansing: durasi_clean, // alias untuk durasi_cleansing_c
    daily_in_cleansing: jmr_c, // alias untuk daily_in_cleansing_c
    daily_out_cleansing: jpr_c, // alias untuk daily_out_cleansing_c

    // Metadata
    diMenit: "-", // tidak applicable untuk shift
    ket: ket,
    ket_in_out: ket_in_out,
    durasi_seharusnya: durasi_seharusnya,
    perner: perner,

    // Debug info tambahan
    debug_info: {
      isShiftActive: isShiftActive,
      shiftDataUsed: shiftData,
      cleansingApplied: jmr_c !== jmr || jpr_c !== jpr,
      rawInput: { jmr, jpr },
      cleansingResult: { jmr_c, jpr_c },
    },
  };

  // console.log("🎯 HASIL AKHIR hitungJKPShift:");
  // console.log(`   - Durasi RAW: ${result.durasi.toFixed(3)} jam`);
  // console.log(
  //   `   - Durasi CLEANSING: ${result.durasi_cleansing_c.toFixed(3)} jam`
  // );
  // console.log(`   - Daily In: ${result.jmr} → ${result.daily_in_cleansing_c}`);
  // console.log(
  //   `   - Daily Out: ${result.jpr} → ${result.daily_out_cleansing_c}`
  // );
  // console.log(`   - Cleansing Applied: ${result.debug_info.cleansingApplied}`);

  return result;
}

function hitungJKPFinal(row) {
  const {
    att_daily_new,
    abs_daily_new,
    att_sap_new,
    abs_sap_new,
    sppd_umum_new,
    jenis_hari,
    jenis_jam_kerja_shift_daily_new,
    jenis_jam_kerja_shift_sap_new,
  } = row;

  const status = [
    att_daily_new,
    abs_daily_new,
    att_sap_new,
    abs_sap_new,
    sppd_umum_new,
  ];

  const hasStatus = status.some((s) => s && s.trim() !== "");
  const shift =
    jenis_jam_kerja_shift_daily_new || jenis_jam_kerja_shift_sap_new;

  const hariLibur = jenis_hari && jenis_hari.toUpperCase().includes("LIBUR");

  // ❌ Tidak ada lagi pemanggilan cleansingDailyIO di sini

  if (hasStatus) {
    const adaABS = [abs_daily_new, abs_sap_new].some(
      (s) => s && s.trim() !== ""
    );
    if (adaABS) {
      return hitungJKPAbs(row);
    }

    const adaATT = [att_daily_new, att_sap_new, sppd_umum_new].some(
      (s) => s && s.trim() !== ""
    );

    if (adaATT && !hariLibur) {
      return hitungJKPAtt(row);
    } else {
      if (!shift) {
        if (hariLibur) {
          return JKPHariLibur(row);
        } else {
          return hitungJKPNormal(row);
        }
      } else {
        const shiftUpper = shift.toUpperCase();
        const shiftKhusus = ["PIKET", "PDKB", "OFF"].some((key) =>
          shiftUpper.includes(key)
        );
        if (shiftKhusus) {
          // Ganti 'shiftKhusus.includes("OFF")' sesuai dengan cara Anda mendeteksi "OFF" pada shift khusus
          if (hariLibur || shiftUpper.includes("OFF")) {
            return JKPHariLibur(row);
          } else {
            return hitungJKPNormal(row);
          }
        } else {
          return hitungJKPShift(row);
        }
      }
    }
  } else {
    if (!shift) {
      if (hariLibur) {
        return JKPHariLibur(row);
      } else {
        return hitungJKPNormal(row);
      }
    } else {
      const shiftUpper = shift.toUpperCase();
      const shiftKhusus = ["PIKET", "PDKB", "OFF"].some((key) =>
        shiftUpper.includes(key)
      );

      // if (shiftKhusus) {
      //   if (hariLibur) {
      //     return JKPHariLibur(row);
      //   } else {
      //     return hitungJKPNormal(row);
      //   }
      // } else {
      //   return hitungJKPShift(row);
      // }

      if (shiftKhusus) {
        // Ganti 'shiftKhusus.includes("OFF")' sesuai dengan cara Anda mendeteksi "OFF" pada shift khusus
        if (hariLibur || shiftUpper.includes("OFF")) {
          return JKPHariLibur(row);
        } else {
          return hitungJKPNormal(row);
        }
      } else {
        return hitungJKPShift(row);
      }
    }
  }
}

function testJKPShiftKomprehensif() {
  console.log("🧪 === TEST JKP SHIFT KOMPREHENSIF ===\n");

  // Test Case 1: Shift Malam dengan cleansing (KASUS BERMASALAH)
  const testCase1 = {
    perner: "98175225",
    tanggal: "2025-02-02",
    ws_rule: "08.00~16.30",
    att_daily_new: "",
    abs_daily_new: "",
    att_sap_new: "",
    abs_sap_new: "",
    sppd_umum_new: "",
    jenis_hari: "LIBUR-NASIONAL-AKHIR PEKAN",
    is_shift_daily_sap: "true",
    value_shift_daily_sap: "Shift2-Malam~00.00~08.00",
    jenis_jam_kerja_shift_daily_new: "",
    jenis_jam_kerja_shift_sap_new: "Shift2-Malam~00.00~08.00",
    daily_in: "00:01:00", // Harus di-cleansing ke 00:00:00
    daily_out: "08:03:49", // Tidak di-cleansing (sudah lewat shift end)
    is_jumat: "bukan jumat",
  };

  // Test Case 2: Shift Siang dengan cleansing
  const testCase2 = {
    perner: "88888888",
    tanggal: "2025-02-25",
    ws_rule: "08.00~16.30",
    att_daily_new: "",
    abs_daily_new: "",
    att_sap_new: "",
    abs_sap_new: "",
    sppd_umum_new: "",
    jenis_hari: "HARI KERJA",
    is_shift_daily_sap: "true",
    value_shift_daily_sap: "Shift2-Siang~16.00~24.00",
    jenis_jam_kerja_shift_daily_new: "",
    jenis_jam_kerja_shift_sap_new: "Shift2-Siang~16.00~24.00",
    daily_in: "15:30:15",
    daily_out: "23:58:30", // Harus di-cleansing ke 24:00:00
    is_jumat: "bukan jumat",
  };

  // Test Case 3: Shift normal tanpa cleansing
  const testCase3 = {
    perner: "77777777",
    tanggal: "2025-02-26",
    ws_rule: "08.00~16.30",
    att_daily_new: "",
    abs_daily_new: "",
    att_sap_new: "",
    abs_sap_new: "",
    sppd_umum_new: "",
    jenis_hari: "HARI KERJA",
    is_shift_daily_sap: "false",
    value_shift_daily_sap: "",
    jenis_jam_kerja_shift_daily_new: "Shift-Pagi~08.00~16.00",
    jenis_jam_kerja_shift_sap_new: "",
    daily_in: "08:15:00",
    daily_out: "16:30:00",
    is_jumat: "bukan jumat",
  };

  // Test Case 4: Edge case - Shift Malam di luar rentang cleansing
  const testCase4 = {
    perner: "99999999",
    tanggal: "2025-02-27",
    ws_rule: "08.00~16.30",
    att_daily_new: "",
    abs_daily_new: "",
    att_sap_new: "",
    abs_sap_new: "",
    sppd_umum_new: "",
    jenis_hari: "HARI KERJA",
    is_shift_daily_sap: "true",
    value_shift_daily_sap: "Shift2-Malam~00.00~08.00",
    jenis_jam_kerja_shift_daily_new: "",
    jenis_jam_kerja_shift_sap_new: "Shift2-Malam~00.00~08.00",
    daily_in: "00:10:00", // Di luar rentang cleansing (>00:05:00)
    daily_out: "07:45:00",
    is_jumat: "bukan jumat",
  };

  const testCases = [
    {
      name: "🌙 Shift Malam dengan Cleansing (KASUS BERMASALAH)",
      data: testCase1,
      expected: { cleansingIn: "00:00:00", cleansingDurasi: 8.0 },
    },
    {
      name: "☀️ Shift Siang dengan Cleansing",
      data: testCase2,
      expected: { cleansingOut: "24:00:00" },
    },
    { name: "🌅 Shift Pagi Normal", data: testCase3, expected: {} },
    {
      name: "🌙 Shift Malam Tanpa Cleansing",
      data: testCase4,
      expected: { cleansingIn: "00:10:00" },
    },
  ];

  let totalTests = 0;
  let passedTests = 0;

  testCases.forEach((testCase, index) => {
    console.log(`\n📋 TEST CASE ${index + 1}: ${testCase.name}`);
    console.log("=".repeat(60));

    try {
      const hasil = hitungJKPShift(testCase.data);
      totalTests++;

      console.log("📊 HASIL PERHITUNGAN:");
      console.log(`Perner               : ${hasil.perner}`);
      console.log(`Durasi RAW           : ${hasil.durasi.toFixed(3)} jam`);
      console.log(
        `Durasi CLEANSING     : ${hasil.durasi_cleansing_c.toFixed(3)} jam`
      );
      console.log(`Daily In RAW         : ${hasil.jmr}`);
      console.log(`Daily Out RAW        : ${hasil.jpr}`);
      console.log(`Daily In CLEANSING   : ${hasil.daily_in_cleansing_c}`);
      console.log(`Daily Out CLEANSING  : ${hasil.daily_out_cleansing_c}`);
      console.log(`JMS (Shift Start)    : ${hasil.jms}`);
      console.log(`JPS (Shift End)      : ${hasil.jps}`);
      console.log(`JMUP RAW             : ${hasil.jmup}`);
      console.log(`JPUP RAW             : ${hasil.jpup}`);

      // Simulasi data endpoint
      console.log("\n📤 DATA UNTUK ENDPOINT:");
      const dataEndpoint = {
        perner: testCase.data.perner,
        tanggal: formatTanggal(testCase.data.tanggal),
        jkp: parseFloat(hasil.durasi.toFixed(3)),
        daily_in_cleansing:
          hasil.daily_in_cleansing_c !== "-"
            ? hasil.daily_in_cleansing_c
            : null,
        daily_out_cleansing:
          hasil.daily_out_cleansing_c !== "-"
            ? hasil.daily_out_cleansing_c
            : null,
        durasi_cleansing: parseFloat(hasil.durasi_cleansing_c.toFixed(3)),
      };
      console.log(JSON.stringify(dataEndpoint, null, 2));

      // Validasi ekspektasi
      console.log("\n✅ VALIDASI HASIL:");
      let testPassed = true;

      if (testCase.expected.cleansingIn) {
        const expected = testCase.expected.cleansingIn;
        const actual = hasil.daily_in_cleansing_c;
        const isValid = actual === expected;
        console.log(
          `${
            isValid ? "✅" : "❌"
          } Daily In Cleansing: Expected ${expected}, Got ${actual}`
        );
        if (!isValid) testPassed = false;
      }

      if (testCase.expected.cleansingOut) {
        const expected = testCase.expected.cleansingOut;
        const actual = hasil.daily_out_cleansing_c;
        const isValid = actual === expected;
        console.log(
          `${
            isValid ? "✅" : "❌"
          } Daily Out Cleansing: Expected ${expected}, Got ${actual}`
        );
        if (!isValid) testPassed = false;
      }

      if (testCase.expected.cleansingDurasi) {
        const expected = testCase.expected.cleansingDurasi;
        const actual = parseFloat(hasil.durasi_cleansing_c.toFixed(3));
        const isValid = actual === expected;
        console.log(
          `${
            isValid ? "✅" : "❌"
          } Durasi Cleansing: Expected ${expected}, Got ${actual}`
        );
        if (!isValid) testPassed = false;
      }

      // Validasi umum
      const generalChecks = [
        {
          name: "Durasi RAW Valid",
          condition: !isNaN(hasil.durasi) && hasil.durasi >= 0,
        },
        {
          name: "Durasi CLEANSING Valid",
          condition:
            !isNaN(hasil.durasi_cleansing_c) && hasil.durasi_cleansing_c >= 0,
        },
        { name: "JMS Parsed", condition: hasil.jms !== "-" },
        { name: "JPS Parsed", condition: hasil.jps !== "-" },
      ];

      generalChecks.forEach((check) => {
        console.log(
          `${check.condition ? "✅" : "❌"} ${check.name}: ${check.condition}`
        );
        if (!check.condition) testPassed = false;
      });

      if (testPassed) {
        passedTests++;
        console.log("🎉 TEST PASSED!");
      } else {
        console.log("💥 TEST FAILED!");
      }
    } catch (error) {
      console.error(`❌ ERROR pada test case ${index + 1}:`, error.message);
      console.log("💥 TEST FAILED!");
    }
  });

  console.log("\n🏁 === RINGKASAN TEST ===");
  console.log(`Total Tests: ${totalTests}`);
  console.log(`Passed: ${passedTests}`);
  console.log(`Failed: ${totalTests - passedTests}`);
  console.log(
    `Success Rate: ${((passedTests / totalTests) * 100).toFixed(1)}%`
  );

  if (passedTests === totalTests) {
    console.log("🎊 SEMUA TEST BERHASIL!");
  } else {
    console.log("⚠️ ADA TEST YANG GAGAL - PERLU DIPERBAIKI!");
  }
}

// =============================================
// JALANKAN TEST
// =============================================
// testJKPShiftKomprehensif();

module.exports = {
  hitungJKPNormal,
  hitungJKPShift,
  hitungJKPFinal,
};

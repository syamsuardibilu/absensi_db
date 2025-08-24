// === API endpoints ===
const PROD_API = "https://absensi-db.onrender.com"; // Render (Produksi)
const LOCAL_API = "http://localhost:3000"; // Local dev
const LAN_API = `http://${location.hostname}:3000`; // Akses via IP LAN

// Deteksi host
const host = location.hostname;
const isLocalHost = /^(localhost|127\.0\.0\.1)$/.test(host);
const isPrivateLAN =
  /^(10\.\d+\.\d+\.\d+|192\.168\.\d+\.\d+|172\.(1[6-9]|2\d|3[0-1])\.\d+\.\d+)$/.test(
    host
  );

// Urutan prioritas: window.BASE_URL (override) -> Local -> LAN -> Prod
const BASE_URL =
  (window.BASE_URL && window.BASE_URL.trim()) ||
  (isLocalHost ? LOCAL_API : isPrivateLAN ? LAN_API : PROD_API);

// Helper fetch
const api = (path, opts = {}) => {
  const p = path.startsWith("/") ? path : `/${path}`;
  return fetch(`${BASE_URL}${p}`, opts);
};

function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
}

const jenisData = [
  "DATA PEGAWAI",
  "DATA HARI LIBUR",
  "CI CO DAILY",
  "ATT ABS Daily",
  "ATT SAP",
  "ABS SAP",
  "SPPD UMUM",
  "WORK SCHEDULE",
  "Substitution Daily",
  "Substitution SAP",
];

const container = document.getElementById("form-container");

jenisData.forEach((jenis) => {
  const idBase = jenis.toLowerCase().replace(/\s+/g, "_").replace(/\W/g, "");

  const block = document.createElement("div");
  block.className = "input-block";

  let dateInputs = "";
  if (jenis === "DATA PEGAWAI") {
    dateInputs = `
      <div class="date-inputs">
        <label>Start Date: <input type="date" id="${idBase}_start"></label>
        <label>End Date: <input type="date" id="${idBase}_end"></label>
      </div>
    `;
  }

  block.innerHTML = `
    <label for="${idBase}">${jenis}</label>
    ${dateInputs}
    <textarea id="${idBase}_textarea" placeholder="Tempelkan data untuk ${jenis}..."></textarea>
      <div class="button-group">
        <button class="btn primary" onclick="prosesData('${idBase}')">Proses</button>
        <button class="btn danger" onclick="hapusData('${idBase}')">Hapus</button>
        
        ${
          idBase !== "data_pegawai"
            ? `<button class="btn gray" onclick="hapusDataDB('${idBase}')">🗑 Hapus DB</button>`
            : ""
        }
      </div>

    <div id="${idBase}_output" class="output-box"></div>
  `;

  (container ? container : { appendChild: () => {} }).appendChild(block);

  const textarea = document.getElementById(`${idBase}_textarea`);
  // SESUDAH - pakai debouncing
  const debouncedTampilkan = debounce(() => tampilkanJumlahBaris(idBase), 300);
  textarea.addEventListener("input", debouncedTampilkan);
});

function prosesData(id) {
  const startTime = Date.now();
  const textarea = document.getElementById(`${id}_textarea`);
  const output = document.getElementById(`${id}_output`);
  const input = textarea.value.trim();

  // Reset output box styling
  output.style.display = "block";
  output.className = "output-box"; // Default class

  if (id === "data_pegawai") {
    const startDate = document.getElementById(`${id}_start`).value;
    const endDate = document.getElementById(`${id}_end`).value;

    if (!startDate || !endDate) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Harap isi start dan end date.";
      return;
    }

    if (endDate < startDate) {
      output.className = "output-box error";
      output.innerText = "❌ End Date tidak boleh lebih kecil dari Start Date.";
      return;
    }

    const perners = input
      .split(/\r?\n/)
      .map((p) => p.trim())
      .filter((p) => p !== "");
    if (perners.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Tidak ada data perner.";
      return;
    }

    output.innerText = "⏳ Mengirim data ke server...";

    // Kirim data ke server
    api("/update-perner-tanggal", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        perners,
        startDate,
        endDate,
      }),
    })
      .then((res) => res.json())
      .then((data) => {
        const endTime = Date.now();
        const duration = endTime - startTime;
        output.className = "output-box";
        output.innerText = data.message + `\n⚡ Completed in: ${duration}ms`;
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengirim ke server: " + err.message;
      });

    return;
  }

  if (id === "data_hari_libur") {
    const rows = input
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line !== "");
    if (rows.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Harap masukkan data hari libur.";
      return;
    }

    const liburData = [];

    for (const row of rows) {
      const parts = row.split("\t");
      if (parts.length < 3) continue; // skip baris tidak lengkap
      const [tgl, jenis, keterangan] = parts;
      liburData.push({
        tanggal: convertTanggalToMySQL(tgl),
        jenis_hari: `LIBUR-${jenis.trim()}-${keterangan.trim()}`,
      });
    }

    output.innerText = "⏳ Mengirim data ke server...";

    api("/update-hari-libur", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ data: liburData }),
    })
      .then((res) => res.json())
      .then((data) => {
        const endTime = Date.now();
        const duration = endTime - startTime;
        output.className = "output-box";
        output.innerText = data.message + `\n⚡ Completed in: ${duration}ms`;
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengirim data hari libur: " + err.message;
      });

    return;
  }

  if (id === "ci_co_daily") {
    const rows = input
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line !== "");

    if (rows.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Harap masukkan data CI CO DAILY.";
      return;
    }

    const dataMap = new Map();

    for (const row of rows) {
      const parts = row.split("\t");
      if (parts.length < 5) continue; // ✨ MODIFIED: Sekarang butuh minimal 5 parts

      // ✨ MODIFIED: Tambah destructuring untuk range
      const [perner, tgl, waktu, tipe, range] = parts;

      const tanggal = convertTanggalToMySQL(tgl);
      const key = `${perner.trim()}||${tanggal}||${tipe.trim().toUpperCase()}`;
      const jam = waktu.trim();

      // ✨ MODIFIED: Convert range to correction text
      const correction = range.trim() === "0" ? "koreksi" : "tanpa koreksi";

      if (!dataMap.has(key)) {
        // ✨ MODIFIED: Store both waktu and correction
        dataMap.set(key, { waktu: jam, correction: correction });
      } else {
        const existing = dataMap.get(key);

        if (tipe.toUpperCase() === "CLOCK_IN" && jam < existing.waktu) {
          // ✨ MODIFIED: Update both waktu and correction when time wins
          dataMap.set(key, { waktu: jam, correction: correction });
        } else if (tipe.toUpperCase() === "CLOCK_OUT" && jam > existing.waktu) {
          // ✨ MODIFIED: Update both waktu and correction when time wins
          dataMap.set(key, { waktu: jam, correction: correction });
        }
        // If time doesn't win, keep existing waktu and correction
      }
    }

    const cleanedData = [];
    for (const [key, value] of dataMap.entries()) {
      const [perner, tanggal, tipe] = key.split("||");

      // ✨ MODIFIED: Include correction in cleanedData
      cleanedData.push({
        perner,
        tanggal,
        waktu: value.waktu,
        tipe,
        correction: value.correction,
      });
    }

    if (cleanedData.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Tidak ada data CI/CO yang valid.";
      return;
    }

    output.innerText = "⏳ Mengirim data ke server...";

    api("/update-ci-co", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ data: cleanedData }),
    })
      .then((res) => res.json())
      .then((data) => {
        const endTime = Date.now();
        const duration = endTime - startTime;
        output.className = "output-box";
        output.innerText = data.message + `\n⚡ Completed in: ${duration}ms`;
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengirim data CI/CO: " + err.message;
      });

    return;
  }

  if (id === "att_abs_daily") {
    const rows = input
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line !== "");

    if (rows.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Harap masukkan data ATT ABS Daily.";
      return;
    }

    const tipeMap = {
      101: ["Cuti Tahunan", "abs"],
      201: ["Cuti Besar", "abs"],
      301: ["Pernikhn pkrj/anak dlm kt", "abs"],
      302: ["Pernikhn pkrj/anak lr kt", "abs"],
      303: ["Pernkhn sdr dlm kota", "abs"],
      304: ["Pernkhn sdr lr kota", "abs"],
      305: ["Isteri melahirkan anak", "abs"],
      306: ["Absen tanpa alasan", "abs"],
      307: ["Istri/ suami/ anak wafat", "abs"],
      308: ["Orang tua/ mertua wafat", "abs"],
      309: ["Sdr kandung pekerja wafat", "abs"],
      310: ["Menantu pekerja wafat", "abs"],
      311: ["Ibadah Haji/Lainnya", "abs"],
      312: ["Khitan/BaptisAnk/Upc.Gigi", "abs"],
      313: ["Cuti Alasan Pribadi", "abs"],
      314: ["Haid", "abs"],
      315: ["Pekerja Melahirkan", "abs"],
      316: ["Melahirkan anak ke 4 dst", "abs"],
      319: ["Khitn/Bptis/UpGigi lrKota", "abs"],
      320: ["Istri/ suami/ anak sakit", "abs"],
      321: ["Orang tua/ mertua sakit", "abs"],
      322: ["Keluarga sakit luar kota", "abs"],
      323: ["Pegawai/Anak Wisuda", "abs"],
      324: ["Pgawai/Ank Wisuda lr kota", "abs"],
      325: ["Keadaan Kahar", "abs"],
      326: ["Sdr kandung wafat lr kota", "abs"],
      327: ["Anak/Mnantu wafat lr kota", "abs"],
      328: ["Ortu/Mertua wafat lr kota", "abs"],
      329: ["Suami/Istri wafat lr kota", "abs"],
      501: ["Keguguran", "abs"],
      502: ["Sakit tanpa surat dokter", "abs"],
      503: ["Sakit dengan surat dokter", "abs"],
      504: ["Sakit berkelanjutan", "abs"],
      601: ["Skorsing", "abs"],
      701: ["Tugas Belajar", "abs"],
      901: ["Penahanan", "abs"],
      902: ["Penahanan dan Proses PHK", "abs"],
      903: ["Penahanan Tindak Pidana", "abs"],
      904: ["Penahanan Lakalantas", "abs"],
      9001: ["Seminar", "att"],
      9002: ["Workshop", "att"],
      9003: ["Perjalanan dinas", "att"],
      9004: ["Rapat di luar kantor", "att"],
      9005: ["Acara di luar kantor", "att"],
      9006: ["Pelatihan dengan kuota", "att"],
      9007: ["Pelatihan tanpa kuota", "att"],
      9008: ["Tugas di luar kantor", "att"],
      9009: ["Tugas Belajar", "att"],
    };

    const attMap = new Map();
    const absMap = new Map();

    for (const row of rows) {
      const parts = row.split("\t");
      if (parts.length < 5) continue;

      const [perner, , start, end, tipeKode] = parts;
      const [sd, sm, sy] = start.split("/");
      const [ed, em, ey] = end.split("/");
      const startDate = new Date(`${sy}-${sm}-${sd}`);
      const endDate = new Date(`${ey}-${em}-${ed}`);

      const tipeKey = tipeKode.replace(/^0+/, ""); // hapus leading zero
      if (!tipeMap[tipeKey]) continue;
      const [tipeText, kategori] = tipeMap[tipeKey];

      for (
        let d = new Date(startDate);
        d <= endDate;
        d.setDate(d.getDate() + 1)
      ) {
        const tanggal = d.toISOString().slice(0, 10);
        const key = `${perner}||${tanggal}`;

        const mapToUse = kategori === "att" ? attMap : absMap;

        if (!mapToUse.has(key)) {
          mapToUse.set(key, new Set());
        }
        mapToUse.get(key).add(tipeText);
      }
    }

    const finalData = [];

    for (const [key, tipeSet] of attMap.entries()) {
      const [perner, tanggal] = key.split("||");
      finalData.push({
        perner,
        tanggal,
        tipe_text: [...tipeSet].join(" || "),
        kategori: "att",
      });
    }

    for (const [key, tipeSet] of absMap.entries()) {
      const [perner, tanggal] = key.split("||");
      finalData.push({
        perner,
        tanggal,
        tipe_text: [...tipeSet].join(" || "),
        kategori: "abs",
      });
    }

    if (finalData.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Tidak ada data valid yang diproses.";
      return;
    }

    output.innerText = "⏳ Mengirim data ke server...";

    api("/update-att-abs-daily", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ data: finalData }),
    })
      .then((res) => res.json())
      .then((data) => {
        const endTime = Date.now();
        const duration = endTime - startTime;
        output.className = "output-box";
        output.innerText = data.message + `\n⚡ Completed in: ${duration}ms`;
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengirim data: " + err.message;
      });

    return;
  }

  if (id === "att_sap") {
    const rows = input
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line !== "");

    if (rows.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Harap masukkan data ATT SAP.";
      return;
    }

    const tipeMap = {
      101: ["Cuti Tahunan", "abs"],
      201: ["Cuti Besar", "abs"],
      301: ["Pernikhn pkrj/anak dlm kt", "abs"],
      302: ["Pernikhn pkrj/anak lr kt", "abs"],
      303: ["Pernkhn sdr dlm kota", "abs"],
      304: ["Pernkhn sdr lr kota", "abs"],
      305: ["Isteri melahirkan anak", "abs"],
      306: ["Absen tanpa alasan", "abs"],
      307: ["Istri/ suami/ anak wafat", "abs"],
      308: ["Orang tua/ mertua wafat", "abs"],
      309: ["Sdr kandung pekerja wafat", "abs"],
      310: ["Menantu pekerja wafat", "abs"],
      311: ["Ibadah Haji/Lainnya", "abs"],
      312: ["Khitan/BaptisAnk/Upc.Gigi", "abs"],
      313: ["Cuti Alasan Pribadi", "abs"],
      314: ["Haid", "abs"],
      315: ["Pekerja Melahirkan", "abs"],
      316: ["Melahirkan anak ke 4 dst", "abs"],
      319: ["Khitn/Bptis/UpGigi lrKota", "abs"],
      320: ["Istri/ suami/ anak sakit", "abs"],
      321: ["Orang tua/ mertua sakit", "abs"],
      322: ["Keluarga sakit luar kota", "abs"],
      323: ["Pegawai/Anak Wisuda", "abs"],
      324: ["Pgawai/Ank Wisuda lr kota", "abs"],
      325: ["Keadaan Kahar", "abs"],
      326: ["Sdr kandung wafat lr kota", "abs"],
      327: ["Anak/Mnantu wafat lr kota", "abs"],
      328: ["Ortu/Mertua wafat lr kota", "abs"],
      329: ["Suami/Istri wafat lr kota", "abs"],
      501: ["Keguguran", "abs"],
      502: ["Sakit tanpa surat dokter", "abs"],
      503: ["Sakit dengan surat dokter", "abs"],
      504: ["Sakit berkelanjutan", "abs"],
      601: ["Skorsing", "abs"],
      701: ["Tugas Belajar", "abs"],
      901: ["Penahanan", "abs"],
      902: ["Penahanan dan Proses PHK", "abs"],
      903: ["Penahanan Tindak Pidana", "abs"],
      904: ["Penahanan Lakalantas", "abs"],
      9001: ["Seminar", "att"],
      9002: ["Workshop", "att"],
      9003: ["Perjalanan dinas", "att"],
      9004: ["Rapat di luar kantor", "att"],
      9005: ["Acara di luar kantor", "att"],
      9006: ["Pelatihan dengan kuota", "att"],
      9007: ["Pelatihan tanpa kuota", "att"],
      9008: ["Tugas di luar kantor", "att"],
      9009: ["Tugas Belajar", "att"],
    };

    const dataMap = new Map();

    for (const row of rows) {
      const parts = row.split("\t");
      if (parts.length < 5) continue;

      const [perner, , start, end, tipeKode] = parts;
      const tipeKey = tipeKode.replace(/^0+/, "");
      if (!tipeMap[tipeKey]) continue;

      const tipeText = tipeMap[tipeKey][0];
      const [sd, sm, sy] = start.split("/");
      const [ed, em, ey] = end.split("/");

      const startDate = new Date(`${sy}-${sm}-${sd}`);
      const endDate = new Date(`${ey}-${em}-${ed}`);

      for (
        let d = new Date(startDate);
        d <= endDate;
        d.setDate(d.getDate() + 1)
      ) {
        const tanggal = d.toISOString().slice(0, 10);
        const key = `${perner}||${tanggal}`;

        if (!dataMap.has(key)) {
          dataMap.set(key, new Set());
        }
        dataMap.get(key).add(tipeText);
      }
    }

    const finalData = [];

    for (const [key, tipeSet] of dataMap.entries()) {
      const [perner, tanggal] = key.split("||");
      finalData.push({
        perner,
        tanggal,
        tipe_text: [...tipeSet].join(" || "),
      });
    }

    if (finalData.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Tidak ada data ATT SAP yang valid.";
      return;
    }

    output.innerText = "⏳ Mengirim data ke server...";

    api("/update-att-sap", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ data: finalData }),
    })
      .then((res) => res.json())
      .then((data) => {
        const endTime = Date.now();
        const duration = endTime - startTime;
        output.className = "output-box";
        output.innerText =
          data.message ||
          `✅ Data ATT SAP berhasil diproses dalam ${duration} ms.`;
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengirim data: " + err.message;
      });

    return;
  }

  if (id === "abs_sap") {
    const rows = input
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line !== "");

    if (rows.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Harap masukkan data ABS SAP.";
      return;
    }

    const tipeMap = {
      101: ["Cuti Tahunan", "abs"],
      201: ["Cuti Besar", "abs"],
      301: ["Pernikhn pkrj/anak dlm kt", "abs"],
      302: ["Pernikhn pkrj/anak lr kt", "abs"],
      303: ["Pernkhn sdr dlm kota", "abs"],
      304: ["Pernkhn sdr lr kota", "abs"],
      305: ["Isteri melahirkan anak", "abs"],
      306: ["Absen tanpa alasan", "abs"],
      307: ["Istri/ suami/ anak wafat", "abs"],
      308: ["Orang tua/ mertua wafat", "abs"],
      309: ["Sdr kandung pekerja wafat", "abs"],
      310: ["Menantu pekerja wafat", "abs"],
      311: ["Ibadah Haji/Lainnya", "abs"],
      312: ["Khitan/BaptisAnk/Upc.Gigi", "abs"],
      313: ["Cuti Alasan Pribadi", "abs"],
      314: ["Haid", "abs"],
      315: ["Pekerja Melahirkan", "abs"],
      316: ["Melahirkan anak ke 4 dst", "abs"],
      319: ["Khitn/Bptis/UpGigi lrKota", "abs"],
      320: ["Istri/ suami/ anak sakit", "abs"],
      321: ["Orang tua/ mertua sakit", "abs"],
      322: ["Keluarga sakit luar kota", "abs"],
      323: ["Pegawai/Anak Wisuda", "abs"],
      324: ["Pgawai/Ank Wisuda lr kota", "abs"],
      325: ["Keadaan Kahar", "abs"],
      326: ["Sdr kandung wafat lr kota", "abs"],
      327: ["Anak/Mnantu wafat lr kota", "abs"],
      328: ["Ortu/Mertua wafat lr kota", "abs"],
      329: ["Suami/Istri wafat lr kota", "abs"],
      501: ["Keguguran", "abs"],
      502: ["Sakit tanpa surat dokter", "abs"],
      503: ["Sakit dengan surat dokter", "abs"],
      504: ["Sakit berkelanjutan", "abs"],
      601: ["Skorsing", "abs"],
      701: ["Tugas Belajar", "abs"],
      901: ["Penahanan", "abs"],
      902: ["Penahanan dan Proses PHK", "abs"],
      903: ["Penahanan Tindak Pidana", "abs"],
      904: ["Penahanan Lakalantas", "abs"],
      9001: ["Seminar", "att"],
      9002: ["Workshop", "att"],
      9003: ["Perjalanan dinas", "att"],
      9004: ["Rapat di luar kantor", "att"],
      9005: ["Acara di luar kantor", "att"],
      9006: ["Pelatihan dengan kuota", "att"],
      9007: ["Pelatihan tanpa kuota", "att"],
      9008: ["Tugas di luar kantor", "att"],
      9009: ["Tugas Belajar", "att"],
    };

    const dataMap = new Map();

    for (const row of rows) {
      const parts = row.split("\t");
      if (parts.length < 5) continue;

      const [perner, , start, end, tipeKode] = parts;
      const tipeKey = tipeKode.replace(/^0+/, "");
      if (!tipeMap[tipeKey]) continue;

      const tipeText = tipeMap[tipeKey][0];
      const [sd, sm, sy] = start.split("/");
      const [ed, em, ey] = end.split("/");

      const startDate = new Date(`${sy}-${sm}-${sd}`);
      const endDate = new Date(`${ey}-${em}-${ed}`);

      for (
        let d = new Date(startDate);
        d <= endDate;
        d.setDate(d.getDate() + 1)
      ) {
        const tanggal = d.toISOString().slice(0, 10);
        const key = `${perner}||${tanggal}`;
        if (!dataMap.has(key)) {
          dataMap.set(key, new Set());
        }
        dataMap.get(key).add(tipeText);
      }
    }

    const finalData = [];

    for (const [key, tipeSet] of dataMap.entries()) {
      const [perner, tanggal] = key.split("||");
      finalData.push({
        perner,
        tanggal,
        tipe_text: [...tipeSet].join(" || "),
      });
    }

    if (finalData.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Tidak ada data ABS SAP yang valid.";
      return;
    }

    output.innerText = "⏳ Mengirim data ke server...";

    api("/update-abs-sap", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ data: finalData }),
    })
      .then((res) => res.json())
      .then((data) => {
        const endTime = Date.now();
        const duration = endTime - startTime;
        output.className = "output-box";
        output.innerText =
          data.message ||
          `✅ Data ABS SAP berhasil diproses dalam ${duration} ms.`;
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengirim data: " + err.message;
      });

    return;
  }

  if (id === "sppd_umum") {
    const rows = input
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line !== "");

    if (rows.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Harap masukkan data SPPD Umum.";
      return;
    }

    const resultMap = new Map();

    for (const row of rows) {
      const parts = row.split("\t");
      if (parts.length < 3) continue;

      const [perner, start, end] = parts;
      const [sd, sm, sy] = start.split("/");
      const [ed, em, ey] = end.split("/");

      const startDate = new Date(`${sy}-${sm}-${sd}`);
      const endDate = new Date(`${ey}-${em}-${ed}`);

      for (
        let d = new Date(startDate);
        d <= endDate;
        d.setDate(d.getDate() + 1)
      ) {
        const tanggal = d.toISOString().slice(0, 10);
        const key = `${perner}||${tanggal}`;

        if (!resultMap.has(key)) {
          resultMap.set(key, []);
        }
        resultMap.get(key).push("Perjalanan dinas");
      }
    }

    const finalData = [];

    for (const [key, values] of resultMap.entries()) {
      const [perner, tanggal] = key.split("||");
      finalData.push({
        perner,
        tanggal,
        keterangan: values.join(" || "),
      });
    }

    if (finalData.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Tidak ada data valid.";
      return;
    }

    output.innerText = "⏳ Mengirim data ke server...";

    api("/update-sppd-umum", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ data: finalData }),
    })
      .then((res) => res.json())
      .then((data) => {
        const endTime = Date.now();
        const duration = endTime - startTime;
        output.className = "output-box";
        output.innerText =
          data.message ||
          `✅ Data SPPD Umum berhasil diproses dalam ${duration} ms.`;
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengirim data: " + err.message;
      });

    return;
  }

  if (id === "work_schedule") {
    const overallStartTime = Date.now(); // ← TAMBAH: Overall timing
    output.innerText = "⏳ Mengambil batas tanggal maksimum dari database...";

    const getLastDateStart = Date.now(); // ← TAMBAH: LastDate timing

    api("/get-lastdate")
      .then((res) => res.json())
      .then(({ lastdate }) => {
        const getLastDateDuration = Date.now() - getLastDateStart; // ← TAMBAH
        console.log(`📅 LastDate retrieved in ${getLastDateDuration}ms`); // ← TAMBAH

        if (!lastdate) {
          output.className = "output-box warning";
          output.innerText =
            "⚠️ Tidak ada tanggal maksimum ditemukan di database.";
          return;
        }

        // ← TAMBAH: Data processing timing
        const dataProcessingStart = Date.now();
        output.innerText = "⏳ Memproses data work schedule...";

        const rows = input
          .split(/\r?\n/)
          .map((line) => line.trim())
          .filter((line) => line !== "");

        const dataMap = new Map();
        let processedRows = 0; // ← TAMBAH: Counter
        let errorRows = 0; // ← TAMBAH: Error counter

        for (const row of rows) {
          const parts = row.split("\t");
          if (parts.length < 6) {
            errorRows++; // ← TAMBAH
            continue;
          }

          try {
            // ← TAMBAH: Error handling
            const [perner, , start, end, , jadwal] = parts;

            const [sd, sm, sy] = start.split("/");
            let [ed, em, ey] = end.split("/");

            let endDate = new Date(`${ey}-${em}-${ed}`);
            if (ed === "31" && em === "12" && ey === "9999") {
              let correctedDate = new Date(lastdate);
              correctedDate.setDate(correctedDate.getDate() + 1);
              endDate = correctedDate;
            }

            const startDate = new Date(`${sy}-${sm}-${sd}`);

            const [seninText, jumatText] = jadwal.split(",J:");
            if (!seninText || !jumatText) {
              errorRows++; // ← TAMBAH
              continue;
            }

            const [jamInNormalRaw, jamOutNormalRaw] = seninText
              .trim()
              .split("-")
              .map((s) => s.replace(":", "."));
            const [jamInJumatRaw, jamOutJumatRaw] = jumatText
              .trim()
              .split("-")
              .map((s) => s.replace(":", "."));

            const jamInNormal = formatJam(jamInNormalRaw);
            const jamOutNormal = formatJam(jamOutNormalRaw);
            const jamInJumat = formatJam(jamInJumatRaw);
            const jamOutJumat = formatJam(jamOutJumatRaw);

            for (
              let d = new Date(startDate);
              d <= endDate;
              d.setDate(d.getDate() + 1)
            ) {
              const tanggal = d.toISOString().slice(0, 10);
              const isJumat = d.getDay() === 5;

              const jamMasuk = isJumat ? jamInJumat : jamInNormal;
              const jamPulang = isJumat ? jamOutJumat : jamOutNormal;
              const nilai = `${jamMasuk}~${jamPulang}`;

              const key = `${perner}||${tanggal}`;
              dataMap.set(key, nilai);
            }

            processedRows++; // ← TAMBAH
          } catch (error) {
            // ← TAMBAH: Error handling
            console.warn(`❌ Error processing work schedule row:`, error);
            errorRows++;
          }
        }

        const finalData = [];

        for (const [key, ws_rule] of dataMap.entries()) {
          const [perner, tanggal] = key.split("||");
          finalData.push({ perner, tanggal, ws_rule });
        }

        const dataProcessingDuration = Date.now() - dataProcessingStart; // ← TAMBAH

        // ← TAMBAH: Enhanced logging
        console.log(`🔧 Work Schedule data processing completed:`);
        console.log(`   - Input rows: ${rows.length}`);
        console.log(`   - Processed rows: ${processedRows}`);
        console.log(`   - Error rows: ${errorRows}`);
        console.log(`   - Generated records: ${finalData.length}`);
        console.log(`   - Processing time: ${dataProcessingDuration}ms`);

        if (finalData.length === 0) {
          output.className = "output-box warning";
          output.innerText = "⚠️ Tidak ada data valid untuk Work Schedule.";
          return;
        }

        // ← TAMBAH: Show processing summary
        output.innerText = `⏳ Mengirim ${finalData.length} records ke server... (diproses dari ${rows.length} input rows)`;

        const apiCallStart = Date.now(); // ← TAMBAH: API timing

        api("/update-work-schedule", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ data: finalData }),
        })
          .then((res) => res.json())
          .then((data) => {
            const overallDuration = Date.now() - overallStartTime; // ← TAMBAH
            const apiCallDuration = Date.now() - apiCallStart; // ← TAMBAH

            output.className = "output-box";

            // ← TAMBAH: Enhanced success message
            const message =
              data.message || "✅ Work Schedule berhasil diproses.";
            const performanceInfo = `
📊 Performance Summary:
   • Overall: ${overallDuration}ms
   • Get LastDate: ${getLastDateDuration}ms  
   • Data Processing: ${dataProcessingDuration}ms
   • API Call: ${apiCallDuration}ms
   • Input → Generated: ${rows.length} → ${finalData.length} records
   • Success Rate: ${Math.round((processedRows / rows.length) * 100)}%`;

            output.innerText = message + performanceInfo;

            // ← TAMBAH: Console summary
            console.log(
              `⚡ Work Schedule frontend completed in ${overallDuration}ms`
            );
            if (data.performance) {
              console.log(`   Backend performance:`, data.performance);
            }
          })
          .catch((err) => {
            output.className = "output-box error";
            output.innerText = "❌ Gagal mengirim data: " + err.message;
          });
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengambil lastdate: " + err.message;
      });

    return;
  }

  if (id === "substitution_daily") {
    const rows = input
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line !== "");

    if (rows.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Harap masukkan data Substitution Daily.";
      return;
    }

    function toHHmm(jamStr) {
      const [hh, mm] = jamStr.split(":");
      return `${hh.padStart(2, "0")}.${mm.padStart(2, "0")}`;
    }

    const tipeSpesial = ["PDKB", "PIKET", "OFF"];
    const dataMap = new Map();

    for (const row of rows) {
      const parts = row.split("\t");
      if (parts.length < 9) continue;

      const perner = parts[0];
      const tglMulai = parts[2];
      const tglAkhir = parts[3];
      const tipe = parts[8].trim();

      const [sd, sm, sy] = tglMulai.split("/");
      const [ed, em, ey] = tglAkhir.split("/");

      const startDate = new Date(`${sy}-${sm}-${sd}`);
      const endDate = new Date(`${ey}-${em}-${ed}`);

      const rawJamMulai = parts[6].trim();
      const rawJamAkhir = parts[7].trim();
      const tipeUpper = tipe.toUpperCase();
      const isTipeSpesial = tipeSpesial.includes(tipeUpper);

      // Khusus untuk Shift2-Malam ubah 24.00 jadi 00.00
      let jamMulai = isTipeSpesial ? "00.00" : toHHmm(rawJamMulai);
      let jamAkhir = isTipeSpesial ? "00.00" : toHHmm(rawJamAkhir);

      if (tipeUpper === "SHIFT2-MALAM") {
        if (jamMulai === "24.00") jamMulai = "00.00";
        if (jamAkhir === "24.00") jamAkhir = "00.00";
      }

      const nilai = `${tipe}~${jamMulai}~${jamAkhir}`;

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
    }

    const finalData = [];

    for (const [key, nilaiArray] of dataMap.entries()) {
      const [perner, tanggal] = key.split("||");

      // Buang duplikat nilai
      const unik = Array.from(new Set(nilaiArray));

      const jenis_shift =
        unik.length === 1
          ? unik[0] // hanya 1 jenis shift → langsung pakai
          : unik.join(" || "); // berbeda → tetap gabung pakai ||

      finalData.push({
        perner,
        tanggal,
        jenis_shift,
      });
    }

    if (finalData.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Tidak ada data valid untuk Substitution Daily.";
      return;
    }

    output.innerText = "⏳ Mengirim data ke server...";

    api("/update-substitution-daily", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ data: finalData }),
    })
      .then((res) => res.json())
      .then((data) => {
        const endTime = Date.now();
        const duration = endTime - startTime;

        output.className = "output-box";
        output.innerText =
          data.message ||
          `✅ Data Substitution Daily berhasil diproses dalam ${duration} ms.`;
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengirim data: " + err.message;
      });

    return;
  }

  if (id === "substitution_sap") {
    const rows = input
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line !== "");

    if (rows.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Harap masukkan data Substitution SAP.";
      return;
    }

    function toHHmm(jamStr) {
      const [hh, mm] = jamStr.split(":");
      return `${hh.padStart(2, "0")}.${mm.padStart(2, "0")}`;
    }

    const dataMap = new Map();
    let barisError = null;

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const parts = row.split("\t");

      if (parts.length === 7) {
        parts.push(""); // tambahkan kolom kosong di akhir agar jadi kolom ke-8
      }

      console.log(parts.length);

      if (parts.length < 8) continue;

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
        // Referensi shift
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
    }

    if (barisError !== null) {
      output.className = "output-box error";
      output.innerText =
        `❌ Baris ${barisError} memiliki jam shift yang tidak dikenali.\n` +
        `Proses dibatalkan. Periksa kembali data yang Anda tempel.`;
      return;
    }

    const finalData = [];

    for (const [key, nilaiArray] of dataMap.entries()) {
      const [perner, tanggal] = key.split("||");

      // Buang duplikat
      const unik = Array.from(new Set(nilaiArray));

      const jenis_shift = unik.length === 1 ? unik[0] : unik.join(" || ");

      finalData.push({
        perner,
        tanggal,
        jenis_shift,
      });
    }

    if (finalData.length === 0) {
      output.className = "output-box warning";
      output.innerText = "⚠️ Tidak ada data valid untuk Substitution SAP.";
      return;
    }

    output.innerText = "⏳ Mengirim data ke server...";

    api("/update-substitution-sap", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ data: finalData }),
    })
      .then((res) => res.json())
      .then((data) => {
        const endTime = Date.now();
        const duration = endTime - startTime;

        output.className = "output-box";
        output.innerText =
          data.message ||
          `✅ Data Substitution SAP berhasil diproses dalam ${duration} ms.`;
      })
      .catch((err) => {
        output.className = "output-box error";
        output.innerText = "❌ Gagal mengirim data: " + err.message;
      });

    return;
  }

  // Untuk jenis data lain tetap seperti sebelumnya
  const lines = input.split(/\r?\n/).filter((line) => line.trim() !== "");
  const jumlahBaris = lines.length;
  output.className = "output-box"; // Ensure default class is applied
  output.innerText =
    `✅ Data "${id
      .replace(/_/g, " ")
      .toUpperCase()}" berhasil diproses.\n📊 Jumlah baris: ${jumlahBaris}\n\n` +
    input;
}

function tampilkanJumlahBaris(id) {
  const textarea = document.getElementById(`${id}_textarea`);
  const output = document.getElementById(`${id}_output`);
  const input = textarea.value.trim();

  if (input) {
    const lines = input.split(/\r?\n/).filter((line) => line.trim() !== "");
    const jumlahBaris = lines.length;

    output.style.display = "block";
    output.className = "output-box"; // Ensure default class is applied
    output.innerText = `📊 Jumlah baris: ${jumlahBaris}`;
  } else {
    output.style.display = "none";
    output.innerText = "";
    output.className = "output-box"; // Reset class when empty
  }
}

function convertTanggalToMySQL(tgl) {
  const [d, m, y] = tgl.split("/");
  return `${y}-${m.padStart(2, "0")}-${d.padStart(2, "0")}`;
}

function hapusData(id) {
  document.getElementById(`${id}_textarea`).value = "";
  if (id === "data_pegawai") {
    document.getElementById(`${id}_start`).value = "";
    document.getElementById(`${id}_end`).value = "";
  }
  const output = document.getElementById(`${id}_output`);
  output.style.display = "none";
  output.innerText = "";
  output.className = "output-box"; // Reset class when cleared
}

function hapusSeluruhData() {
  if (
    !confirm("⚠️ Anda yakin ingin menghapus SEMUA data di tabel olah_absensi?")
  )
    return;

  api("/hapus-semua-data", {
    method: "POST",
  })
    .then((res) => res.json())
    .then((data) => {
      alert(data.message || "✅ Semua data berhasil dihapus.");
    })
    .catch((err) => {
      alert("❌ Gagal menghapus data: " + err.message);
    });
}

function hapusDataDB(id) {
  if (!confirm("⚠️ Yakin ingin menghapus data ini dari database?")) return;

  api("/hapus-data-db", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ jenis: id }),
  })
    .then((res) => res.json())
    .then((data) => {
      alert(data.message || "✅ Data berhasil dihapus dari database.");
    })
    .catch((err) => {
      alert("❌ Gagal menghapus data dari database: " + err.message);
    });
}

// This function was not used in the original script.js, but kept for completeness if needed.
// async function submitCICODailyData(text, resultBox) {
//   const cleanedData = parseCICODailyInput(text); // parseCICODailyInput is not defined in this context
//   if (cleanedData.length === 0) {
//     resultBox.innerText = "❌ Tidak ada data valid yang diproses.";
//     return;
//   }

//   try {
//     const res = await api("/update-ci-co", {
//       method: "POST",
//       headers: { "Content-Type": "application/json" },
//       body: JSON.stringify({ data: cleanedData }),
//     });

//     const json = await res.json();
//     resultBox.innerText = json.message || "✅ Data berhasil dikirim.";
//   } catch (err) {
//     console.error("❌ Error:", err);
//     resultBox.innerText = "❌ Gagal mengirim data ke server.";
//   }
// }

function formatJam(jamStr) {
  const [jam, menit] = jamStr.split(".");
  const jamFix = jam.padStart(2, "0");
  const menitFix = (menit || "00").padStart(2, "0");
  return `${jamFix}.${menitFix}`;
}

function prosesStatusGandaNormal() {
  const tombol = document.getElementById("prosesGandaNormalBtn");
  tombol.innerText = "⏳ Memproses status ganda/normal...";
  tombol.disabled = true;

  api("/update-status-ganda", {
    method: "POST",
  })
    .then((res) => res.json())
    .then((data) => {
      alert(data.message || "✅ Status ganda/normal berhasil diperbarui.");
    })
    .catch((err) => {
      alert("❌ Gagal memproses status ganda/normal: " + err.message);
    })
    .finally(() => {
      tombol.innerText = "⚙️ Proses Status Ganda/Normal";
      tombol.disabled = false;
    });
}

// Inisialisasi global
window.logJKP = [];
var logJKP = [];

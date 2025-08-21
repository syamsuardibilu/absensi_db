// ===================================================================
// 🧪 STANDALONE TEST FOR KETERANGAN KEHADIRAN LOGIC
// File: test-standalone.js
//
// Cara menjalankan:
// node test-standalone.js
// ===================================================================

const fs = require("fs");
const path = require("path");

// ===================================================================
// 🎯 LOGIC FUNCTIONS (COPY EXACT DARI YANG AKAN DIIMPLEMENTASI)
// ===================================================================

/**
 * Fungsi untuk menentukan apakah hari tersebut wajib kerja atau tidak
 */
function cekWajibKerja(status_jam_kerja, jenis_hari) {
  if (!status_jam_kerja) return false;

  const statusLower = String(status_jam_kerja).toLowerCase();
  const jenisHariLower = String(jenis_hari || "").toLowerCase();

  // Shift OFF = tidak wajib kerja
  if (statusLower.includes("off")) {
    return false;
  }

  // Normal di hari libur = tidak wajib kerja
  if (statusLower.includes("normal") && jenisHariLower.includes("libur")) {
    return false;
  }

  // Selain itu = wajib kerja
  // Termasuk: Shift (Pagi/Siang/Malam), PIKET, PDKB, Normal di hari kerja
  return true;
}

/**
 * Fungsi utama untuk menentukan keterangan_kehadiran
 */
function tentukanKeteranganKehadiran(row) {
  try {
    const { status_jam_kerja, jenis_hari, status_absen, value_att_abs } = row;

    // STEP 1: Cek apakah wajib kerja
    const isWajibKerja = cekWajibKerja(status_jam_kerja, jenis_hari);

    // Jika tidak wajib kerja = otomatis OK
    if (!isWajibKerja) {
      return "Dengan Absen/Dengan Keterangan";
    }

    // STEP 2: Wajib kerja - cek ada absen lengkap
    const adaAbsenLengkap = status_absen === "Lengkap";

    // STEP 3: Wajib kerja - cek ada keterangan valid
    let adaKeteranganValid = false;
    if (value_att_abs && value_att_abs.trim() !== "") {
      const valueAttAbs = String(value_att_abs).toLowerCase();

      // Keterangan valid: cuti, ijin, SPPD, atau attendance yang dijustifikasi
      adaKeteranganValid =
        valueAttAbs.includes("abs_") || // Absence (cuti/ijin)
        valueAttAbs.includes("sppd_") || // SPPD/tugas luar
        valueAttAbs.includes("att_") || // Attendance justification
        valueAttAbs.includes("cuti") || // Explicit cuti
        valueAttAbs.includes("ijin") || // Explicit ijin
        valueAttAbs.includes("sakit"); // Sakit
    }

    // STEP 4: Final decision
    const result =
      adaAbsenLengkap || adaKeteranganValid
        ? "Dengan Absen/Dengan Keterangan"
        : "Tanpa Keterangan";

    return result;
  } catch (error) {
    console.error(
      `❌ Error dalam tentukanKeteranganKehadiran untuk ${row.perner}:`,
      error
    );
    return "Dengan Absen/Dengan Keterangan";
  }
}

// ===================================================================
// 🧪 TEST CASES
// ===================================================================

const testCases = [
  {
    name: "Normal worker, hari kerja, absen lengkap",
    data: {
      perner: "TEST001",
      tanggal: "2025-01-15",
      status_jam_kerja: "Normal => (08:00-17:00)",
      jenis_hari: "HARI KERJA",
      status_absen: "Lengkap",
      value_att_abs: null,
    },
    expected: "Dengan Absen/Dengan Keterangan",
    category: "Normal Scenarios",
  },
  {
    name: "Normal worker, hari kerja, tidak absen, tanpa keterangan",
    data: {
      perner: "TEST002",
      tanggal: "2025-01-15",
      status_jam_kerja: "Normal => (08:00-17:00)",
      jenis_hari: "HARI KERJA",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: null,
    },
    expected: "Tanpa Keterangan",
    category: "Problem Cases",
  },
  {
    name: "Normal worker, hari kerja, tidak absen, tapi ada cuti",
    data: {
      perner: "TEST003",
      tanggal: "2025-01-15",
      status_jam_kerja: "Normal => (08:00-17:00)",
      jenis_hari: "HARI KERJA",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: "abs_sap_new => Cuti",
    },
    expected: "Dengan Absen/Dengan Keterangan",
    category: "Justified Absence",
  },
  {
    name: "Shift worker, hari libur, ada jadwal",
    data: {
      perner: "TEST004",
      tanggal: "2025-01-12",
      status_jam_kerja: "Shift => Pagi (08:00-16:00)",
      jenis_hari: "HARI LIBUR",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: null,
    },
    expected: "Tanpa Keterangan",
    category: "Shift Work",
  },
  {
    name: "Shift OFF",
    data: {
      perner: "TEST005",
      tanggal: "2025-01-15",
      status_jam_kerja: "Shift => OFF",
      jenis_hari: "HARI KERJA",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: null,
    },
    expected: "Dengan Absen/Dengan Keterangan",
    category: "Off Days",
  },
  {
    name: "Normal worker di hari libur",
    data: {
      perner: "TEST006",
      tanggal: "2025-01-12",
      status_jam_kerja: "Normal => (08:00-17:00)",
      jenis_hari: "HARI LIBUR",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: null,
    },
    expected: "Dengan Absen/Dengan Keterangan",
    category: "Holiday Scenarios",
  },
  {
    name: "PIKET di hari libur",
    data: {
      perner: "TEST007",
      tanggal: "2025-01-12",
      status_jam_kerja: "Normal => PIKET (08:00-17:00)",
      jenis_hari: "HARI LIBUR",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: null,
    },
    expected: "Tanpa Keterangan",
    category: "Special Duty",
  },
  {
    name: "Worker dengan SPPD",
    data: {
      perner: "TEST008",
      tanggal: "2025-01-15",
      status_jam_kerja: "Normal => (08:00-17:00)",
      jenis_hari: "HARI KERJA",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: "sppd_umum_new => Perjalanan dinas",
    },
    expected: "Dengan Absen/Dengan Keterangan",
    category: "Business Trip",
  },
  {
    name: "PDKB di hari libur",
    data: {
      perner: "TEST009",
      tanggal: "2025-01-12",
      status_jam_kerja: "Normal => PDKB (08:00-17:00)",
      jenis_hari: "HARI LIBUR",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: null,
    },
    expected: "Tanpa Keterangan",
    category: "Special Duty",
  },
  {
    name: "Worker dengan ijin sakit",
    data: {
      perner: "TEST010",
      tanggal: "2025-01-15",
      status_jam_kerja: "Normal => (08:00-17:00)",
      jenis_hari: "HARI KERJA",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: "abs_daily_new => sakit",
    },
    expected: "Dengan Absen/Dengan Keterangan",
    category: "Medical Leave",
  },
  {
    name: "Shift malam, hari kerja, tidak absen",
    data: {
      perner: "TEST011",
      tanggal: "2025-01-15",
      status_jam_kerja: "Shift => Malam (00:00-08:00)",
      jenis_hari: "HARI KERJA",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: null,
    },
    expected: "Tanpa Keterangan",
    category: "Shift Work",
  },
  {
    name: "Worker dengan ijin explicit",
    data: {
      perner: "TEST012",
      tanggal: "2025-01-15",
      status_jam_kerja: "Normal => (08:00-17:00)",
      jenis_hari: "HARI KERJA",
      status_absen: "Tidak lengkap -> tidak absen",
      value_att_abs: "att_daily_new => ijin keluarga",
    },
    expected: "Dengan Absen/Dengan Keterangan",
    category: "Personal Leave",
  },
];

// ===================================================================
// 🧪 TEST RUNNER
// ===================================================================

function runTests() {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const logFile = path.join(
    __dirname,
    `test-keterangan-kehadiran-${timestamp}.txt`
  );

  let logContent = "";

  function log(message) {
    logContent += message + "\n";
    console.log(message);
  }

  console.log(
    "🚀 Starting standalone test for KETERANGAN KEHADIRAN logic...\n"
  );

  log(
    "================================================================================"
  );
  log("🧪 KETERANGAN KEHADIRAN - STANDALONE TEST RESULTS");
  log(
    "================================================================================"
  );
  log(`⏰ Test Run Time: ${new Date().toLocaleString("id-ID")}`);
  log(`📁 Log File: ${logFile}`);
  log(`🧪 Total Test Cases: ${testCases.length}`);
  log(
    "================================================================================\n"
  );

  let passedTests = 0;
  let failedTests = 0;
  const results = [];
  const categories = {};

  // Group test cases by category
  testCases.forEach((testCase) => {
    if (!categories[testCase.category]) {
      categories[testCase.category] = [];
    }
    categories[testCase.category].push(testCase);
  });

  // Run tests by category
  Object.keys(categories).forEach((categoryName) => {
    log(`📂 CATEGORY: ${categoryName.toUpperCase()}`);
    log(
      "================================================================================"
    );

    categories[categoryName].forEach((testCase, index) => {
      const globalIndex = testCases.indexOf(testCase) + 1;

      log(`\n🔍 TEST ${globalIndex}: ${testCase.name}`);
      log(
        "--------------------------------------------------------------------------------"
      );
      log("📋 INPUT:");
      log(`   PERNER: ${testCase.data.perner}`);
      log(`   Tanggal: ${testCase.data.tanggal}`);
      log(`   Status Jam Kerja: ${testCase.data.status_jam_kerja}`);
      log(`   Jenis Hari: ${testCase.data.jenis_hari}`);
      log(`   Status Absen: ${testCase.data.status_absen}`);
      log(`   Value Att Abs: ${testCase.data.value_att_abs || "NULL"}`);
      log("");

      try {
        // Test cekWajibKerja first
        const isWajibKerja = cekWajibKerja(
          testCase.data.status_jam_kerja,
          testCase.data.jenis_hari
        );
        log(`🔧 LOGIC BREAKDOWN:`);
        log(`   Is Wajib Kerja: ${isWajibKerja}`);

        if (isWajibKerja) {
          const adaAbsenLengkap = testCase.data.status_absen === "Lengkap";
          const adaKeteranganValid =
            testCase.data.value_att_abs &&
            testCase.data.value_att_abs.trim() !== "" &&
            (testCase.data.value_att_abs.toLowerCase().includes("abs_") ||
              testCase.data.value_att_abs.toLowerCase().includes("sppd_") ||
              testCase.data.value_att_abs.toLowerCase().includes("att_") ||
              testCase.data.value_att_abs.toLowerCase().includes("cuti") ||
              testCase.data.value_att_abs.toLowerCase().includes("ijin") ||
              testCase.data.value_att_abs.toLowerCase().includes("sakit"));

          log(`   Ada Absen Lengkap: ${adaAbsenLengkap}`);
          log(`   Ada Keterangan Valid: ${adaKeteranganValid}`);
        }
        log("");

        // Run main function
        const result = tentukanKeteranganKehadiran(testCase.data);
        const isPass = result === testCase.expected;

        log(`🎯 RESULT:`);
        log(`   Expected: "${testCase.expected}"`);
        log(`   Actual:   "${result}"`);
        log(`   Status:   ${isPass ? "✅ PASS" : "❌ FAIL"}`);

        if (isPass) {
          passedTests++;
        } else {
          failedTests++;
        }

        results.push({
          test: globalIndex,
          name: testCase.name,
          category: testCase.category,
          status: isPass ? "PASS" : "FAIL",
          expected: testCase.expected,
          actual: result,
          isWajibKerja: isWajibKerja,
        });
      } catch (error) {
        log(`🎯 RESULT:`);
        log(`   Expected: "${testCase.expected}"`);
        log(`   Actual:   ERROR`);
        log(`   Status:   ❌ ERROR`);
        log(`   Error:    ${error.message}`);

        failedTests++;
        results.push({
          test: globalIndex,
          name: testCase.name,
          category: testCase.category,
          status: "ERROR",
          expected: testCase.expected,
          actual: null,
          error: error.message,
        });
      }
    });

    log(
      "\n================================================================================\n"
    );
  });

  // Summary
  const successRate = ((passedTests / testCases.length) * 100).toFixed(1);

  log("📊 FINAL SUMMARY");
  log(
    "================================================================================"
  );
  log(`✅ PASSED: ${passedTests}/${testCases.length}`);
  log(`❌ FAILED: ${failedTests}/${testCases.length}`);
  log(`📈 SUCCESS RATE: ${successRate}%`);
  log(
    "================================================================================\n"
  );

  // Category breakdown
  log("📂 RESULTS BY CATEGORY:");
  log(
    "================================================================================"
  );
  Object.keys(categories).forEach((categoryName) => {
    const categoryResults = results.filter((r) => r.category === categoryName);
    const categoryPassed = categoryResults.filter(
      (r) => r.status === "PASS"
    ).length;
    const categoryTotal = categoryResults.length;
    const categoryRate = ((categoryPassed / categoryTotal) * 100).toFixed(1);

    log(
      `📁 ${categoryName}: ${categoryPassed}/${categoryTotal} (${categoryRate}%)`
    );
  });
  log(
    "================================================================================\n"
  );

  // Failed tests detail
  if (failedTests > 0) {
    log("❌ FAILED TEST DETAILS:");
    log(
      "================================================================================"
    );
    results
      .filter((r) => r.status !== "PASS")
      .forEach((result) => {
        log(`${result.test}. ${result.name}`);
        log(`   Category: ${result.category}`);
        log(`   Expected: "${result.expected}"`);
        log(`   Actual: "${result.actual || "ERROR"}"`);
        if (result.error) {
          log(`   Error: ${result.error}`);
        }
        log("");
      });
    log(
      "================================================================================\n"
    );
  }

  // Logic documentation
  log("📚 LOGIC DOCUMENTATION:");
  log(
    "================================================================================"
  );
  log("🔧 Function: cekWajibKerja(status_jam_kerja, jenis_hari)");
  log("   Returns FALSE if:");
  log('   - status_jam_kerja contains "off" (case insensitive)');
  log(
    '   - status_jam_kerja contains "normal" AND jenis_hari contains "libur"'
  );
  log("   Returns TRUE for all other cases");
  log("");
  log("🎯 Function: tentukanKeteranganKehadiran(row)");
  log('   If NOT wajib kerja → "Dengan Absen/Dengan Keterangan"');
  log("   If wajib kerja:");
  log('     - Check status_absen === "Lengkap"');
  log(
    "     - Check value_att_abs contains: abs_, sppd_, att_, cuti, ijin, sakit"
  );
  log('     - If either is true → "Dengan Absen/Dengan Keterangan"');
  log('     - If both false → "Tanpa Keterangan"');
  log(
    "================================================================================\n"
  );

  log(`📁 Test completed successfully!`);
  log(`📄 Log file: ${logFile}`);
  log(`⏰ Finished at: ${new Date().toLocaleString("id-ID")}`);

  // Save to file
  try {
    fs.writeFileSync(logFile, logContent, "utf8");
    console.log(`\n✅ Test results saved to: ${logFile}`);
  } catch (error) {
    console.log(`\n❌ Failed to save log file: ${error.message}`);
  }

  return {
    passed: passedTests,
    failed: failedTests,
    total: testCases.length,
    successRate: successRate,
    logFile: logFile,
  };
}

// ===================================================================
// 🚀 RUN TEST
// ===================================================================

console.log("🧪 KETERANGAN KEHADIRAN - STANDALONE TEST");
console.log("==========================================");
console.log("");

runTests();

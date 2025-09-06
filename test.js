// Tambahkan function helper yang mungkin missing
function formatTanggalSafeMatrix(date) {
  if (!date) return null;

  // Convert to MySQL date format: YYYY-MM-DD
  if (date instanceof Date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }

  return date;
}

// Perbaikan endpoint dengan error handling yang lebih baik
app.get("/getAttendanceMatrix", (req, res) => {
  const { month, year } = req.query;
  const startTime = Date.now();

  console.log(`🔄 Starting attendance matrix request for ${month}/${year}`);

  try {
    // Validate required parameters
    if (!month || !year) {
      console.error("❌ Missing parameters:", { month, year });
      return res.status(400).json({
        success: false,
        message: "Parameter month dan year wajib diisi",
        error: "Missing required parameters",
      });
    }

    const monthNum = parseInt(month);
    const yearNum = parseInt(year);

    // Validate month range
    if (monthNum < 1 || monthNum > 12 || isNaN(monthNum)) {
      console.error("❌ Invalid month:", monthNum);
      return res.status(400).json({
        success: false,
        message: "Month harus antara 1-12",
        error: "Invalid month parameter",
      });
    }

    // Validate year range
    if (yearNum < 2020 || yearNum > 2030 || isNaN(yearNum)) {
      console.error("❌ Invalid year:", yearNum);
      return res.status(400).json({
        success: false,
        message: "Year harus antara 2020-2030",
        error: "Invalid year parameter",
      });
    }

    // Calculate date range for the month
    const startDate = new Date(yearNum, monthNum - 1, 1);
    const endDate = new Date(yearNum, monthNum, 0); // Last day of the month
    const daysInMonth = endDate.getDate();

    const startDateStr = formatTanggalSafeMatrix(startDate);
    const endDateStr = formatTanggalSafeMatrix(endDate);

    console.log(
      `📅 Matrix period: ${startDateStr} to ${endDateStr} (${daysInMonth} days)`
    );

    // Test database connection first
    if (!conn) {
      console.error("❌ Database connection not available");
      return res.status(500).json({
        success: false,
        message: "Database connection error",
        error: "Database not connected",
      });
    }

    // Optimized SQL query with better error handling
    const sql = `
      SELECT 
        oa.perner,
        DATE(oa.tanggal) as tanggal_only,
        DAY(oa.tanggal) as day_number,
        oa.status_absen,
        oa.daily_in_cleansing,
        oa.daily_out_cleansing,
        oa.value_att_abs,
        oa.status_jam_kerja,
        oa.jenis_hari,
        oa.correction_in,
        oa.correction_out,
        COALESCE(dp.nama, CONCAT('Pegawai ', oa.perner)) as nama,
        COALESCE(dp.nip, '') as nip,
        COALESCE(dp.bidang, 'Belum Diisi') as bidang,
        dp.no_telp
      FROM olah_absensi oa
      LEFT JOIN data_pegawai dp ON oa.perner = dp.perner
      WHERE oa.tanggal >= ? AND oa.tanggal <= ?
        AND oa.perner IS NOT NULL
      ORDER BY oa.perner ASC, oa.tanggal ASC
    `;

    console.log("🔍 Executing SQL query...");

    conn.query(sql, [startDateStr, endDateStr], (err, results) => {
      if (err) {
        console.error("❌ Database query error:", err);
        return res.status(500).json({
          success: false,
          message: "Gagal mengambil data matrix absensi",
          error: err.message,
          sqlError: err.code,
        });
      }

      const queryDuration = Date.now() - startTime;
      console.log(
        `✅ Query completed: ${results.length} records in ${queryDuration}ms`
      );

      if (results.length === 0) {
        console.log("⚠️ No data found for the period");
        return res.json({
          success: true,
          message: `Tidak ada data absensi untuk ${getMonthName(
            monthNum
          )} ${yearNum}`,
          period: {
            month: monthNum,
            year: yearNum,
            monthName: getMonthName(monthNum),
            daysInMonth: daysInMonth,
            dateRange: `${startDateStr} - ${endDateStr}`,
          },
          matrix: [],
          statistics: {
            totalEmployees: 0,
            totalRecords: 0,
            coverage: 0,
            statusBreakdown: {
              lengkap: 0,
              tidak_lengkap: 0,
              cuti_ijin: 0,
              hari_libur: 0,
              no_data: 0,
            },
          },
          performance: {
            queryDuration: queryDuration,
            processingDuration: 0,
            totalDuration: queryDuration,
          },
          generatedAt: new Date().toISOString(),
        });
      }

      try {
        // Process the matrix data
        console.log("🔄 Processing matrix data...");
        const processStart = Date.now();
        const { matrixData, statistics } = processAttendanceMatrix(
          results,
          daysInMonth
        );
        const processingDuration = Date.now() - processStart;

        const totalDuration = Date.now() - startTime;

        console.log(
          `✅ Matrix processing completed: ${matrixData.length} employees, ${statistics.totalRecords} records`
        );
        console.log(
          `⚡ Performance: Query=${queryDuration}ms, Processing=${processingDuration}ms, Total=${totalDuration}ms`
        );

        res.json({
          success: true,
          message: `Data matrix absensi berhasil diambil untuk ${getMonthName(
            monthNum
          )} ${yearNum}`,
          period: {
            month: monthNum,
            year: yearNum,
            monthName: getMonthName(monthNum),
            daysInMonth: daysInMonth,
            dateRange: `${startDateStr} - ${endDateStr}`,
          },
          matrix: matrixData,
          statistics: statistics,
          performance: {
            queryDuration: queryDuration,
            processingDuration: processingDuration,
            totalDuration: totalDuration,
            recordsPerSecond: Math.round(
              results.length / (totalDuration / 1000)
            ),
          },
          generatedAt: new Date().toISOString(),
        });
      } catch (processError) {
        console.error("❌ Matrix processing error:", processError);
        return res.status(500).json({
          success: false,
          message: "Error processing matrix data",
          error: processError.message,
          phase: "data_processing",
        });
      }
    });
  } catch (generalError) {
    console.error("❌ General error in getAttendanceMatrix:", generalError);
    return res.status(500).json({
      success: false,
      message: "Internal server error",
      error: generalError.message,
      phase: "general",
    });
  }
});

// Helper function dengan error handling
function processAttendanceMatrix(rawData, daysInMonth) {
  try {
    console.log(
      `🔄 Processing ${rawData.length} raw records for ${daysInMonth} days`
    );

    const employeeMap = new Map();
    const statistics = {
      totalEmployees: 0,
      totalRecords: rawData.length,
      coverage: 0,
      statusBreakdown: {
        lengkap: 0,
        tidak_lengkap: 0,
        cuti_ijin: 0,
        hari_libur: 0,
        no_data: 0,
      },
    };

    // Group data by employee
    rawData.forEach((record, index) => {
      try {
        const perner = record.perner;

        if (!perner) {
          console.warn(`⚠️ Record ${index} has no perner, skipping`);
          return;
        }

        if (!employeeMap.has(perner)) {
          employeeMap.set(perner, {
            perner: perner,
            nama: record.nama || `Pegawai ${perner}`,
            nip: record.nip || "",
            bidang: record.bidang || "Belum Diisi",
            no_telp: record.no_telp || "",
            attendanceData: {},
            summary: {
              total_days: 0,
              lengkap: 0,
              tidak_lengkap: 0,
              cuti_ijin: 0,
              hari_libur: 0,
              no_data: 0,
            },
          });
        }

        const employee = employeeMap.get(perner);
        const day = record.day_number;

        if (!day || day < 1 || day > 31) {
          console.warn(`⚠️ Invalid day number ${day} for record ${index}`);
          return;
        }

        // Map status_absen to display codes
        let statusCode = "-";
        let cssClass = "status-empty";
        const statusAbsen = record.status_absen;

        if (statusAbsen === "-") {
          statusCode = "C/I";
          cssClass = "status-ci";
          statistics.statusBreakdown.cuti_ijin++;
          employee.summary.cuti_ijin++;
        } else if (statusAbsen === "Bukan hari kerja") {
          statusCode = "HL";
          cssClass = "status-hl";
          statistics.statusBreakdown.hari_libur++;
          employee.summary.hari_libur++;
        } else if (statusAbsen === "Lengkap") {
          statusCode = "L";
          cssClass = "status-l";
          statistics.statusBreakdown.lengkap++;
          employee.summary.lengkap++;
        } else if (
          statusAbsen &&
          statusAbsen.toLowerCase().includes("tidak lengkap")
        ) {
          statusCode = "TL";
          cssClass = "status-tl";
          statistics.statusBreakdown.tidak_lengkap++;
          employee.summary.tidak_lengkap++;
        } else {
          statistics.statusBreakdown.no_data++;
          employee.summary.no_data++;
        }

        // Store attendance data for this day
        employee.attendanceData[day] = {
          status: statusCode,
          cssClass: cssClass,
          originalStatus: statusAbsen,
          tanggal: record.tanggal_only,
          daily_in_cleansing: record.daily_in_cleansing,
          daily_out_cleansing: record.daily_out_cleansing,
          keterangan: record.value_att_abs,
          jenis_hari: record.jenis_hari,
          status_jam_kerja: record.status_jam_kerja,
          correction_in: record.correction_in,
          correction_out: record.correction_out,
          tooltip: `${day}: ${statusAbsen || "Tidak ada data"}`,
        };

        employee.summary.total_days++;
      } catch (recordError) {
        console.error(`❌ Error processing record ${index}:`, recordError);
      }
    });

    // Fill missing days with empty status
    employeeMap.forEach((employee) => {
      for (let day = 1; day <= daysInMonth; day++) {
        if (!employee.attendanceData[day]) {
          employee.attendanceData[day] = {
            status: "-",
            cssClass: "status-empty",
            originalStatus: null,
            tanggal: null,
            daily_in_cleansing: null,
            daily_out_cleansing: null,
            keterangan: null,
            jenis_hari: null,
            status_jam_kerja: null,
            correction_in: null,
            correction_out: null,
            tooltip: `${day}: Tidak ada data`,
          };
        }
      }
    });

    // Convert to array and sort
    const matrixData = Array.from(employeeMap.values()).sort((a, b) =>
      a.perner.localeCompare(b.perner)
    );

    // Calculate final statistics
    statistics.totalEmployees = matrixData.length;
    const totalPossibleRecords = statistics.totalEmployees * daysInMonth;
    statistics.coverage =
      totalPossibleRecords > 0
        ? Math.round(
            ((totalPossibleRecords - statistics.statusBreakdown.no_data) /
              totalPossibleRecords) *
              100
          )
        : 0;

    console.log(
      `✅ Processing complete: ${statistics.totalEmployees} employees processed`
    );

    return { matrixData, statistics };
  } catch (error) {
    console.error("❌ Critical error in processAttendanceMatrix:", error);
    throw new Error(`Matrix processing failed: ${error.message}`);
  }
}

// Helper function for month names
function getMonthName(month) {
  const months = [
    "",
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
  return months[month] || `Month ${month}`;
}

// Test endpoint untuk debugging
app.get("/test-db", (req, res) => {
  if (!conn) {
    return res.status(500).json({
      success: false,
      error: "Database connection not available",
    });
  }

  conn.query(
    "SELECT COUNT(*) as count FROM olah_absensi LIMIT 1",
    (err, results) => {
      if (err) {
        return res.status(500).json({
          success: false,
          error: err.message,
          sqlError: err.code,
        });
      }

      res.json({
        success: true,
        message: "Database connection OK",
        totalRecords: results[0].count,
        timestamp: new Date().toISOString(),
      });
    }
  );
});

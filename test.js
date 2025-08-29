// ENHANCED BULK UPLOAD WITH DUPLICATE HANDLING OPTIONS
app.post("/api/data_pegawai/bulk_upload", (req, res) => {
  const startTime = Date.now();
  console.log(
    "📁 Starting bulk upload data pegawai with duplicate handling..."
  );

  const {
    data,
    filename,
    total_records,
    duplicate_action = "overwrite",
  } = req.body;

  console.log(`🔧 Duplicate handling mode: ${duplicate_action}`);

  // Input validation (same as before)
  if (!data || !Array.isArray(data) || data.length === 0) {
    return res.status(400).json({
      success: false,
      message: "❌ Data tidak valid atau kosong",
      error: "No data provided",
    });
  }

  if (data.length > 5000) {
    return res.status(400).json({
      success: false,
      message: "❌ Terlalu banyak data. Maksimal 5000 records per upload",
      error: "Record limit exceeded",
    });
  }

  // Validation logic (same as before)
  const validatedData = [];
  const errors = [];
  const pernerSet = new Set();
  const nipSet = new Set();

  const getFieldValue = (row, field) => {
    const keys = Object.keys(row);
    const matchingKey = keys.find(
      (key) =>
        key.toLowerCase().includes(field.toLowerCase()) ||
        field.toLowerCase().includes(key.toLowerCase())
    );
    return matchingKey ? String(row[matchingKey] || "").trim() : "";
  };

  // Same validation logic as before...
  data.forEach((row, index) => {
    const rowNum = index + 1;
    const errors_for_row = [];

    const perner = getFieldValue(row, "perner");
    const nip = getFieldValue(row, "nip");
    const nama = getFieldValue(row, "nama");
    const bidang = getFieldValue(row, "bidang");
    const no_telp =
      getFieldValue(row, "no_telp") ||
      getFieldValue(row, "phone") ||
      getFieldValue(row, "telp");

    // Validation logic (same as before)
    if (!perner) errors_for_row.push("PERNER kosong");
    if (!nip) errors_for_row.push("NIP kosong");
    if (!nama) errors_for_row.push("Nama kosong");
    if (!bidang) errors_for_row.push("Bidang kosong");

    // Phone number cleaning (same as before)
    let final_no_telp = no_telp || "";
    if (final_no_telp.trim()) {
      let cleanPhone = final_no_telp.trim();
      if (cleanPhone.startsWith("+62")) {
        cleanPhone = "0" + cleanPhone.substring(3);
      } else if (cleanPhone.startsWith("62") && cleanPhone.length > 10) {
        cleanPhone = "0" + cleanPhone.substring(2);
      }
      cleanPhone = cleanPhone.replace(/[^\d]/g, "");
      if (
        cleanPhone.length >= 10 &&
        cleanPhone.length <= 15 &&
        (cleanPhone.startsWith("08") || cleanPhone.startsWith("628"))
      ) {
        if (cleanPhone.startsWith("628")) {
          cleanPhone = "0" + cleanPhone.substring(3);
        }
        final_no_telp = cleanPhone;
      } else {
        final_no_telp = "-";
      }
    } else {
      final_no_telp = "-";
    }

    // Check duplicates and other validations (same as before)
    if (perner && pernerSet.has(perner)) {
      errors_for_row.push(`PERNER "${perner}" duplikat dalam file`);
    } else if (perner) {
      pernerSet.add(perner);
    }

    if (nip && nipSet.has(nip)) {
      errors_for_row.push(`NIP "${nip}" duplikat dalam file`);
    } else if (nip) {
      nipSet.add(nip);
    }

    if (errors_for_row.length > 0) {
      errors.push({
        row: rowNum,
        perner: perner || "N/A",
        message: errors_for_row.join(", "),
      });
    } else {
      validatedData.push({
        perner,
        nip,
        nama,
        bidang,
        no_telp: final_no_telp,
      });
    }
  });

  if (errors.length > 0) {
    return res.status(400).json({
      success: false,
      message: `❌ Validasi gagal. ${errors.length} baris mengandung error`,
      data: {
        total_records: data.length,
        valid_records: validatedData.length,
        invalid_records: errors.length,
        errors: errors.slice(0, 50),
        validation_duration_ms: Date.now() - startTime,
      },
    });
  }

  // ENHANCED DATABASE OPERATIONS BASED ON DUPLICATE ACTION
  console.log(
    `💾 Starting database operations for ${validatedData.length} records with action: ${duplicate_action}`
  );

  // First, handle backup option
  if (duplicate_action === "backup") {
    // Create history table if it doesn't exist
    const createHistorySQL = `
      CREATE TABLE IF NOT EXISTS data_pegawai_history (
        id INT AUTO_INCREMENT PRIMARY KEY,
        perner VARCHAR(50),
        nip VARCHAR(50),
        nama VARCHAR(100),
        bidang VARCHAR(100),
        no_telp VARCHAR(20),
        backup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        backup_reason VARCHAR(100) DEFAULT 'bulk_upload_backup'
      )
    `;

    conn.query(createHistorySQL, (createErr) => {
      if (createErr) {
        console.warn(
          "⚠️ Warning: Could not create history table:",
          createErr.message
        );
      }
      handleDuplicates();
    });
  } else {
    handleDuplicates();
  }

  function handleDuplicates() {
    // Check for existing records
    const pernerList = validatedData.map((item) => item.perner);
    const pernerCheckSQL = `
      SELECT perner, nip, nama, bidang, no_telp FROM data_pegawai 
      WHERE perner IN (${pernerList.map(() => "?").join(",")})
    `;

    conn.query(pernerCheckSQL, pernerList, (checkErr, existingRecords) => {
      if (checkErr) {
        console.error("❌ Error checking existing PERNER:", checkErr);
        return res.status(500).json({
          success: false,
          message: "❌ Gagal mengecek data yang sudah ada",
          error: checkErr.message,
        });
      }

      const existingMap = new Map();
      existingRecords.forEach((record) => {
        existingMap.set(record.perner, record);
      });

      console.log(`🔍 Found ${existingMap.size} existing PERNER in database`);

      let newInserts = [];
      let updates = [];
      let skipped = [];
      let backedUp = [];

      // Process each record based on duplicate action
      validatedData.forEach((item) => {
        if (existingMap.has(item.perner)) {
          switch (duplicate_action) {
            case "skip":
              skipped.push(item);
              break;
            case "overwrite":
            case "backup":
              updates.push(item);
              break;
            case "merge":
              // Merge: only update non-empty fields
              const existing = existingMap.get(item.perner);
              const mergedItem = {
                perner: item.perner,
                nip: item.nip || existing.nip,
                nama: item.nama || existing.nama,
                bidang: item.bidang || existing.bidang,
                no_telp: item.no_telp || existing.no_telp,
              };
              updates.push(mergedItem);
              break;
            default:
              updates.push(item);
          }
        } else {
          newInserts.push(item);
        }
      });

      console.log(
        `📊 Operations planned: ${newInserts.length} inserts, ${updates.length} updates, ${skipped.length} skipped`
      );

      // Execute operations
      executeOperations();

      function executeOperations() {
        let completedOperations = 0;
        let successfulInserts = 0;
        let updatedRecords = 0;
        let skippedRecords = skipped.length;
        let failedInserts = 0;
        const operationErrors = [];

        const totalOperations = newInserts.length + updates.length;

        const checkComplete = () => {
          if (completedOperations === totalOperations) {
            const endTime = Date.now();
            const duration = endTime - startTime;
            const successfulOperations = successfulInserts + updatedRecords;

            console.log(`🎉 Bulk upload completed in ${duration}ms`);
            console.log(
              `📊 Results: ${successfulInserts} inserted, ${updatedRecords} updated, ${skippedRecords} skipped, ${failedInserts} failed`
            );

            res.json({
              success: true,
              message: `✅ Upload berhasil dengan mode "${duplicate_action}"! ${successfulOperations} dari ${totalOperations} records berhasil diproses`,
              data: {
                filename: filename || "pasted_data",
                total_records: data.length,
                successful_inserts: successfulInserts,
                updated_records: updatedRecords,
                skipped_records: skippedRecords,
                failed_inserts: failedInserts,
                duplicate_action: duplicate_action,
                errors: operationErrors.slice(0, 20),
                processing_duration_ms: duration,
                success_rate: Math.round(
                  (successfulOperations / validatedData.length) * 100
                ),
                method: "bulk_upload_with_duplicate_handling",
              },
              timestamp: new Date().toISOString(),
            });
          }
        };

        // Handle backup before updates
        if (duplicate_action === "backup" && updates.length > 0) {
          const backupSQL = `
            INSERT INTO data_pegawai_history (perner, nip, nama, bidang, no_telp, backup_reason)
            SELECT perner, nip, nama, bidang, no_telp, 'pre_bulk_update'
            FROM data_pegawai
            WHERE perner IN (${updates.map(() => "?").join(",")})
          `;

          const backupParams = updates.map((item) => item.perner);

          conn.query(backupSQL, backupParams, (backupErr, backupResult) => {
            if (backupErr) {
              console.error("❌ Backup error:", backupErr);
              operationErrors.push(`Backup failed: ${backupErr.message}`);
            } else {
              console.log(`💾 Backed up ${backupResult.affectedRows} records`);
            }
            proceedWithOperations();
          });
        } else {
          proceedWithOperations();
        }

        function proceedWithOperations() {
          // Handle new inserts
          if (newInserts.length > 0) {
            const insertSQL = `
              INSERT INTO data_pegawai (perner, nip, nama, bidang, no_telp) 
              VALUES ${newInserts.map(() => "(?, ?, ?, ?, ?)").join(", ")}
            `;

            const insertParams = [];
            newInserts.forEach((item) => {
              insertParams.push(
                item.perner,
                item.nip,
                item.nama,
                item.bidang,
                item.no_telp
              );
            });

            conn.query(insertSQL, insertParams, (insertErr, insertResult) => {
              if (insertErr) {
                console.error("❌ Bulk insert error:", insertErr);
                operationErrors.push(
                  `Bulk insert failed: ${insertErr.message}`
                );
                failedInserts += newInserts.length;
              } else {
                successfulInserts = insertResult.affectedRows;
                console.log(`✅ Inserted ${successfulInserts} new records`);
              }

              completedOperations += newInserts.length;
              checkComplete();
            });
          }

          // Handle updates
          if (updates.length === 0) {
            completedOperations += 0;
            checkComplete();
          } else {
            updates.forEach((item) => {
              const updateSQL = `
                UPDATE data_pegawai 
                SET nip = ?, nama = ?, bidang = ?, no_telp = ?
                WHERE perner = ?
              `;

              conn.query(
                updateSQL,
                [item.nip, item.nama, item.bidang, item.no_telp, item.perner],
                (updateErr, updateResult) => {
                  if (updateErr) {
                    console.error(
                      `❌ Update error for PERNER ${item.perner}:`,
                      updateErr
                    );
                    operationErrors.push(
                      `Update failed for PERNER ${item.perner}: ${updateErr.message}`
                    );
                    failedInserts++;
                  } else if (updateResult.affectedRows > 0) {
                    updatedRecords++;
                  }

                  completedOperations++;
                  checkComplete();
                }
              );
            });
          }
        }
      }
    });
  }
});

app.post("/update-status-ganda", (req, res) => {
  console.log("🔄 Starting SIMPLIFIED status ganda processing...");
  const overallStartTime = Date.now();

  // STEP 1: Reset (same as before)
  const resetSQL = `
    UPDATE ${tables.OLAH_ABSEN} SET
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
      FROM ${tables.OLAH_ABSEN} WHERE tanggal IS NOT NULL ORDER BY perner, tanggal
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

        // Jika ada lebih dari 1 sumber dan berbeda → Ganda
        if (nilaiIsi.length > 1) {
          const unik = new Set(nilaiIsi);
          if (unik.size > 1) status = "Ganda";
        }

        // Jika ada "||" di salah satu sumber → Ganda
        if (nilaiIsi.some((v) => v.includes("||"))) {
          status = "Ganda";
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
              UPDATE ${tables.OLAH_ABSEN} SET
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
              // console.log(
              //   `✅ ${group.name} Batch ${chunkIndex + 1}/${chunks.length}: ${
              //     result.affectedRows
              //   } rows (${taskDuration}ms)`
              // );

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

// Konstanta untuk keterangan valid
const VALID_KETERANGAN = {
  ABSENCE: ['abs_', 'cuti', 'ijin', 'sakit'],
  BUSINESS_TRIP: ['sppd_'],
  ATTENDANCE_JUSTIFICATION: ['att_']
};

function cekWajibKerja(statusJamKerja, jenisHari) {
  try {
    const isDebugMode = process.env.NODE_ENV === "development";
    
    // Normalisasi input
    const status = String(statusJamKerja || '').toLowerCase();
    const hari = String(jenisHari || '').toLowerCase();
    
    if (isDebugMode) {
      console.log(`🔍 cekWajibKerja - status: ${status}, hari: ${hari}`);
    }

    // ATURAN BISNIS:
    // 1. Shift OFF = tidak wajib kerja (apapun jenis harinya)
    // 2. Mengandung "normal" = jam kerja normal:
    //    - Normal + Hari kerja = WAJIB KERJA
    //    - Normal + Hari libur = TIDAK wajib kerja
    // 3. Shift lainnya (Pagi, Siang, Malam) = wajib kerja (apapun jenis harinya)
    
    // Case 1: Shift OFF
    if (status.includes('shift') && status.includes('off')) {
      if (isDebugMode) console.log('   → Shift OFF: tidak wajib kerja');
      return false;
    }
    
    // Case 2: Jam kerja Normal (mengandung kata "normal")
    if (status.includes('normal')) {
      const isHariKerja = hari.includes('kerja');
      const result = isHariKerja; // Normal + Hari kerja = wajib kerja
      
      if (isDebugMode) {
        console.log(`   → Normal + ${isHariKerja ? 'Hari kerja' : 'Hari libur'}: ${result ? 'wajib kerja' : 'tidak wajib kerja'}`);
      }
      return result;
    }
    
    // Case 3: Shift lainnya (Pagi, Siang, Malam, dll)
    if (isDebugMode) console.log('   → Shift non-Normal: wajib kerja');
    return true;
    
  } catch (error) {
    console.error('❌ Error dalam cekWajibKerja:', error);
    // Default ke wajib kerja untuk safety
    return true;
  }
}

function cekKeteranganValid(valueAttAbs) {
  if (!valueAttAbs || String(valueAttAbs).trim() === '' || valueAttAbs === '—') {
    return false;
  }
  
  const value = String(valueAttAbs).toLowerCase();
  
  // Gabungkan semua keterangan valid
  const allValidKeywords = [
    ...VALID_KETERANGAN.ABSENCE,
    ...VALID_KETERANGAN.BUSINESS_TRIP,
    ...VALID_KETERANGAN.ATTENDANCE_JUSTIFICATION
  ];
  
  return allValidKeywords.some(keyword => value.includes(keyword));
}

function tentukanKeteranganKehadiran(row) {
  try {
    const { status_jam_kerja, jenis_hari, status_absen, value_att_abs } = row;
    
    // Debug log untuk development
    const isDebugMode = process.env.NODE_ENV === "development";
    if (isDebugMode) {
      console.log(`🔍 Debug keterangan_kehadiran - PERNER: ${row.perner}, Tanggal: ${row.tanggal}`);
      console.log(`   status_jam_kerja: ${status_jam_kerja}`);
      console.log(`   jenis_hari: ${jenis_hari}`);
      console.log(`   status_absen: ${status_absen}`);
      console.log(`   value_att_abs: ${value_att_abs}`);
    }
    
    // STEP 1: Cek apakah wajib kerja
    const isWajibKerja = cekWajibKerja(status_jam_kerja, jenis_hari);
    if (isDebugMode) {
      console.log(`   isWajibKerja: ${isWajibKerja}`);
    }
    
    // STEP 2: Jika tidak wajib kerja → "Bukan hari wajib kerja"
    if (!isWajibKerja) {
      if (isDebugMode) {
        console.log(`   🔵 Result: Bukan hari wajib kerja`);
      }
      return "Bukan hari wajib kerja";
    }
    
    // STEP 3: Wajib kerja - cek ada absen lengkap
    const adaAbsenLengkap = status_absen === "Lengkap";
    
    // STEP 4: Wajib kerja - cek ada keterangan valid
    const adaKeteranganValid = cekKeteranganValid(value_att_abs);
    
    if (isDebugMode) {
      console.log(`   adaAbsenLengkap: ${adaAbsenLengkap}`);
      console.log(`   adaKeteranganValid: ${adaKeteranganValid}`);
    }
    
    // STEP 5: Final decision untuk hari wajib kerja
    let result;
    if (adaAbsenLengkap || adaKeteranganValid) {
      result = "Dengan Absen/Dengan Keterangan";
      if (isDebugMode) {
        console.log(`   ✅ Result: ${result} (ada bukti kehadiran)`);
      }
    } else {
      result = "Tanpa Keterangan";
      if (isDebugMode) {
        console.log(`   ❌ Result: ${result} (tidak ada bukti kehadiran)`);
      }
    }
    
    return result;
    
  } catch (error) {
    console.error(`❌ Error dalam tentukanKeteranganKehadiran untuk ${row.perner}:`, error);
    // Default ke safe value jika ada error
    return "Dengan Absen/Dengan Keterangan";
  }
}

// Export functions untuk testing
module.exports = {
  tentukanKeteranganKehadiran,
  cekWajibKerja,
  cekKeteranganValid,
  VALID_KETERANGAN
};
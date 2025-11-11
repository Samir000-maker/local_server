// server.js – Smart Instagram-level compression (skips already-compressed videos)

const ffmpegInstaller = require('@ffmpeg-installer/ffmpeg');
const ffprobeInstaller = require('@ffprobe-installer/ffprobe');
const ffmpeg = require('fluent-ffmpeg');

ffmpeg.setFfmpegPath(ffmpegInstaller.path);
ffmpeg.setFfprobePath(ffprobeInstaller.path);

const express = require('express');
const multer = require('multer');
const sharp = require('sharp');
const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const http = require('http');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// ── CONFIG ────────────────────────────────────────────────────────────────────
const CLOUDFLARE_API_ENDPOINT = 'https://d90a9dc9787962f745733993f3f1766d.r2.cloudflarestorage.com';
const R2_PUBLIC_BASE_URL     = 'https://pub-b86353e4f63d45f8bf7e94b3143a1d8b.r2.dev';
const BUCKET_NAME            = 'my-app-posts';
const ACCESS_KEY_ID          = '5d5420127538adcf5c50d41735752ffd';
const SECRET_ACCESS_KEY      = '6cbc2488a34434653f26399d1644260b50731e7316c97ac4ba7364d66373ff1b';
const MAX_BODY_LIMIT         = '50gb';

// ── COMPRESSION THRESHOLDS ───────────────────────────────────────────────────
const COMPRESSION_THRESHOLDS = {
  // If video bitrate is below these values, it's already well-compressed
  MAX_BITRATE_720P: 1500,    // kbps - Instagram uses ~1000-1500 for 720p
  MAX_BITRATE_1080P: 2500,   // kbps - Instagram uses ~2000-2500 for 1080p
  MAX_RESOLUTION: 1080,      // Skip compression if already 1080p or lower AND low bitrate
  MIN_FILE_SIZE_MB: 5       // Only compress if file is larger than 15MB
};

const app = express();

// ── PREPARE DIRECTORIES ───────────────────────────────────────────────────────
const tmpDir    = path.join(__dirname, 'tmp');
const uploadDir = path.join(__dirname, 'uploads');
const activeUploads = new Map();

[tmpDir, uploadDir].forEach(dir => {
  if (!fsSync.existsSync(dir)) fsSync.mkdirSync(dir, { recursive: true });
});

// ── MULTER (UNLIMITED UPLOAD SIZE) ─────────────────────────────────────────────
const upload = multer({
  dest: tmpDir,
  limits: { fileSize: Infinity, fieldSize: Infinity },
  fileFilter: (req, file, cb) => {
    if (/^image\/|^video\//.test(file.mimetype)) cb(null, true);
    else cb(new Error('Only image and video files are allowed'));
  }
});

// ── EXPRESS BODY PARSING (VERY LARGE) ────────────────────────────────────────
app.use(express.json({  limit: MAX_BODY_LIMIT }));
app.use(express.urlencoded({ limit: MAX_BODY_LIMIT, extended: true }));

// ── R2 CLIENT ────────────────────────────────────────────────────────────────
const s3 = new S3Client({
  endpoint: CLOUDFLARE_API_ENDPOINT,
  region:   'auto',
  credentials: {
    accessKeyId:     ACCESS_KEY_ID,
    secretAccessKey: SECRET_ACCESS_KEY
  }
});

// ── UTILITIES ────────────────────────────────────────────────────────────────
const timestamp = () => new Date().toISOString();
function sanitizeFilename(name) {
  return name.toLowerCase()
    .replace(/[^a-z0-9]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}

async function uploadToR2(key, buffer, contentType) {
  console.log(`[${timestamp()}] ⬆️ Uploading ${key} (${(buffer.length/1024/1024).toFixed(2)}MB)`);
  await s3.send(new PutObjectCommand({
    Bucket:       BUCKET_NAME,
    Key:          key,
    Body:         buffer,
    ContentType:  contentType,
    CacheControl: 'public, max-age=31536000',
    Metadata:     { 'upload-timestamp': Date.now().toString() }
  }));
  const url = `${R2_PUBLIC_BASE_URL}/${key}`;
  console.log(`[${timestamp()}] ✅ Uploaded: ${url}`);
  return url;
}

// ── IMAGE PROCESSING (Instagram-style: natural quality) ──────────────────────
async function processImage(inputPath, maxWidth = 1080, quality = 85) {
  console.log(`[${timestamp()}] 🖼️ Processing image (Instagram-style)`);
  
  const metadata = await sharp(inputPath).metadata();
  console.log(`• Original: ${metadata.width}x${metadata.height}, format: ${metadata.format}`);
  
  let img = sharp(inputPath);
  
  // Resize to max 1080px width (Instagram standard)
  if (metadata.width > maxWidth) {
    img = img.resize({ 
      width: maxWidth, 
      fit: 'inside', 
      withoutEnlargement: true 
    });
  }
  
  // Instagram uses progressive JPEG with moderate quality (85-90)
  const out = await img
    .jpeg({ 
      quality: quality,
      progressive: true,
      mozjpeg: true,
      chromaSubsampling: '4:2:0'
    })
    .toBuffer();
    
  console.log(`[${timestamp()}] ✅ Image processed: ${(out.length/1024).toFixed(0)}KB`);
  return out;
}

// ── VIDEO ANALYSIS: Check if already compressed ──────────────────────────────
async function analyzeVideoCompression(inputPath) {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(inputPath, (err, meta) => {
      if (err) return reject(err);
      
      const vs = meta.streams.find(s => s.codec_type === 'video');
      if (!vs) return reject(new Error('No video stream found'));
      
      const width = vs.width;
      const height = vs.height;
      const codec = vs.codec_name;
      const duration = meta.format.duration || 0;
      const fileSize = meta.format.size || 0;
      const fileSizeMB = fileSize / (1024 * 1024);
      
      // Calculate bitrate (kbps)
      let bitrate = 0;
      if (vs.bit_rate) {
        bitrate = Math.round(vs.bit_rate / 1000);
      } else if (meta.format.bit_rate) {
        bitrate = Math.round(meta.format.bit_rate / 1000);
      } else if (duration > 0 && fileSize > 0) {
        bitrate = Math.round((fileSize * 8) / (duration * 1000));
      }
      
      console.log(`\n[${timestamp()}] 📊 VIDEO ANALYSIS:`);
      console.log(`   Resolution: ${width}x${height}`);
      console.log(`   Codec: ${codec}`);
      console.log(`   Duration: ${duration.toFixed(1)}s`);
      console.log(`   File Size: ${fileSizeMB.toFixed(2)}MB`);
      console.log(`   Bitrate: ${bitrate}kbps`);
      
      // Determine if compression is needed
      const maxResolution = Math.max(width, height);
      const isH264 = codec.toLowerCase().includes('h264') || codec.toLowerCase().includes('avc');
      
      // Check if already well-compressed
      let needsCompression = true;
      let reason = '';
      
      if (fileSizeMB < COMPRESSION_THRESHOLDS.MIN_FILE_SIZE_MB) {
        needsCompression = false;
        reason = `File size (${fileSizeMB.toFixed(2)}MB) is already small`;
      } else if (maxResolution <= 720 && bitrate > 0 && bitrate <= COMPRESSION_THRESHOLDS.MAX_BITRATE_720P) {
        needsCompression = false;
        reason = `720p video with bitrate ${bitrate}kbps is already optimized`;
      } else if (maxResolution <= 1080 && bitrate > 0 && bitrate <= COMPRESSION_THRESHOLDS.MAX_BITRATE_1080P) {
        needsCompression = false;
        reason = `1080p video with bitrate ${bitrate}kbps is already optimized`;
      } else if (isH264 && maxResolution <= 1080 && bitrate > 0 && bitrate <= 3000) {
        needsCompression = false;
        reason = `H.264 video is already well-compressed (bitrate: ${bitrate}kbps)`;
      }
      
      if (!needsCompression) {
        console.log(`   ✅ SKIP COMPRESSION: ${reason}`);
      } else {
        console.log(`   🔄 NEEDS COMPRESSION: Bitrate ${bitrate}kbps exceeds thresholds or file is large`);
      }
      
      resolve({
        needsCompression,
        reason,
        metadata: {
          width,
          height,
          codec,
          duration,
          fileSize,
          fileSizeMB,
          bitrate
        }
      });
    });
  });
}

// ── VIDEO PROCESSING (Instagram-level: clean compression) ────────────────────
async function processVideo(inputPath, outputPath) {
  console.log(`[${timestamp()}] 🎬 Starting Instagram-level compression`);
  
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(inputPath, (err, meta) => {
      if (err) return reject(err);
      
      const vs = meta.streams.find(s => s.codec_type === 'video');
      const as = meta.streams.find(s => s.codec_type === 'audio');
      
      console.log(`• Input: ${vs.width}x${vs.height}, ${meta.format.duration?.toFixed(1) || 0}s`);
      console.log(`• Codec: ${vs.codec_name}, Bitrate: ${(meta.format.bit_rate / 1000).toFixed(0)}kbps`);
      
      // Determine target resolution (Instagram uses max 1080p)
      let targetHeight = 1080;
      let targetWidth = 1920;
      
      // Keep aspect ratio, scale down if larger than 1080p
      if (vs.height > 1080 || vs.width > 1920) {
        const aspectRatio = vs.width / vs.height;
        if (aspectRatio > 16/9) {
          targetWidth = 1920;
          targetHeight = Math.round(1920 / aspectRatio);
        } else {
          targetHeight = 1080;
          targetWidth = Math.round(1080 * aspectRatio);
        }
      } else {
        targetWidth = vs.width;
        targetHeight = vs.height;
      }
      
      console.log(`• Target: ${targetWidth}x${targetHeight}`);
      
      // Build video filters (only scaling, no color modifications)
      const videoFilters = [
        `scale=${targetWidth}:${targetHeight}:force_original_aspect_ratio=decrease`,
        'scale=trunc(iw/2)*2:trunc(ih/2)*2'
      ];
      
      const ffmpegCommand = ffmpeg(inputPath)
        .videoCodec('libx264')
        .videoFilters(videoFilters)
        .outputOptions([
          '-preset medium',
          '-crf 23',
          '-profile:v main',
          '-level 4.0',
          '-pix_fmt yuv420p',
          '-movflags +faststart',
          '-g 60',
          '-bf 2',
          '-maxrate 2500k',
          '-bufsize 5000k'
        ])
        .format('mp4');
      
      // Audio settings
      if (as) {
        ffmpegCommand
          .audioCodec('aac')
          .audioBitrate('128k')
          .audioChannels(2)
          .audioFrequency(44100);
      } else {
        ffmpegCommand.noAudio();
      }
      
      ffmpegCommand
        .on('start', cmd => {
          console.log(`[${timestamp()}] 🚀 FFmpeg started`);
        })
        .on('progress', p => {
          if (p.percent && p.percent > 0) {
            console.log(`[${timestamp()}] 📊 Progress: ${Math.round(p.percent)}%`);
          }
        })
        .on('end', () => {
          console.log(`[${timestamp()}] ✅ Video compressed (Instagram-level quality)`);
          resolve();
        })
        .on('error', err => {
          console.error(`[${timestamp()}] ❌ FFmpeg error:`, err.message);
          reject(err);
        })
        .save(outputPath);
    });
  });
}

// ── CLEANUP UTILITY ──────────────────────────────────────────────────────────
async function cleanupFiles(files) {
  for (const filePath of files) {
    if (!filePath) continue;
    try {
      if (fsSync.existsSync(filePath)) {
        await fs.unlink(filePath);
        console.log(`[${timestamp()}] 🧹 Cleaned: ${path.basename(filePath)}`);
      }
    } catch (err) {
      console.warn(`[${timestamp()}] ⚠️ Cleanup failed for ${path.basename(filePath)}:`, err.message);
    }
  }
}

// ── UPLOAD ENDPOINT ──────────────────────────────────────────────────────────
app.post('/upload', upload.single('media'), async (req, res) => {
  const uploadId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  const start = Date.now();
  
  console.log(`\n[${timestamp()}] 📥 Upload ${uploadId} started`);
  
  if (!req.file) {
    console.error(`[${uploadId}] ❌ No file in request`);
    return res.status(400).json({ success: false, message: 'No file uploaded' });
  }
  
  const { path: tmpPath, mimetype, originalname, size } = req.file;
  console.log(`[${uploadId}] File: ${originalname} (${(size/1024/1024).toFixed(2)}MB)`);
  console.log(`[${uploadId}] Type: ${mimetype}`);
  
  // Track active upload
  activeUploads.set(uploadId, { start, file: originalname, size });
  
  // Set generous timeout
  //res.setTimeout(600000); // 10 minutes
  
  // Keep connection alive
  const keepAliveInterval = setInterval(() => {
    if (!res.headersSent) {
      res.write(' ');
    }
  }, 30000);
  
  let inputWithExt, processedPath;
  const filesToCleanup = [tmpPath];
  
  try {
    const ext = path.extname(originalname).toLowerCase();
    inputWithExt = `${tmpPath}${ext}`;
    filesToCleanup.push(inputWithExt);
    
    console.log(`[${uploadId}] 🔄 Preparing file...`);
    await fs.rename(tmpPath, inputWithExt);
    
    const base = sanitizeFilename(path.basename(originalname, ext));
    const keyBase = `${base}_${Date.now()}`;
    let buffer, contentType, finalKey;
    let compressionSkipped = false;
    let compressionReason = '';
    
    if (mimetype.startsWith('image/')) {
      console.log(`[${uploadId}] 🖼️ Processing image...`);
      buffer = await processImage(inputWithExt);
      contentType = 'image/jpeg';
      finalKey = `${keyBase}.jpg`;
      
    } else if (mimetype.startsWith('video/')) {
      console.log(`[${uploadId}] 🎬 Analyzing video...`);
      
      // Analyze if video needs compression
      const analysis = await analyzeVideoCompression(inputWithExt);
      
      if (!analysis.needsCompression) {
        // SKIP COMPRESSION - Upload original file
        console.log(`[${uploadId}] ⚡ Skipping compression: ${analysis.reason}`);
        buffer = await fs.readFile(inputWithExt);
        compressionSkipped = true;
        compressionReason = analysis.reason;
      } else {
        // COMPRESS VIDEO
        console.log(`[${uploadId}] 🎬 Compressing video...`);
        processedPath = path.join(tmpDir, `${keyBase}_compressed.mp4`);
        filesToCleanup.push(processedPath);
        
        // Process with timeout protection
        const ffmpegTimeout = setTimeout(() => {
          console.error(`[${uploadId}] ⏱️ Video processing timeout (5min)`);
          throw new Error('Video processing timeout - file may be too large or corrupted');
        }, 300000);
        
        try {
          await processVideo(inputWithExt, processedPath);
          clearTimeout(ffmpegTimeout);
        } catch (err) {
          clearTimeout(ffmpegTimeout);
          throw err;
        }
        
        // Read processed video
        buffer = await fs.readFile(processedPath);
      }
      
      contentType = 'video/mp4';
      finalKey = `${keyBase}.mp4`;
      
      const reductionPercent = ((1 - buffer.length/size) * 100).toFixed(1);
      console.log(`[${uploadId}] 📊 Original: ${(size/1024/1024).toFixed(2)}MB → Final: ${(buffer.length/1024/1024).toFixed(2)}MB (${reductionPercent}% reduction)`);
      
    } else {
      throw new Error(`Unsupported file type: ${mimetype}`);
    }
    
    // Upload to R2
    console.log(`[${uploadId}] ⬆️ Uploading to R2...`);
    const url = await uploadToR2(finalKey, buffer, contentType);
    
    const took = ((Date.now() - start) / 1000).toFixed(2);
    console.log(`[${uploadId}] ✅ Upload complete in ${took}s`);
    
    // Cleanup
    clearInterval(keepAliveInterval);
    activeUploads.delete(uploadId);
    await cleanupFiles(filesToCleanup);
    
    // Prepare response
    const response = {
      success: true,
      url,
      uploadId,
      processing_time_seconds: parseFloat(took),
      original_size_bytes: size,
      final_size_bytes: buffer.length,
      compression_ratio: ((1 - buffer.length/size) * 100).toFixed(1) + '%',
      file_type: mimetype.startsWith('image/') ? 'image' : 'video'
    };
    
    if (compressionSkipped) {
      response.compression_skipped = true;
      response.skip_reason = compressionReason;
    }
    
    return res.json(response);
    
  } catch (err) {
    console.error(`[${uploadId}] 🔥 Error:`, err.message);
    console.error(err.stack);
    
    clearInterval(keepAliveInterval);
    activeUploads.delete(uploadId);
    await cleanupFiles(filesToCleanup);
    
    return res.status(500).json({ 
      success: false, 
      message: err.message,
      uploadId
    });
  }
});

// ── MONITORING ENDPOINT ──────────────────────────────────────────────────────
app.get('/upload-status', (req, res) => {
  const active = Array.from(activeUploads.entries()).map(([id, data]) => ({
    id,
    file: data.file,
    size_mb: (data.size / 1024 / 1024).toFixed(2),
    duration_seconds: ((Date.now() - data.start) / 1000).toFixed(1)
  }));
  
  res.json({
    active_uploads: active.length,
    uploads: active,
    timestamp: timestamp()
  });
});

app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

// Alternative simple health check (if you prefer minimal response)
app.get('/ping', (req, res) => {
  res.status(200).send('pong');
});

// ── ERROR HANDLER ────────────────────────────────────────────────────────────
app.use((err, req, res, next) => {
  console.error(`[${timestamp()}] 💥 Unhandled error:`, err.message);
  console.error(err.stack);
  
  if (!res.headersSent) {
    res.status(500).json({ 
      success: false, 
      message: err.message || 'Internal server error'
    });
  }
});

// ── START SERVER ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const server = http.createServer(app);

// Remove all timeouts for handling large uploads
server.timeout = 0;
server.keepAliveTimeout = 0;
server.headersTimeout = 0;
server.requestTimeout = 0;

server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`🚀 Server started on http://0.0.0.0:${PORT}`);
  console.log(`📱 Smart Instagram-level compression enabled`);
  console.log(`   ✓ Images: 1080px max, 85% quality, progressive JPEG`);
  console.log(`   ✓ Videos: Intelligent compression detection`);
  console.log(`   ✓ Skip compression if already optimized (saves time & quality)`);
  console.log(`\n📊 Compression Skip Rules:`);
  console.log(`   • Files under ${COMPRESSION_THRESHOLDS.MIN_FILE_SIZE_MB}MB → Upload as-is`);
  console.log(`   • 720p videos with bitrate ≤ ${COMPRESSION_THRESHOLDS.MAX_BITRATE_720P}kbps → Skip`);
  console.log(`   • 1080p videos with bitrate ≤ ${COMPRESSION_THRESHOLDS.MAX_BITRATE_1080P}kbps → Skip`);
  console.log(`   • Already compressed H.264 videos → Skip`);
  console.log(`${'='.repeat(70)}\n`);
});

const express = require('express');
const mongoose = require('mongoose');
const path = require('path');
const multer = require('multer');
const { Readable } = require('node:stream');
const { pipeline } = require('node:stream/promises');

// Anwendung konfigurieren
const app = express();
const PORT = 3000;

let coverBucket;

// Datei-Upload: maximal eine Datei mit 5 MB
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 5 * 1024 * 1024,
    files: 1
  }
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Schema und Modell
const buchSchema = new mongoose.Schema({
  titel: { type: String, required: true },
  autor: String,
  isbn: { type: String, trim: true },
  erscheinungsjahr: Number,
  preis: { type: Number, min: 0 },
  coverId: mongoose.Schema.Types.ObjectId
});

const Buch = mongoose.model('Buch', buchSchema, 'buecher');

// Nur erlaubte Buchfelder übernehmen
function buchDaten(body) {
  const daten = {};

  for (const feld of [
    'titel',
    'autor',
    'isbn',
    'erscheinungsjahr',
    'preis'
  ]) {
    if (Object.prototype.hasOwnProperty.call(body, feld)) {
      daten[feld] = body[feld];
    }
  }

  return daten;
}

// Einfache Formatprüfung für PNG und JPEG
function bildTyp(buffer) {
  const pngSignatur = Buffer.from([
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a
  ]);

  if (
    buffer.length >= 8 &&
    buffer.subarray(0, 8).equals(pngSignatur)
  ) {
    return 'image/png';
  }

  if (
    buffer.length >= 3 &&
    buffer[0] === 0xff &&
    buffer[1] === 0xd8 &&
    buffer[2] === 0xff
  ) {
    return 'image/jpeg';
  }

  return null;
}

// ID für alle Routen mit :id prüfen
app.param('id', (req, res, next, id) => {
  if (!mongoose.isObjectIdOrHexString(id)) {
    return res.status(400).json({
      fehler: 'Ungültige Buch-ID'
    });
  }

  next();
});

// Startseite
app.get('/', (req, res) => {
  res.send('Hallo, Welt! Die Bücher findest du unter /buecher');
});

// READ: Alle Bücher lesen
app.get('/buecher', async (req, res, next) => {
  try {
    const buecher = await Buch.find({});
    res.json(buecher);
  } catch (error) {
    next(error);
  }
});

// CREATE: Neues Buch erstellen
app.post('/buecher', async (req, res, next) => {
  try {
    const buch = await Buch.create(buchDaten(req.body || {}));
    res.status(201).json(buch);
  } catch (error) {
    next(error);
  }
});

// UPDATE: Einzelne Buchfelder ändern
app.patch('/buecher/:id', async (req, res, next) => {
  try {
    const daten = buchDaten(req.body || {});

    if (Object.keys(daten).length === 0) {
      return res.status(400).json({
        fehler: 'Keine Buchfelder zum Ändern angegeben'
      });
    }

    const buch = await Buch.findByIdAndUpdate(
      req.params.id,
      { $set: daten },
      {
        returnDocument: 'after',
        runValidators: true
      }
    );

    if (!buch) {
      return res.status(404).json({
        fehler: 'Buch nicht gefunden'
      });
    }

    res.json(buch);
  } catch (error) {
    next(error);
  }
});

// DELETE: Buch und zugehöriges Cover löschen
app.delete('/buecher/:id', async (req, res, next) => {
  try {
    const buch = await Buch.findByIdAndDelete(req.params.id);

    if (!buch) {
      return res.status(404).json({
        fehler: 'Buch nicht gefunden'
      });
    }

    if (buch.coverId) {
      await coverBucket.delete(buch.coverId).catch(error => {
        console.error('Cover nicht entfernt:', error.message);
      });
    }

    res.json({
      nachricht: 'Buch gelöscht'
    });
  } catch (error) {
    next(error);
  }
});

// Cover hochladen oder ersetzen
app.post(
  '/buecher/:id/cover',
  upload.single('cover'),
  async (req, res, next) => {
    let neueCoverId;
    let zugeordnet = false;

    try {
      const buch = await Buch.findById(req.params.id);

      if (!buch) {
        return res.status(404).json({
          fehler: 'Buch nicht gefunden'
        });
      }

      if (!req.file) {
        return res.status(400).json({
          fehler: 'Bitte eine Datei auswählen'
        });
      }

      const mimeType = bildTyp(req.file.buffer);

      if (!mimeType) {
        return res.status(400).json({
          fehler: 'Nur JPEG- und PNG-Bilder sind erlaubt'
        });
      }

      const endung = mimeType === 'image/png' ? 'png' : 'jpg';

      const stream = coverBucket.openUploadStream(
        `cover-${buch._id}.${endung}`,
        {
          metadata: {
            mimeType,
            buchId: buch._id
          }
        }
      );

      neueCoverId = stream.id;

      await pipeline(
        Readable.from([req.file.buffer]),
        stream
      );

      const alteCoverId = buch.coverId;

      buch.coverId = neueCoverId;
      await buch.save();
      zugeordnet = true;

      // Bisheriges Cover nach erfolgreichem Ersetzen entfernen
      if (alteCoverId) {
        await coverBucket.delete(alteCoverId).catch(error => {
          console.error(
            'Altes Cover nicht entfernt:',
            error.message
          );
        });
      }

      res.status(201).json({
        nachricht: 'Cover gespeichert',
        coverId: neueCoverId
      });
    } catch (error) {
      // Nicht zugeordnete Upload-Datei nach Möglichkeit entfernen
      if (neueCoverId && !zugeordnet) {
        await coverBucket.delete(neueCoverId).catch(() => {});
      }

      next(error);
    }
  }
);

// Cover anzeigen
app.get('/buecher/:id/cover', async (req, res, next) => {
  try {
    const buch = await Buch.findById(req.params.id);

    if (!buch || !buch.coverId) {
      return res.status(404).json({
        fehler: 'Kein Cover vorhanden'
      });
    }

    const datei = await coverBucket
      .find({ _id: buch.coverId })
      .next();

    if (!datei) {
      return res.status(404).json({
        fehler: 'Coverdatei nicht gefunden'
      });
    }

    res.setHeader(
      'Content-Type',
      datei.metadata?.mimeType || 'application/octet-stream'
    );
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Cache-Control', 'no-store');

    const stream = coverBucket.openDownloadStream(buch.coverId);

    stream.on('error', error => {
      if (!res.headersSent) {
        next(error);
      } else {
        res.destroy(error);
      }
    });

    stream.pipe(res);
  } catch (error) {
    next(error);
  }
});

// Zentrale Fehlerbehandlung: nach allen Routen
app.use((error, req, res, next) => {
  if (res.headersSent) {
    return next(error);
  }

  // Fehler beim Datei-Upload
  if (error instanceof multer.MulterError) {
    return res.status(400).json({
      fehler: error.code === 'LIMIT_FILE_SIZE'
        ? 'Das Bild darf maximal 5 MB gross sein'
        : 'Datei-Upload fehlgeschlagen'
    });
  }

  // Validierungsfehler und ungültiges JSON
  if (
    error.name === 'ValidationError' ||
    error.name === 'CastError' ||
    error.type === 'entity.parse.failed'
  ) {
    return res.status(400).json({
      fehler: error.message
    });
  }

  console.error(error);

  res.status(500).json({
    fehler: 'Interner Serverfehler'
  });
});

// Datenbank verbinden und Webserver starten
async function start() {
  try {
    await mongoose.connect('mongodb://127.0.0.1:27017/test', {
      user: 'localAdmin',
      pass: 'secret',
      authSource: 'admin',
      replicaSet: 'rs0'
    });

    coverBucket = new mongoose.mongo.GridFSBucket(
      mongoose.connection.db,
      { bucketName: 'covers' }
    );

    console.log('Mit MongoDB verbunden');

    app.listen(PORT, '127.0.0.1', () => {
      console.log(`Server läuft auf http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error(
      'Verbindung fehlgeschlagen:',
      error.message
    );

    process.exitCode = 1;
  }
}

start();
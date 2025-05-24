const fs = require('fs');
const csvParser = require('csv-parser');
const fastCsv = require('fast-csv');
const path = require('path');

// === EDIT THESE FILE NAMES ===
const inputFilePath = path.join(__dirname, 'EDIT_THIS_NAME.csv'); // Your original CSV file
const outputFilePath = path.join(__dirname, 'processed_done_total.csv'); // Where to save the cleaned 

// Helper: Convert time string to minutes (max 60)
function convertToMinutes(timeString) {
    if (!timeString) return 0;
    const parts = timeString.split(':').map(Number);
    let minutes = 0;

    if (parts.length === 3) {
        const [h, m, s] = parts;
        minutes = h * 60 + m + Math.floor(s / 60);
    } else if (parts.length === 2) {
        const [m, s] = parts;
        minutes = m + Math.floor(s / 60);
    }

    return Math.min(minutes, 60);
}

// Helper: Clean and validate email
function cleanEmail(email) {
    if (typeof email !== 'string') return null;
    const cleaned = email.replace(',', '.').trim().toLowerCase();
    const regex = /^[^@]+@[^@]+\.[^@]+$/;
    return regex.test(cleaned) ? cleaned : null;
}

// Helper: Parse Czech date string to JS Date
function parseCzechDate(dateStr) {
    const [d, m, yhms] = dateStr.split('.');
    if (!d || !m || !yhms) return null;

    const [y, time] = yhms.trim().split(' ');
    if (!y || !time) return null;

    const [h, min] = time.split(':').map(Number);
    return new Date(`${y}-${m}-${d}T${String(h).padStart(2, '0')}:${String(min).padStart(2, '0')}:00`);
}

const covidStart = new Date('2020-03-13');
const covidEnd = new Date('2021-05-30');

const results = [];

fs.createReadStream(inputFilePath)
    .pipe(csvParser({ separator: ',' }))
    .on('data', (row) => {
        const cleanedEmail = cleanEmail(row.teacherEmail);
        if (!cleanedEmail || row.zam !== '0') return;

        const dateCreated = parseCzechDate(row.dateCreated);
        const covid = dateCreated && dateCreated >= covidStart && dateCreated <= covidEnd ? 1 : 0;

        row.timeSpentMinutes = convertToMinutes(row.timeSpent);
        row.cleanedEmail = cleanedEmail;
        row.COVID = covid;

        if (row.exerciseDescription === 'cvičení s celou třídou') {
            row.exerciseDescription = 'Modes of the work with the activity';
        }

        if (row.from === 'free') {
            row.from = 'Fred';
        }

        row.b4 = (row.b4 || '') + ' [https://dejepisplus.npi.cz/historicka-gramotnost/]';

        results.push(row);
    })
    .on('end', () => {
        const ws = fs.createWriteStream(outputFilePath);
        fastCsv
            .write(results, { headers: true, delimiter: '\t' })
            .pipe(ws)
            .on('finish', () => {
                console.log(`✅ Cleaned dataset saved to: ${outputFilePath}`);
            });
    });

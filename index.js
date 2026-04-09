require("dotenv").config(); // added

const express = require("express");
const axios = require("axios");
const sql = require("mssql");

const app = express();

// -------- DB CONFIG --------
const dbConfig = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_NAME,
  options: {
    encrypt: false,
    trustServerCertificate: true
  }
};

// -------- GLOBALS --------
const countries = ["india", "united-states", "united-kingdom", "australia", "germany", "france", "singapore", , "netherlands", "albania", "algeria", "andorra", "angola", "antigua-and-barbuda", "argentina", "armenia", "aruba", "austria", "azerbaijan", "the-bahamas", "bahrain", "belgium", "bolivia", "bosnia-and-herzegovina", "botswana", "brazil", "brunei", "bulgaria", "cambodia", "cameroon", "canada", "central-african-republic", "chile", "china", "colombia", "congo", "democratic-republic-of-the-congo", "costa-rica", "croatia", "curacao", "cyprus", "czech-republic", "denmark", "dominican-republic", "ecuador", "egypt", "el-salvador", "estonia", "fiji", "finland", "gambia", "ghana", "greece", "greenland", "grenada", "guatemala", "guernsey", "guinea", "guyana", "haiti", "italy--roma", "honduras", "hong-kong-sar", "hungary", "iceland", "indonesia", "iraq", "ireland", "isle-of-man", "israel", "italy", "jamaica", "japan", "jersey", "jordan", "kazakhstan", "kenya", "south-korea", "kuwait", "latvia", "lebanon", "liberia", "libya", "liechtenstein", "lithuania", "luxembourg", "mauritius", "mexico", "moldova", "monaco", "mongolia", "montenegro", "morocco", "namibia", "nepal", "new-zealand", "nicaragua", "nigeria", "niue", "norway", "oman", "pakistan", "panama", "papua-new-guinea", "paraguay", "peru", "philippines", "poland", "portugal", "qatar", "romania", "russia", "rwanda", "saint-kitts-and-nevis", "saint-lucia", "saint-vincent-and-the-grenadines", "san-marino", "saudi-arabia", "senegal", "serbia", "sint-maarten", "slovakia", "slovenia", "south-africa", "spain", "sri-lanka", "suriname", "sweden", "switzerland", "taiwan", "tajikistan", "tanzania", "thailand", "togo", "trinidad-and-tobago", "tunisia", "turkey", "turkmenistan", "uganda", "ukraine", "united-arab-emirates", "uruguay", "uzbekistan", "venezuela", "vietnam", "zambia", "zimbabwe"];
const allIDs = new Map();

// -------- FETCH HTML --------
async function getHTML(url) {
  try {
    const res = await axios.get(url, {
      headers: { "User-Agent": "Mozilla/5.0" },
      timeout: 15000
    });
    return res.data;
  } catch (err) {
    return "";
  }
}

// -------- EXTRACT IDS --------
function extractIDs(html) {
  const regex = /data-event-id="(\d+)"/g;
  const ids = new Set();
  let match;

  while ((match = regex.exec(html)) !== null) {
    ids.add(match[1]);
  }

  return Array.from(ids);
}

// -------- COMPARE --------
function equalArrays(a, b) {
  if (a.length !== b.length) return false;
  return a.every((v, i) => v === b[i]);
}

// -------- FETCH EVENT --------
async function fetchEventDetails(eventID) {
  try {
    const url = `https://www.eventbriteapi.com/v3/events/${eventID}/?expand=organizer,category,subcategory,venue&token=${process.env.EVENTBRITE_TOKEN}`;
    const res = await axios.get(url);
    return res.data;
  } catch (err) {
    console.log("API Error:", err.message);
    return null;
  }
}

// -------- FETCH TICKET --------
async function fetchTicketPrice(eventID) {
  try {
    const url = `https://www.eventbriteapi.com/v3/events/${eventID}/ticket_classes/?token=${process.env.EVENTBRITE_TOKEN}`;
    const res = await axios.get(url);

    let lowest = -1;

    for (let t of res.data.ticket_classes) {
      if (t.free) return "0.00";

      let cost = parseFloat((t.cost?.display || "0").replace("$", "")) || 0;
      let fee = parseFloat((t.fee?.display || "0").replace("$", "")) || 0;
      let tax = parseFloat((t.tax?.display || "0").replace("$", "")) || 0;

      let total = cost + fee + tax;

      if (total === 0) return "0.00";
      if (lowest < 0 || total < lowest) lowest = total;
    }

    return lowest < 0 ? "0.00" : lowest.toFixed(2);
  } catch {
    return "0.00";
  }
}

// -------- SAVE EVENT --------
// (UNCHANGED)
async function saveEvent(pool, event) {
  if (!event || !event.id) return;

  let address = "", city = "", state = "", zipcode = "";
  let organizer = "", category = "", subcategory = "";

  if (event.venue) {
    address = event.venue.name || "";
    city = event.venue.address?.city || "";
    state = event.venue.address?.region || "";
    zipcode = event.venue.address?.postal_code || "";
  }

  let location = `${address}, ${city}, ${state}`;

  if (event.organizer) organizer = event.organizer.name;
  if (event.category) category = event.category.name;
  if (event.subcategory) subcategory = event.subcategory.name;

  let statusBit = event.status === "live" ? 1 : 0;
  let racc = event.is_series ? 1 : 0;

  const fee = await fetchTicketPrice(event.id);

  try {
    await pool.request()
      .input("id", sql.BigInt, event.id)
      .input("title", sql.VarChar, event.name?.text || "")
      .input("desc", sql.VarChar, event.description?.text || "")
      .input("start", sql.DateTime, event.start?.local || null)
      .input("end", sql.DateTime, event.end?.local || null)
      .input("address", sql.VarChar, address)
      .input("city", sql.VarChar, city)
      .input("state", sql.VarChar, state)
      .input("zip", sql.VarChar, zipcode)
      .input("org", sql.VarChar, organizer)
      .input("loc", sql.VarChar, location)
      .input("status", sql.Bit, statusBit)
      .input("racc", sql.TinyInt, racc)
      .input("url", sql.VarChar, event.url)
      .input("fee", sql.Decimal(10, 2), fee)
      .input("cat", sql.VarChar, category)
      .input("sub", sql.VarChar, subcategory)
      .input("capacity", sql.Int, event.capacity || 0)
      .query(`/* SAME QUERY */`);

    console.log("Saved:", event.id);
  } catch (err) {
    console.log("DB Error:", err.message);
  }
}

// -------- MAIN --------
async function start() {
  try {
    const pool = await sql.connect(dbConfig);
    console.log("Connected to DB ✅");

    for (let country of countries) {
      await scrapeCountry(pool, country);
    }

    console.log("Total Events:", allIDs.size);
  } catch (err) {
    console.error(err);
  }
}

start();
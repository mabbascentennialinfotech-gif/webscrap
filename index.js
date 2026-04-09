const express = require("express");
const axios = require("axios");
const sql = require("mssql");

const app = express();

// -------- DB CONFIG (ENV) --------
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
const countries = ["india", "united-states", "united-kingdom"];
const allIDs = new Map();

const delay = (ms) => new Promise(r => setTimeout(r, ms));

// -------- FETCH HTML --------
async function getHTML(url) {
  try {
    const res = await axios.get(url, {
      headers: { "User-Agent": "Mozilla/5.0" },
      timeout: 15000
    });
    return res.data;
  } catch {
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

// -------- FETCH EVENT --------
async function fetchEventDetails(eventID) {
  try {
    const res = await axios.get(
      `https://www.eventbriteapi.com/v3/events/${eventID}/?expand=organizer,category,subcategory,venue`,
      {
        headers: {
          Authorization: `Bearer ${process.env.EVENTBRITE_TOKEN}`
        }
      }
    );
    return res.data;
  } catch (err) {
    console.log("Event API Error:", err.response?.status);
    return null;
  }
}

// -------- FETCH TICKET --------
async function fetchTicketPrice(eventID) {
  try {
    const res = await axios.get(
      `https://www.eventbriteapi.com/v3/events/${eventID}/ticket_classes/`,
      {
        headers: {
          Authorization: `Bearer ${process.env.EVENTBRITE_TOKEN}`
        }
      }
    );

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
  } catch (err) {
    console.log("Ticket API Error:", err.response?.status);
    return "0.00";
  }
}

// -------- SAVE EVENT --------
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
      .query(`MERGE event AS target
USING (SELECT @id AS eventID) AS source
ON (target.eventID = source.eventID)
WHEN MATCHED THEN UPDATE SET
    event_title=@title,
    event_desc=@desc,
    edate=@start,
    EventEndDate=@end,
    address=@address,
    city=@city,
    state=@state,
    zipcode=@zip,
    contact_name=@org,
    location=@loc,
    status=@status,
    raccurance=@racc,
    url=@url,
    fee=@fee,
    event_type=@cat,
    event_subType=@sub,
    CompanyName=@org,
    numberOfseats=@capacity
WHEN NOT MATCHED THEN
INSERT (eventID,event_title,event_desc,edate,EventEndDate,address,city,state,zipcode,contact_name,location,status,raccurance,url,fee,event_type,event_subType,CompanyName,numberOfseats)
VALUES (@id,@title,@desc,@start,@end,@address,@city,@state,@zip,@org,@loc,@status,@racc,@url,@fee,@cat,@sub,@org,@capacity);`);

    console.log("Saved:", event.id);
  } catch (err) {
    console.log("DB Error:", err.message);
  }
}

// -------- SCRAPER --------
async function scrape(pool) {
  for (let country of countries) {
    let html = await getHTML(`https://www.eventbrite.com/d/${country}/all-events/`);
    let ids = extractIDs(html);

    for (let id of ids) {
      if (!allIDs.has(id)) {
        allIDs.set(id, true);

        let event = await fetchEventDetails(id);
        await saveEvent(pool, event);

        await delay(1000); // slow down
      }
    }
  }
}

// -------- MAIN --------
async function start() {
  try {
    const pool = await sql.connect(dbConfig);
    console.log("Connected to DB ✅");

    await scrape(pool);

    console.log("Done. Total:", allIDs.size);
  } catch (err) {
    console.error(err);
  }
}

// -------- SERVER (Render needs this) --------
const PORT = process.env.PORT || 3000;

app.get("/", (req, res) => {
  res.send("Scraper running");
});

app.listen(PORT, () => {
  console.log("Server running on port " + PORT);
  start(); // run scraper
});
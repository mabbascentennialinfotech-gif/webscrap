const express = require("express");
const axios = require("axios");
const sql = require("mssql");

const app = express();

// -------- DB CONFIG --------
const dbConfig = {
  user: "db_ac6fa0_eventbrite_admin",
  password: "Pankaj@12345",
  server: "sql5075.site4now.net",
  database: "db_ac6fa0_eventbrite",
  options: {
    encrypt: false,
    trustServerCertificate: true
  }
};

app.use((req, res, next) => {
  res.setHeader('X-Frame-Options', 'ALLOWALL');
  next();
});

// -------- GLOBALS --------
const countries = ["india", "united-states", "united-kingdom", "australia", "germany", "france", "singapore", "netherlands", "albania", "algeria", "andorra", "angola", "antigua-and-barbuda", "argentina", "armenia", "aruba", "austria", "azerbaijan", "the-bahamas", "bahrain", "belgium", "bolivia", "bosnia-and-herzegovina", "botswana", "brazil", "brunei", "bulgaria", "cambodia", "cameroon", "canada", "central-african-republic", "chile", "china", "colombia", "congo", "democratic-republic-of-the-congo", "costa-rica", "croatia", "curacao", "cyprus", "czech-republic", "denmark", "dominican-republic", "ecuador", "egypt", "el-salvador", "estonia", "fiji", "finland", "gambia", "ghana", "greece", "greenland", "grenada", "guatemala", "guernsey", "guinea", "guyana", "haiti", "italy--roma", "honduras", "hong-kong-sar", "hungary", "iceland", "indonesia", "iraq", "ireland", "isle-of-man", "israel", "italy", "jamaica", "japan", "jersey", "jordan", "kazakhstan", "kenya", "south-korea", "kuwait", "latvia", "lebanon", "liberia", "libya", "liechtenstein", "lithuania", "luxembourg", "mauritius", "mexico", "moldova", "monaco", "mongolia", "montenegro", "morocco", "namibia", "nepal", "new-zealand", "nicaragua", "nigeria", "niue", "norway", "oman", "pakistan", "panama", "papua-new-guinea", "paraguay", "peru", "philippines", "poland", "portugal", "qatar", "romania", "russia", "rwanda", "saint-kitts-and-nevis", "saint-lucia", "saint-vincent-and-the-grenadines", "san-marino", "saudi-arabia", "senegal", "serbia", "sint-maarten", "slovakia", "slovenia", "south-africa", "spain", "sri-lanka", "suriname", "sweden", "switzerland", "taiwan", "tajikistan", "tanzania", "thailand", "togo", "trinidad-and-tobago", "tunisia", "turkey", "turkmenistan", "uganda", "ukraine", "united-arab-emirates", "uruguay", "uzbekistan", "venezuela", "vietnam", "zambia", "zimbabwe"];
const allIDs = new Map();


async function loadExistingIDs(pool) {
  const result = await pool.request()
    .query("SELECT eventID FROM event");

  return new Set(result.recordset.map(row => String(row.eventID)));
}

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
    const url = `https://www.eventbriteapi.com/v3/events/${eventID}/?expand=organizer,category,subcategory,venue&token=UGKZE2XZOKOS4YOPKL3L`;
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
    const url = `https://www.eventbriteapi.com/v3/events/${eventID}/ticket_classes/?token=WR6UPGT7SQCLLE5CD2VR`;
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
      .query(`
MERGE event AS target
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
    numberOfseats=@capacity,
    content=@desc
WHEN NOT MATCHED THEN
INSERT (eventID,event_title,event_desc,edate,EventEndDate,address,city,state,zipcode,contact_name,location,status,raccurance,url,fee,event_type,event_subType,CompanyName,numberOfseats,content)
VALUES (@id,@title,@desc,@start,@end,@address,@city,@state,@zip,@org,@loc,@status,@racc,@url,@fee,@cat,@sub,@org,@capacity,@desc);
        `);

    console.log("Saved:", event.id);
  } catch (err) {
    console.log("DB Error:", err.message);
  }
}

// -------- SCRAPER --------
async function scrapeCountry(pool, country, existingIDs) {
  let page = 1;
  let lastIDs = [];

  while (true) {
    let url = `https://www.eventbrite.com/d/${country}/all-events/?page=${page}`;
    let html = await getHTML(url);

    if (!html) break;

    let ids = extractIDs(html);

    if (ids.length === 0 || equalArrays(ids, lastIDs)) break;

    for (let id of ids) {
      id = String(id);

      // ✅ Skip if already processed in current run
      if (allIDs.has(id)) continue;

      allIDs.set(id, true);

      // ✅ Skip if already in DB
      if (existingIDs.has(id)) {
        console.log("Skipped (already in DB):", id);
        continue; // 🚀 NO API CALL
      }

      console.log("Fetching:", id);

      let event = await fetchEventDetails(id);

      if (event) {
        await saveEvent(pool, event);
        console.log("Saved:", event.id);

        // ✅ Add to existing set so not repeated again
        existingIDs.add(id);
      } else {
        console.log("Failed:", id);
      }

      await new Promise(r => setTimeout(r, 300));
    }

    lastIDs = ids;
    page++;
  }
}

// -------- MAIN --------
async function start() {
  try {
    const pool = await sql.connect(dbConfig);
    console.log("Connected to DB ✅");

    // ✅ Load all existing IDs from DB (IMPORTANT)
    const existingIDs = await loadExistingIDs(pool);

    for (let country of countries) {
      await scrapeCountry(pool, country, existingIDs);
    }

    console.log("Total Events:", allIDs.size);
  } catch (err) {
    console.error(err);
  }
}

app.get("/", (req, res) => {
  allIDs.clear();
  start().catch(err => console.log("Scraper Error:", err));
  res.send(`
        <h2>Scraper started ✅</h2>
        <p><a href="/status">View Processed IDs</a></p>
    `);
});

app.get("/status", (req, res) => {
  let output = "<h2>Processed IDs</h2>";
  output += `<p>Total: ${allIDs.size}</p>`;
  output += "<ul>";

  for (let id of Array.from(allIDs.keys()).slice(0, 100)) {
    output += `<li>${id}</li>`;
  }

  output += "</ul>";

  res.send(output);
});
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log("Server started on port", PORT);
});
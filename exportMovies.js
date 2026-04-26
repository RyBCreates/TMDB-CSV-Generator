require("dotenv").config();

const fs = require("fs");
const axios = require("axios");
const csvParser = require("csv-parser");
const pLimit = require("p-limit").default;
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

const API_KEY = process.env.TMDB_KEY;
const limit = pLimit(5);

const INPUT_CSV = "2026-03-12 Rated Movies.csv";
const OUTPUT_CSV = "simms_movies.csv";

const csvWriter = createCsvWriter({
  path: OUTPUT_CSV,
  header: [
    { id: "title", title: "Title" },
    { id: "releaseYear", title: "Release Year" },
    { id: "director", title: "Director" },
    { id: "dp", title: "DP" },

    { id: "editor", title: "Editor" },
    { id: "productionDesigner", title: "Production Designer" },
    { id: "writer", title: "Writer" },
    { id: "producer", title: "Producer" },
    { id: "actors", title: "Actors" },
    { id: "studio", title: "Studio" },
    { id: "countryOfOrigin", title: "Country of Origin" },
    { id: "originalLanguageTitle", title: "Title in Original Language" },

    { id: "tmdbId", title: "TMDb ID" },
    { id: "imdbId", title: "IMDb ID" },
    { id: "yourRating", title: "Your Rating" },
  ],
});

function readInputCsv() {
  return new Promise((resolve, reject) => {
    const rows = [];

    fs.createReadStream(INPUT_CSV)
      .pipe(csvParser())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

async function getMovieDetails(tmdbId) {
  const url = `https://api.themoviedb.org/3/movie/${tmdbId}`;

  const response = await axios.get(url, {
    params: {
      api_key: API_KEY,
      append_to_response: "credits",
    },
  });

  return response.data;
}

function namesByJobs(crew, jobs) {
  return crew
    .filter((person) => jobs.includes(person.job))
    .map((person) => person.name)
    .filter(Boolean)
    .filter((name, index, arr) => arr.indexOf(name) === index)
    .join("|");
}

async function processMovie(row) {
  try {
    const tmdbId = row["TMDb ID"];

    if (!tmdbId || row["Type"] !== "movie") {
      return null;
    }

    const movie = await getMovieDetails(tmdbId);

    const crew = movie.credits?.crew || [];
    const cast = movie.credits?.cast || [];

    return {
      title: movie.title || row["Name"] || "",
      releaseYear: movie.release_date?.split("-")[0] || "",
      director: namesByJobs(crew, ["Director"]),
      dp: namesByJobs(crew, ["Director of Photography", "Cinematography"]),

      editor: namesByJobs(crew, ["Editor"]),
      productionDesigner: namesByJobs(crew, [
        "Production Design",
        "Production Designer",
      ]),
      writer: namesByJobs(crew, ["Writer", "Screenplay", "Story"]),
      producer: namesByJobs(crew, ["Producer"]),
      actors: cast
        .slice(0, 10)
        .map((actor) => actor.name)
        .join("|"),

      studio: (movie.production_companies || [])
        .map((company) => company.name)
        .join("|"),

      countryOfOrigin: (movie.production_countries || [])
        .map((country) => country.name)
        .join("|"),

      originalLanguageTitle: movie.original_title || "",

      tmdbId,
      imdbId: row["IMDb ID"] || movie.imdb_id || "",
      yourRating: row["Your Rating"] || "",
    };
  } catch (error) {
    console.error(`Failed on ${row["Name"]} / TMDb ID ${row["TMDb ID"]}`);

    return {
      title: row["Name"] || "",
      releaseYear: row["Release Date"]?.split("-")[0] || "",
      director: "",
      dp: "",
      editor: "",
      productionDesigner: "",
      writer: "",
      producer: "",
      actors: "",
      studio: "",
      countryOfOrigin: "",
      originalLanguageTitle: "",
      tmdbId: row["TMDb ID"] || "",
      imdbId: row["IMDb ID"] || "",
      yourRating: row["Your Rating"] || "",
    };
  }
}

async function main() {
  if (!API_KEY) {
    throw new Error("Missing TMDB_KEY in .env file");
  }

  const inputMovies = await readInputCsv();

  console.log(`Loaded ${inputMovies.length} rows from CSV`);

  const tasks = inputMovies.map((row) => limit(() => processMovie(row)));

  const results = (await Promise.all(tasks)).filter(Boolean);

  await csvWriter.writeRecords(results);

  console.log(`CSV Export Complete: ${OUTPUT_CSV}`);
}

main().catch(console.error);

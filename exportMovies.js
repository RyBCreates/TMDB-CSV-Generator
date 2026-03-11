require("dotenv").config();
const axios = require("axios");
const pLimit = require("p-limit").default;
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

const API_KEY = process.env.TMDB_KEY;
const limit = pLimit(5);

const csvWriter = createCsvWriter({
  path: "movies.csv",
  header: [
    { id: "title", title: "Title" },
    { id: "year", title: "Year" },
    { id: "director", title: "Director" },
    { id: "dop", title: "Cinematographer" },
    { id: "actors", title: "Actors" },
    { id: "rating", title: "TMDB_Rating" },
  ],
});

async function getMoviesPage(page) {
  const url = "https://api.themoviedb.org/3/discover/movie";

  const response = await axios.get(url, {
    params: {
      api_key: API_KEY,
      page,
      vote_count_gte: 200,
      sort_by: "popularity.desc",
    },
  });

  return response.data.results;
}

async function getCredits(movieId) {
  const url = `https://api.themoviedb.org/3/movie/${movieId}/credits`;

  const response = await axios.get(url, {
    params: { api_key: API_KEY },
  });

  return response.data;
}

async function processMovie(movie) {
  const credits = await getCredits(movie.id);

  let director = "";
  let dop = "";

  credits.crew.forEach((c) => {
    if (c.job === "Director") director = c.name;
    if (c.job === "Director of Photography") dop = c.name;
  });

  const actors = credits.cast
    .slice(0, 5)
    .map((a) => a.name)
    .join("|");

  return {
    title: movie.title,
    year: movie.release_date?.split("-")[0] || "",
    director,
    dop,
    actors,
    rating: movie.vote_average,
  };
}

async function main() {
  let movies = [];

  for (let page = 1; page <= 100; page++) {
    console.log("Fetching page", page);

    const pageMovies = await getMoviesPage(page);

    const tasks = pageMovies.map((movie) => limit(() => processMovie(movie)));

    const results = await Promise.all(tasks);

    movies.push(...results);
  }

  await csvWriter.writeRecords(movies);

  console.log("CSV Export Complete!");
}

main();

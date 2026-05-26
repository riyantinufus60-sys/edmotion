const mysql = require("mysql2");

const db = mysql.createConnection({
  host    : process.env.DB_HOST     || "localhost",
  port    : process.env.DB_PORT     || 3306,
  user    : process.env.DB_USER     || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_NAME     || "railway",
});

db.connect((err) => {
  if (err) {
    console.error("[DB] faceexpresi gagal connect:", err.message);
  } else {
    console.log("[DB] faceexpresi connected ke database:", process.env.DB_NAME || "railway");
  }
});

module.exports = db;

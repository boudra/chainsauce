import dotenv from "dotenv";

dotenv.config();

export default {
  storageDir: process.env.STORAGE_DIR || "./data",
  port: Number(process.env.PORT || "4000"),
};

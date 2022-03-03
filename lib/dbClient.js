import { config } from "dotenv";
config();

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

const REGION = "us-east-2";

const dbClient = new DynamoDBClient({
  region: REGION,
  endpoint: "http://localhost:8000",
  credentials: {
    accessKeyId: process.env.ACCESS_KEY_ID,
    secretAccessKey: process.env.SECRET_ACCESS_KEY,
  },
});

export default dbClient;
import { config } from "dotenv";
config();
import express from "express";
import fs from "fs";

import dbClient from "./lib/dbClient.js";
import ddbDocClient from "./lib/ddbDocClient.js";
import { CreateTableCommand } from "@aws-sdk/client-dynamodb";
import { PutCommand, GetCommand, UpdateCommand, DeleteCommand } from "@aws-sdk/lib-dynamodb";

const app = express();

// Body parser
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// Create table
app.post("/create-table", async (req, res) => {
  const params = {
    TableName: "Movies",
    KeySchema: [
      { AttributeName: "year", KeyType: "HASH" }, //Partition key
      { AttributeName: "title", KeyType: "RANGE" }, //Sort key
    ],
    AttributeDefinitions: [
      { AttributeName: "year", AttributeType: "N" },
      { AttributeName: "title", AttributeType: "S" },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10,
    },
  };
  try {
    const result = await dbClient.send(new CreateTableCommand(params));
    res.json(result);
  } catch (error) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

// Load Sample Data
// Put items using Document Client (PutCommand)
app.post("/load-sample-data", async (req, res) => {
  const allMovies = JSON.parse(
    fs.readFileSync("./sample-data/moviedata.json", "utf8")
  );
  allMovies.forEach(async (movie) => {
    let params = {
      TableName: "Movies",
      Item: {
        year: movie.year,
        title: movie.title,
        info: movie.info,
      },
    };
    try {
      const data = await ddbDocClient.send(new PutCommand(params));
      console.log("Success - item added or updated", data);
    } catch (err) {
      console.log("Error", err);
      res.json({ message: err.message });
    }
  });
  res.json({ message: "Success" });
});

// Create new item
app.post("/create-item", async (req, res) => {
  const params = {
    TableName: "Movies",
    Item: {
      year: req.body.year,
      title: req.body.title,
      info: req.body.info,
    },
  };
  try {
    const data = await ddbDocClient.send(new PutCommand(params));
    console.log("Success - item added or updated", data);
    res.json(data);
  } catch (err) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

// Get item using Document Client (GetCommand)
app.get("/get-item", async (req, res) => {
  const params = {
    TableName: "Movies",
    Key: {
      year: parseInt(req.query.year),
      title: req.query.title,
    },
  };
  try {
    const data = await ddbDocClient.send(new GetCommand(params));
    console.log("Success - item retrieved", data);
    res.json(data);
  } catch (err) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

// Update item using Document Client (UpdateCommand)
app.post("/update-item", async (req, res) => {
  const params = {
    TableName: "Movies",
    Key: {
      year: parseInt(req.body.year),
      title: req.body.title,
    },
    UpdateExpression: "set info.rating = :r, info.plot=:p, info.actors=:a",
    ExpressionAttributeValues: {
      ":r": req.body.info.rating,
      ":p": req.body.info.plot,
      ":a": req.body.info.actors,
    },
    ReturnValues: "UPDATED_NEW",
  };
  try {
    const data = await ddbDocClient.send(new UpdateCommand(params));
    console.log("Success - item added or updated", data);
    res.json(data);
  } catch (err) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

// Increment an Atomic Counter
app.post("/increment-rating", async (req, res) => {
  const params = {
    TableName: "Movies",
    Key: {
      year: parseInt(req.body.year),
      title: req.body.title,
    },
    UpdateExpression: "set info.rating = info.rating + :val",
    ExpressionAttributeValues: {
      ":val": 1,
    },
    ReturnValues: "UPDATED_NEW",
  };
  try {
    const data = await ddbDocClient.send(new UpdateCommand(params));
    console.log("Success - item added or updated", data);
    res.json(data);
  } catch (err) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

// Update an item Conditionally
app.post("/update-item-conditionally", async (req, res) => {
  const params = {
    TableName: "Movies",
    Key: {
      year: parseInt(req.body.year),
      title: req.body.title,
    },
    UpdateExpression: "remove info.actors[0]",
    ConditionExpression: "size(info.actors) >= :num",
    ExpressionAttributeValues: {
      ":num": 3,
    },
    ReturnValues: "UPDATED_NEW",
  };
  try {
    const data = await ddbDocClient.send(new UpdateCommand(params));
    console.log("Success - item added or updated", data);
    res.json(data);
  } catch (err) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

// Delete an item Conditionally
app.delete("/delete-item", async (req, res) => {
  const params = {
    TableName: "Movies",
    Key: {
      year: parseInt(req.query.year),
      title: req.query.title,
    },
    ConditionExpression: "info.rating <= :val",
    ExpressionAttributeValues: {
      ":val": 8.0,
    },
  };
  try {
    const data = await ddbDocClient.send(new DeleteCommand(params));
    console.log("Success - item deleted", data);
    res.json(data);
  } catch (err) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

const PORT = 5000 | process.env.PORT;
app.listen(PORT, () => console.log(`server running in PORT ${PORT}`));

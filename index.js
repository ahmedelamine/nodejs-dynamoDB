import { config } from "dotenv";
config();
import express from "express";
import fs from "fs";

import dbClient from "./lib/dbClient.js";
import ddbDocClient from "./lib/ddbDocClient.js";
import { CreateTableCommand } from "@aws-sdk/client-dynamodb";
import {
  PutCommand,
  GetCommand,
  UpdateCommand,
  DeleteCommand,
  QueryCommand,
  ScanCommand,
} from "@aws-sdk/lib-dynamodb";

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

// Query - All movies released in a given year
/*
  ExpressionAttributeNames provides name substitution. 
  You use this because year is a reserved word in Amazon DynamoDB. 
  You can't use it directly in any expression, including KeyConditionExpression. 
  You use the expression attribute name #yr to address this.
  
  ExpressionAttributeValues provides value substitution. 
  You use this because you cannot use literals in any expression, 
  including KeyConditionExpression. You use the expression attribute
  value :yyyy to address this.
*/

// ProjectionExpression specifies the attributes you want in the scan result.
// FilterExpression specifies a condition that returns only items that satisfy the condition. All other items are discarded.

app.get("/query-movies-by-year", async (req, res) => {
  const params = {
    TableName: "Movies",
    KeyConditionExpression: "#yr = :yyyy",
    ExpressionAttributeNames: {
      "#yr": "year",
    },
    ExpressionAttributeValues: {
      ":yyyy": parseInt(req.query.year),
    },
  };
  try {
    const data = await ddbDocClient.send(new QueryCommand(params));
    // console.log("Success - item retrieved", data);
    res.json(data);
  } catch (err) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

// Query - All Movies Released in a Year with Certain Titles
app.get("/query-movies-by-year-and-title", async (req, res) => {
  const params = {
    TableName: "Movies",
    ProjectionExpression: "#yr, title, info.genres, info.actors",
    KeyConditionExpression:
      "#yr = :yyyy and title between :letter1 and :letter2",
    ExpressionAttributeNames: {
      "#yr": "year",
    },
    ExpressionAttributeValues: {
      ":yyyy": parseInt(req.query.year),
      ":letter1": req.query.letter1,
      ":letter2": req.query.letter2,
    },
  };
  try {
    const data = await ddbDocClient.send(new QueryCommand(params));
    // console.log("Success - item retrieved", data);
    res.json(data);
  } catch (err) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

// Scan
app.get("/scan-movies", async (req, res) => {
  const params = {
    TableName: "Movies",
    ProjectionExpression: "#yr, title, info.rating",
    FilterExpression: "#yr between :start_yr and :end_yr",
    ExpressionAttributeNames: {
      "#yr": "year",
    },
    ExpressionAttributeValues: {
      ":start_yr": parseInt(req.query.start_yr),
      ":end_yr": parseInt(req.query.end_yr),
    },
  };
  try {
    const data = await ddbDocClient.send(new ScanCommand(params));
    // console.log("Success - item retrieved", data);
    res.json(data);
  } catch (err) {
    console.log("Error", err);
    res.json({ message: err.message });
  }
});

const PORT = 5000 | process.env.PORT;
app.listen(PORT, () => console.log(`server running in PORT ${PORT}`));

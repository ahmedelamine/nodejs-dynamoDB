/*
    Purpose:
    ddbDocClient.js is a helper function that creates an Amazon DynamoDB service document client.
*/
import { DynamoDBDocumentClient} from "@aws-sdk/lib-dynamodb";
import dbClient from "./dbClient.js";

const REGION = "us-east-2";

const marshallOptions = {
    // Whether to automatically convert empty strings, blobs, and sets to `null`.
    convertEmptyValues: false, // false, by default.
    // Whether to remove undefined values while marshalling.
    removeUndefinedValues: false, // false, by default.
    // Whether to convert typeof object to map attribute.
    convertClassInstanceToMap: false, // false, by default.
};

const unmarshallOptions = {
    // Whether to return numbers as a string instead of converting them to native JavaScript numbers.
    wrapNumbers: false, // false, by default.
};

const translateConfig = { marshallOptions, unmarshallOptions };

// Create the DynamoDB Document client.
const ddbDocClient = DynamoDBDocumentClient.from(dbClient, translateConfig);

export default ddbDocClient;


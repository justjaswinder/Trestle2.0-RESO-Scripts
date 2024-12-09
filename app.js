const express = require("express");
const oauth2 = require("simple-oauth2");
const app = express();
const axios = require("axios");
const cron = require("node-cron");
const mysql = require("mysql");
const odata = require("odata");
const xmljs = require("xml-js");
const { Upload } = require("@aws-sdk/lib-storage");
const fs = require('fs');
const path = require('path');
require("dotenv").config();

// Initialize axios-retry  

const { S3 } = require("@aws-sdk/client-s3");

let isFinished = false;
// Configuration of S3 client
const s3 = new S3({
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID, // Recommended to use environment variables
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
  region: "us-east-1", // Ensure you use the correct region
});

const db = mysql.createConnection({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
});

db.connect((error) => {
  if (error) throw error;
  console.log("Successfully connected to the database.");
});

let latestUpdatedTime;

db.query(
  "SELECT MAX(updated_date) AS LatestUpdate FROM latest_date",
  (error, results, fields) => {
    if (results == undefined) {
      latestUpdatedTime = 0;
    } else {
      latestUpdatedTime = new Date(results[0].LatestUpdate).getTime();
    }
  }
);

async function authenticate() {
  const trestleConfig = {
    client: {
      id: process.env.CLIENT_ID,
      secret: process.env.CLIENT_SECRET,
    },
    auth: {
      tokenHost: process.env.TOKEN_HOST,
      tokenPath: process.env.TOKEN_PATH,
    },
  };

  try {
    const o = new oauth2.ClientCredentials(trestleConfig);
    return await o.getToken({ scope: "api" });
  } catch (error) {
    console.log(error);
  }
}

const relations = [
  { edm: "Edm.Int32", sql: "INT" },
  { edm: "Edm.Int64", sql: "INT" },
  { edm: "Edm.String", sql: "TEXT" },
  { edm: "Edm.Boolean", sql: "BOOLEAN" },
  { edm: "Edm.Date", sql: "DATE" },
  { edm: "Edm.DateTimeOffset", sql: "DATETIME" },
  { edm: "Edm.Decimal", sql: "INT" },
];

function createMetaData(metaDataTableName, field) {
  let insertingData = "";
  if (field.name == "Property" || field.name == "NavigationProperty") {
    let relation = relations.find((r) => r.edm == field.attributes.Type);
    if (relation) {
      insertingData = `'${field.attributes.Name}', '${relation.sql}'`;
    } else {
      insertingData = `'${field.attributes.Name}', 'ENUM'`;
    }
    const insertMetaDataQuery = `
      INSERT INTO ${metaDataTableName.toLowerCase()} (field, type)
      SELECT '${field.attributes.Name}', '${relation ? relation.sql : "ENUM"}'
      FROM dual
      WHERE NOT EXISTS (
          SELECT 1
          FROM ${metaDataTableName.toLowerCase()}
          WHERE field = '${field.attributes.Name}'
      );
    `;
    db.query(insertMetaDataQuery, (err, result) => {
      if (err) throw err;
      // console.log("data inserted");
    });
  } else return false;
}

async function createMetaDataTable(metadata) {
  try {
    const parsedData = JSON.parse(metadata);
    if (
      parsedData.elements &&
      parsedData.elements[0].elements &&
      parsedData.elements[0].elements[0].elements
    ) {
      parsedData.elements[0].elements[0].elements.forEach(async (schema) => {
        await createTables(schema);
      });
    } else {
      console.log(
        "Expected data structure is not met in the provided metadata."
      );
    }
  } catch (e) {
    console.log("Failed to parse metadata or process data:", e);
  }
}

async function saveDataHandle(element) {
  try {

    if (element == "Property") {

      let nextLink = "https://api-prod.corelogic.com/trestle/odata/Property?$expand=Media";

      console.log("fetching data ...");

      while (nextLink) {

        const accessToken = await authenticate();
        const trestle = odata.o(process.env.API_URL, {
          headers: {
            Authorization: "Bearer " + accessToken.token.access_token,
          },
          fragment: "",
        });

        let property = await trestle
          .get(nextLink)
          .query()
          .catch((e) => console.log(e));

        // Collect all unique property keys from all items
        nextLink = property["@odata.nextLink"];
        
        const allPropertyKeys = [
          ...new Set(property.value.flatMap(Object.keys)),
        ];

        // Fetch all field IDs at once
        const getFieldIdQuery = `SELECT id, field FROM ${element.toLowerCase()}_metadata WHERE field IN (${allPropertyKeys
          .map((key) => `"${key}"`)
          .join(", ")})`;
        const metadataResults = await new Promise((resolve, reject) => {
          db.query(getFieldIdQuery, (err, result) => {
            if (err) return reject(err);
            resolve(
              result.reduce(
                (acc, item) => ({ ...acc, [item.field]: item.id }),
                {}
              )
            );
          });
        });
        let bulkInsertValues = [];
        let bulkInsertMedias = [];
        for (let propertyItem of property.value) {
          let primaryKey = propertyItem[`ListingKey`];

          if ( 
            new Date(propertyItem["ModificationTimestamp"]).getTime() <
            latestUpdatedTime
          )
            continue;

          let allMediaKeys = [ ...new Set(propertyItem.Media.flatMap(Object.keys))];

          for (let propertyKey of Object.keys(propertyItem)) {
            let fieldId = metadataResults[propertyKey];
            if (
              !fieldId ||
              !propertyItem[propertyKey] ||
              propertyKey == "Media"
            )
              continue;
            bulkInsertValues.push([
              primaryKey,
              fieldId,
              propertyItem[propertyKey],
            ]);
          }

          if(allMediaKeys.length != 0) {
            const getMediaFieldIdQuery = `SELECT id, field FROM media_metadata WHERE field IN (${allMediaKeys
              .map((key) => `"${key}"`)
              .join(", ")})`;

            const mediaMetadataResults = await new Promise((resolve, reject) => {
              db.query(getMediaFieldIdQuery, (err, result) => {
                if (err) return reject(err);
                resolve(
                  result.reduce(
                    (acc, item) => ({ ...acc, [item.field]: item.id }),
                    {}
                  )
                );
              });
            });
            for (let media of propertyItem.Media) {
              if (
                new Date(media["MediaModificationTimestamp"]).getTime() <
                latestUpdatedTime
              )
                continue;
              
              app.delete(
                `uploads/images/2024/${primaryKey}/`,
                async (req, res) => {
                  const key = req.body.key; // Assuming the filename is sent in the request body
  
                  const params = {
                    Bucket: process.env.AWS_S3_BUCKET_NAME,
                    Key: key,
                  };
  
                  try {
                    await s3Client.send(new DeleteObjectCommand(params));
                    res.send({ message: "File successfully deleted" });
                  } catch (err) {}
                }
              );
              let uploadedUrl = await handleMediaUpload(media, primaryKey);
              for (let mediaKey of Object.keys(media)) {
                let mediaFieldId = mediaMetadataResults[mediaKey];
                if (media[mediaKey] == null) continue;
                if (mediaKey == "MediaURL") {
                  bulkInsertMedias.push([media["MediaKey"], mediaFieldId, uploadedUrl]);
                } else {
                  bulkInsertMedias.push([
                    media["MediaKey"],
                    mediaFieldId,
                    media[mediaKey],
                  ]);
                }
              }
              let insertMediaQuery = `
                INSERT INTO media (primary_key, field_id, value)
                  VALUES ?
                  ON DUPLICATE KEY UPDATE
                    value = CASE
                      WHEN primary_key != VALUES(primary_key) THEN VALUES(value)
                      ELSE value
                    END;
              `;
              await new Promise((resolve, reject) => {
                db.query(insertMediaQuery, [bulkInsertMedias], (err, result) =>
                  err ? reject(err) : resolve(result)
                );
              });
              bulkInsertMedias = []
            }
          }

          if (bulkInsertValues.length > 0) {
            const insertQuery = `
              INSERT INTO ${element.toLowerCase()} (primary_key, field_id, value)
              VALUES ?
              ON DUPLICATE KEY UPDATE
                value = CASE
                  WHEN primary_key != VALUES(primary_key) THEN VALUES(value)
                  ELSE value
                END;
            `;
            await new Promise((resolve, reject) => {
              db.query(insertQuery, [bulkInsertValues], (err, result) =>
                err ? reject(err) : resolve(result)
              );
            });
            bulkInsertValues = []
          }
        }
      }
    } else {
      
      const accessToken = await authenticate();
      const trestle = odata.o(process.env.API_URL, {
        headers: {
          Authorization: "Bearer " + accessToken.token.access_token,
        },
        fragment: "",
      });
      let allProperty = await trestle
        .get(
          `https://api-prod.corelogic.com/trestle/odata/${element}?&replication=true`
        )
        .query()
        .catch((e) => console.log(e));

      if (allProperty.value)
        allProperty.value.forEach((propertyItem) => {
          const propertyKeys = Object.keys(propertyItem);

          propertyKeys.forEach((propertyKey) => {
            let getFieldIdQuery = `SELECT id FROM ${element.toLowerCase()}_metadata WHERE field = "${propertyKey}"`;

            db.query(getFieldIdQuery, (err, result) => {
              if (err) {
                return;
              }

              if (!result.length) {
                console.log("No field ID found, skipping insertion.");
                return;
              }

              const fieldId = result[0].id;
              if (!propertyItem[propertyKey]) {
                return;
              }

              const insertOrUpdatePropertyData = `
                INSERT INTO ${element.toLowerCase()} (primary_key, field_id, value)
                VALUES (?, ?, ?)
                ON DUPLICATE KEY UPDATE
                  value = CASE
                    WHEN primary_key != VALUES(primary_key) THEN VALUES(value)
                    ELSE value
                  END;
              `;

              const key = propertyItem[`${element}Key`];
              const values = [key, fieldId, propertyItem[propertyKey]];
              db.query(insertOrUpdatePropertyData, values, (err, result) => {
                if (err) {
                  console.log(err);
                  return;
                }
              });
            });
          });
        });
    }
    console.log(`${element} Success`);
  } catch(err) {
    console.log(err);
    
    console.log(`${element} failed`);
  }
}

async function handleMediaUpload(media, primaryKey) {
  try {
    const response = await axios({
      url: media["MediaURL"],
      method: "GET",
      responseType: "stream",
    });

    const fileExtension = media["MediaType"];
    const s3Key = `uploads/images/2024/${primaryKey}/${media["MediaKey"]
      .split("/")
      .pop()}.${fileExtension}`;

    const params = {
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: s3Key,
      Body: response.data,
    };

    const parallelUploads3 = new Upload({
      client: s3,
      params: params,
    });

    const uploadResult = await parallelUploads3.done();
    console.log("Upload successful:", uploadResult.Location);
    return uploadResult.Location;
  } catch (err) {
    return false;
  }
}

async function createTables(schema) {
  schema.elements.forEach((metadata) => {
    if (schema.attributes.Namespace == "CoreLogic.DataStandard.RESO.DD") {
      const TableName = `\`${metadata.attributes.Name.replace(/\./g, "_")}\``.toLowerCase();
      const metaDataTableName = `\`${metadata.attributes.Name.replace(
        /\./g,
        "_"
      )}_metadata\``.toLowerCase();

      let createMetaDataQuery = `
        CREATE TABLE IF NOT EXISTS ${metaDataTableName.toLowerCase()} (
          id INT AUTO_INCREMENT PRIMARY KEY,
          field TEXT,
          type TEXT,
          description TEXT,
          is_primary BOOLEAN,
          is_deleted BOOLEAN
        )
      `;

      let createTableQuery = `
        CREATE TABLE IF NOT EXISTS ${TableName.toLowerCase()} (
          id INT AUTO_INCREMENT PRIMARY KEY,
          primary_key TEXT,
          field_id INT,
          value TEXT,
          FOREIGN KEY (field_id) REFERENCES ${metaDataTableName.toLowerCase()}(id),
          is_deleted BOOLEAN,
          is_updated BOOLEAN
        );
      `;

      db.query(createMetaDataQuery, (err, result) => {
        if (err) throw err;
        // console.log("MetaDataTable created");

        db.query(createTableQuery, (err, result) => {
          if (err) throw err; // Handling error
          // console.log("Table table created");
        });
      });

      metadata.elements.forEach((field) => {
        createMetaData(metaDataTableName, field);
      });
    } 
    else if (
      schema.attributes.Namespace === "CoreLogic.DataStandard.RESO.WebAPI"
    ) {
      const TableName = `\`${metadata.attributes.Name.replace(/\./g, "_")}\``;

      let columns = [];
      const promises = metadata.elements.map((column) => {
        // Using map to convert each item to a promise
        return new Promise((resolve, reject) => {
          // Encapsulating the processing of each column in a promise
          if (column.name === "Property") {
            let relation = relations.find(
              (item) => item.edm === column.attributes.Type
            );

            columns.push(
              `${column.attributes.Name} ${relation ? relation.sql : "TEXT"}`
            );
          }
          resolve(); // Resolving the promise after processing each column
        });
      });

      Promise.all(promises)
        .then(() => {
          let createTableQuery = `CREATE TABLE IF NOT EXISTS  ${TableName.toLowerCase()} (${columns.join(
            ", "
          )})
            `;
          db.query(createTableQuery, (err, result) => {
            if (err) throw err; // Handling error
          });
        })
        .catch((error) => {
          console.error("There was an error processing the columns:", error);
        });
    } else if (
      schema.attributes.Namespace === "CoreLogic.DataStandard.RESO.DD.Enums" ||
      schema.attributes.Namespace ===
        "CoreLogic.DataStandard.RESO.DD.Enums.Multi"
    ) {
      const TableName = `\`${metadata.attributes.Name.replace(/\./g, "_")}\``;

      db.query(
        `CREATE TABLE IF NOT EXISTS ${TableName.toLowerCase()} (id INT AUTO_INCREMENT PRIMARY KEY, Name TEXT, Value TEXT, Description TEXT)`,
        (err, result) => {
          if (err) throw err; // Handling error
        }
      );

      metadata.elements.forEach((enumValue) => {
        if (enumValue.elements) {
          const standardNameElement = enumValue.elements.find(
            (element) =>
              element.attributes.Term === "RESO.OData.Metadata.StandardName"
          );

          if (standardNameElement) {
            const fieldName = enumValue.attributes.Name;
            const value = standardNameElement.attributes.String; // Obtaining the standard name string

            // Using placeholders (?) for parameters in the query
            let insertEnumDataQuery = `
                INSERT INTO ${TableName.toLowerCase()} (Name, value)
                SELECT ?, ? WHERE NOT EXISTS (
                    SELECT 1 FROM ${TableName.toLowerCase()} WHERE Name = ?
                );
            `;

            // Execute query with parameters
            db.query(
              insertEnumDataQuery,
              [fieldName, value, fieldName],
              (err, result) => {
                if (err) {
                  console.error("Error executing query:", err);
                  return;
                }
              }
            );
          }
        }
      });
    } else {
      return false;
    }
  });
}

async function fetchDataAndProcess() {
  //Autentication
  const accessToken = await authenticate();
  const trestle = odata.o(process.env.API_URL, {
    headers: {
      Authorization: "Bearer " + accessToken.token.access_token,
    },
    fragment: "",
  });

  //fetch metadata
  let originMetadata = await trestle
    .get("$metadata")
    .fetch()
    .catch((e) => console.log(e));

  let text = await originMetadata.text();
  let metadata = xmljs.xml2json(text, { compact: false, spaces: 4 });

  //create metadata tables
  await createMetaDataTable(metadata);
  // Fetch data and save the data
  const elements = [
    "Property",
    // "Media",
    "Member",
    "Teams",
    "TeamMembers",
    "Field",
    "Lookup",
    "Office",
    "OpenHouse",
    "PropertyRooms",
    "PropertyUnitTypes",
  ];

  for (const element of elements) {
    await saveDataHandle(element);
  }
  // elements.map(async (element) => {
  //   await saveDataHandle(element);
  // })
}

async function startScript() {
  await fetchDataAndProcess();
  latestUpdatedTime = new Date();

  // Format date to MySQL DATETIME format
  const mysqlDateTime = latestUpdatedTime
    .toISOString()
    .slice(0, 19)
    .replace("T", " ");

  let LatestUpdatedDateDBCreationQuery = `
    CREATE TABLE IF NOT EXISTS \`latest_date\` (
      id INT AUTO_INCREMENT PRIMARY KEY,
      updated_date DATETIME
    );
  `;
  db.query(LatestUpdatedDateDBCreationQuery, (err, result) => {
    if (err) throw err;
  });

  db.query(
    'SELECT * FROM `latest_date` WHERE `id` = "1"',
    function (error, results, fields) {
      if (results.length !== 0) {
        var sql = `UPDATE latest_date SET updated_date = ? WHERE id = 1`;
        var params = [mysqlDateTime]; // Parameters used in the SQL query
      } else {
        var sql = `INSERT INTO latest_date (updated_date) VALUES (?)`;
        var params = [mysqlDateTime];
      }

      // Use these in your query call
      db.query(sql, params, (err, result) => {
        if (err) throw err;
      });
    }
  );
}

// Schedule the tas k to run every 5 minutes
app.listen(3000, () => {
  console.log("Server running on http://localhost:3000");
  console.log("This is version 2.1.1 project for trestle");
  startScript(() => {
    // Assuming startScript is asynchronous and calling back when done
    isFinished = true;
    checkAndStartCronJob();
  });
});

function checkAndStartCronJob() {
  if (isFinished) {
    cron.schedule("*/20 * * * *", () => {
      console.log("---------Start Updating-------------");
      startScript();
    });
  }
}

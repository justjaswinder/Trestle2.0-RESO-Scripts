const express = require("express");
const oauth2 = require("simple-oauth2");
const app = express();
const axios = require("axios");
const cron = require("node-cron");
const mysql = require("mysql");
const AWS = require("aws-sdk");
const odata = require("odata");
const xmljs = require("xml-js");

require("dotenv").config();

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: "us-east-1", // change to your region
});

const s3 = new AWS.S3();

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
      INSERT INTO ${metaDataTableName} (field, type)
      SELECT '${field.attributes.Name}', '${relation ? relation.sql : "ENUM"}'
      FROM dual
      WHERE NOT EXISTS (
          SELECT 1
          FROM ${metaDataTableName}
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
      parsedData.elements[0].elements[0].elements.forEach((schema) => {
        createTables(schema);
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
  const accessToken = await authenticate();
  const trestle = odata.o(process.env.API_URL, {
    headers: {
      Authorization: "Bearer " + accessToken.token.access_token,
    },
    fragment: "",
  });

  let allProperty = await trestle
    .get(element)
    .query({ $top: 2 })
    .catch((e) => console.log(e));

  if (allProperty.value)
    allProperty.value.forEach((propertyItem) => {
      const propertyKeys = Object.keys(propertyItem);

      propertyKeys.forEach((propertyKey) => {
        let getFieldIdQuery = `SELECT id FROM ${element}_metadata WHERE field = "${propertyKey}"`;

        db.query(getFieldIdQuery, (err, result) => {
          if (err) {
            console.log(err);
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
            INSERT INTO ${element} (primary_key, field_id, value)
            VALUES (?, ?, ?)
            ON DUPLICATE KEY UPDATE
              value = CASE
                WHEN primary_key != VALUES(primary_key) THEN VALUES(value)
                ELSE value
              END;
          `;

          const key = propertyItem[`${element}Key`];

          if (
            element === "Media" &&
            propertyKey === "MediaURL" &&
            propertyItem[propertyKey]
          ) {
            try {
              axios({
                url: propertyItem[propertyKey],
                method: "GET",
                responseType: "stream",
              })
                .then((response) => {
                  const params = {
                    Bucket: process.env.AWS_S3_BUCKET_NAME,
                    Key: `uploads/${Date.now()}-${propertyItem[propertyKey]
                      .split("/")
                      .pop()}.${propertyItem["MediaType"]}`,
                    Body: response.data,
                  };

                  s3.upload(params)
                    .promise()
                    .then((data) => {
                      console.log("Upload successful:", data.Location);
                      const values = [key, fieldId, data.Location];

                      db.query(
                        insertOrUpdatePropertyData,
                        values,
                        (err, result) => {
                          if (err) {
                            console.log("err");
                            return;
                          }
                        }
                      );
                    })
                    .catch((err) => {
                      console.log("Error during S3 upload:", err);
                    });
                  // Assume you have the S3 upload logic here:
                  // You might want to set up AWS SDK or similar to handle it.
                })
                .catch(console.log);
            } catch (err) {
              console.log("Error processing propertyKey:");
            }
          } else {
            const values = [key, fieldId, propertyItem[propertyKey]];

            db.query(insertOrUpdatePropertyData, values, (err, result) => {
              if (err) {
                console.log(err);
                return;
              }
            });
          }
        });
      });
      console.log("data is saved");
    });
}

async function createTables(schema) {
  schema.elements.forEach((metadata) => {
    if (schema.attributes.Namespace == "CoreLogic.DataStandard.RESO.DD") {
      const TableName = `\`${metadata.attributes.Name.replace(/\./g, "_")}\``;
      const metaDataTableName = `\`${metadata.attributes.Name.replace(
        /\./g,
        "_"
      )}_metadata\``;

      let createMetaDataQuery = `
        CREATE TABLE IF NOT EXISTS ${metaDataTableName} (
          id INT AUTO_INCREMENT PRIMARY KEY,
          field VARCHAR(100),
          type VARCHAR(100),
          description VARCHAR(250),
          is_primary BOOLEAN,
          is_deleted BOOLEAN
        )
      `;

      let createTableQuery = `
        CREATE TABLE IF NOT EXISTS ${TableName} (
          id INT AUTO_INCREMENT PRIMARY KEY,
          primary_key VARCHAR(100),
          field_id INT,
          value VARCHAR(250),
          FOREIGN KEY (field_id) REFERENCES ${metaDataTableName}(id)
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
    else if (schema.attributes.Namespace === "CoreLogic.DataStandard.RESO.WebAPI") {
      const TableName = `\`${metadata.attributes.Name.replace(/\./g, "_")}\``;

      let columns = [];
      const promises = metadata.elements.map(column => {  // Using map to convert each item to a promise
          return new Promise((resolve, reject) => {  // Encapsulating the processing of each column in a promise
            if (column.name === "Property") {
              let relation = relations.find(item => item.edm === column.attributes.Type);

              columns.push(`${column.attributes.Name} ${relation ? relation.sql : 'TEXT'}`);
            }
            resolve();  // Resolving the promise after processing each column
          });
      });

      Promise.all(promises)
        .then(() => {
          let createTableQuery = `CREATE TABLE IF NOT EXISTS  ${TableName} (${columns.join(", ")})
            `
          db.query(createTableQuery, (err, result) => {
            if (err) throw err; // Handling error
          })
        })
        .catch(error => {
          console.error('There was an error processing the columns:', error);
      });
    }
    else if(schema.attributes.Namespace === "CoreLogic.DataStandard.RESO.DD.Enums" || schema.attributes.Namespace === "CoreLogic.DataStandard.RESO.DD.Enums.Multi") {
      const TableName = `\`${metadata.attributes.Name.replace(/\./g, "_")}\``;

      db.query(`CREATE TABLE IF NOT EXISTS ${TableName} (id INT AUTO_INCREMENT PRIMARY KEY, Name TEXT, Value TEXT, Description TEXT)`, (err, result) => {
        if (err) throw err; // Handling error
      })

      metadata.elements.forEach(enumValue => {
        if(enumValue.elements) {
          const standardNameElement = enumValue.elements.find(element => element.attributes.Term === "RESO.OData.Metadata.StandardName");

          if (standardNameElement) {
            const fieldName = enumValue.attributes.Name; // I'm assuming enumValue has a Name attribute representing the field
            const value = standardNameElement.attributes.String; // Obtaining the standard name string

            let insertEnumDataQuery = `
              INSERT INTO ${TableName} (Name, value)
              SELECT '${escape(fieldName)}', '${escape(value)}'
              WHERE NOT EXISTS (
                SELECT 1 FROM ${TableName} WHERE Name = '${escape(fieldName)}'
              );
            `;

            db.query(insertEnumDataQuery, (err, result) => {
              if (err) {
                return;
              }
            });
          }
        }

      });
    }
    else {
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
    "Member",
    "Teams",
    "TeamMembers",
    "Field",
    "Lookup",
    "Office",
    "OpenHouse",
    "PropertyRooms",
    "PropertyUnitTypes",
    "Media",
  ];
  await Promise.all(
    elements.map(async (element) => {
      await saveDataHandle(element);
    })
  );
}

// Schedule the tas k to run every 5 minutes
// cron.schedule('*/1 * * * *', () => {
//    // Call your function  // Call your function
// });

app.listen(3000, () => {
  console.log("Server running on http://localhost:3000");
  fetchDataAndProcess();
});

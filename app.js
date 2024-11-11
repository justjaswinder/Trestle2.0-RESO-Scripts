const express = require('express');
const app = express();
const cron = require('node-cron');
const mysql = require('mysql');
const AWS = require('aws-sdk');
// const { fetchDataAndProcess } = require('./yourDataFunction');  // Ensure this is correctly imported

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: 'us-east-1' // change to your region
});

const s3 = new AWS.S3();

const db = mysql.createConnection({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE
});

db.connect(error => {
  if (error) throw error;
  console.log("Successfully connected to the database.");
});



async function fetchDataAndProcess() {
  try {
    const response = await axios.get(process.env.TRESTLE_ENDPOINT, {
      headers: { 'Authorization': `Bearer ${process.env.TRESTLE_API_KEY}` }
    });

    // Assuming the images come as URLs in the response
    const images = response.data.images; // Adjust this according to the actual response structure

    for (let imageUrl of images) {
      uploadImageToS3(imageUrl);
    }
  } catch (error) {
    console.error('Error fetching data:', error);
  }

  try {
    const imageResponse = await axios.get(imageUrl, { responseType: 'arraybuffer' });
    const contentType = imageResponse.headers['content-type'];
    const buffer = Buffer.from(imageResponse.data, 'binary');
    const imageName = path.basename(imageUrl);

    const params = {
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: `images/${uuidv4()}-${imageName}`,
      Body: buffer,
      ContentType: contentType
    };

    const s3Upload = await s3.upload(params).promise();

    console.log('Successfully uploaded image to S3:', s3Upload.Location);
    saveImageInfoToDatabase(s3Upload.Location, imageName);
  } catch (error) {
    console.error('Failed to upload image to S3:', error);
  }

  const query = 'INSERT INTO images (name, url) VALUES (?, ?)';
  db.query(query, [name, url], (error, results) => {
    if (error) throw error;
    console.log('Saved image info to database:', results.insertId);
  });
}
// Schedule the tas k to run every 5 minutes
cron.schedule('*/1 * * * *', () => {
    console.log('Running data fetch every 5 minutes');
    fetchDataAndProcess();  // Call your function
});

app.listen(3000, () => {
    console.log('Server running on http://localhost:3000');
});
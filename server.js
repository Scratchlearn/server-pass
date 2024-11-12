// const express = require('express');
// const { BigQuery } = require('@google-cloud/bigquery');
// const cors = require('cors');
// const path = require('path');
// const dotenv = require('dotenv');
// dotenv.config();

// const projectId = process.env.GOOGLE_PROJECT_ID;
// const keyFilePath = path.join(__dirname, process.env.GOOGLE_KEY_FILE);
// const bigQueryDataset = process.env.BIGQUERY_DATASET;
// const bigQueryTable = process.env.BIGQUERY_TABLE;
// const bigQueryTable2 = "Per_Key_Per_Day";
// const bigQueryTable3 = "Per_Person_Per_Day"

// // Initialize express app
// const app = express();

// console.log('Key file path:', keyFilePath);
// console.log(bigQueryDataset);
// console.log(bigQueryTable);

// const bigQueryClient = new BigQuery({
//   keyFilename: keyFilePath, // Use forward slashes
//   projectId: projectId, // Your Google Cloud project ID
//   scopes: ['https://www.googleapis.com/auth/drive'], // Add Google Drive scope
// });

// // Middleware setup
// app.use(cors()); // Enable CORS
// app.use(express.json()); // To handle JSON requests

// // Route to get data from BigQuery
// app.get('/api/data', async (req, res) => {
//   try {
//     // Get limit and offset from query parameters, with default values
//     const limit = parseInt(req.query.limit, 1000) || 1000; // default to 10 rows
//     const offset = parseInt(req.query.offset, 1000) || 0; // default to start at the beginning

//     const query = `
//       SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
//       ORDER BY DelCode_w_o__
//       LIMIT @limit OFFSET @offset
//     `;

//     const options = {
//       query: query,
//       params: { limit, offset },
//     };

//     const [rows] = await bigQueryClient.query(options);
//     console.log('Data fetched from BigQuery:', rows);

//     // Group the data by DelCode_w_o__
//     const groupedData = rows.reduce((acc, item) => {
//       const key = item.DelCode_w_o__;
//       if (!acc[key]) {
//         acc[key] = [];
//       }
//       acc[key].push(item);
//       return acc;
//     }, {});

//     res.status(200).json(groupedData);
//   } catch (err) {
//     console.error('Error querying BigQuery:', err.message, err.stack);
//     res.status(500).json({ message: err.message, stack: err.stack });
//   }
// });


// // Route to get data from the Per_Key_Per_Day table with summed Duration
// app.get('/api/per-key-per-day', async (req, res) => {
//   try {
//     const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\``;
//     const [rows] = await bigQueryClient.query(query);

//     // Group the data by Key and sum the Duration
//     const groupedData = rows.reduce((acc, item) => {
//       const key = item.Key;
//       if (!acc[key]) {
//         acc[key] = { totalDuration: 0, entries: [] };
//       }
//       acc[key].totalDuration += parseFloat(item.Duration) || 0; // Add Duration
//       acc[key].entries.push(item); // Add the item itself for reference if needed
//       return acc;
//     }, {});

//     console.log("Grouped data with total duration:", groupedData);
//     res.status(200).json(groupedData);
//   } catch (err) {
//     console.error('Error querying Per_Key_Per_Day:', err.message, err.stack);
//     res.status(500).json({ message: err.message, stack: err.stack });
//   }
// });

// // Route to get data from the Per_Person_Per_Day table with summed Duration
// app.get('/api/per-person-per-day', async (req, res) => {
//   try {
//     const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable3}\``;
//     const [rows] = await bigQueryClient.query(query);

//     // Directly return the fetched rows
//     console.log("Fetched data:", rows);
//     res.status(200).json(rows);
//   } catch (err) {
//     console.error('Error querying Per_Key_Per_Day:', err.message, err.stack);
//     res.status(500).json({ message: err.message, stack: err.stack });
//   }
// });







// // post req
// app.post('/api/data', async (req, res) => {
//   const {
//     Key,
//     Delivery_code,
//     DelCode_w_o__,
//     Step_ID,
//     Task_Details,
//     Frequency___Timeline,
//     Client,
//     Short_description,
//     Planned_Start_Timestamp,
//     Planned_Delivery_Timestamp,
//     Responsibility,
//     Current_Status,
//     Total_Tasks,
//     Completed_Tasks,
//     Planned_Tasks,
//     Percent_Tasks_Completed,
//     Created_at,
//     Updated_at,
//     Time_Left_For_Next_Task_dd_hh_mm_ss,
//     Percent_Delivery_Planned,
//     Card_Corner_Status,
//     sliders // This is expected to be an array of slider data
//   } = req.body;
  
//   console.log(req.body);
  
//   // Check if sliders data is provided
//   if (!sliders || sliders.length === 0) {
//     return res.status(400).send({ error: 'Slider data is mandatory.' });
//   }

//   try {
//     // Handle task details (insert or update)
//     const checkQuery = `SELECT Key FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` WHERE Key = @Key`;
//     const checkOptions = {
//       query: checkQuery,
//       params: { Key },
//       types: { Key: 'INT64' }
//     };

//     const [existingTasks] = await bigQueryClient.query(checkOptions);

//     if (existingTasks.length > 0) {
//       // If task exists, update it
//       const updateQuery = `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` SET 
//         Delivery_code = @Delivery_code,
//         DelCode_w_o__ = @DelCode_w_o__,
//         Step_ID = @Step_ID,
//         Task_Details = @Task_Details,
//         Frequency___Timeline = @Frequency___Timeline,
//         Client = @Client,
//         Short_description = @Short_description,
//         Planned_Start_Timestamp = @Planned_Start_Timestamp,
//         Planned_Delivery_Timestamp = @Planned_Delivery_Timestamp,
//         Responsibility = @Responsibility,
//         Current_Status = @Current_Status,
//         Total_Tasks = @Total_Tasks,
//         Completed_Tasks = @Completed_Tasks,
//         Planned_Tasks = @Planned_Tasks,
//         Percent_Tasks_Completed = @Percent_Tasks_Completed,
//         Created_at = @Created_at,
//         Updated_at = @Updated_at,
//         Time_Left_For_Next_Task_dd_hh_mm_ss = @Time_Left_For_Next_Task_dd_hh_mm_ss,
//         Percent_Delivery_Planned = @Percent_Delivery_Planned,
//         Card_Corner_Status = @Card_Corner_Status
//         WHERE Key = @Key`;

//       const updateOptions = {
//         query: updateQuery,
//         params: {
//           Key,
//           Delivery_code,
//           DelCode_w_o__,
//           Step_ID,
//           Task_Details,
//           Frequency___Timeline,
//           Client,
//           Short_description,
//           Planned_Start_Timestamp,
//           Planned_Delivery_Timestamp,
//           Responsibility,
//           Current_Status,
//           Total_Tasks,
//           Completed_Tasks,
//           Planned_Tasks,
//           Percent_Tasks_Completed,
//           Created_at,
//           Updated_at,
//           Time_Left_For_Next_Task_dd_hh_mm_ss,
//           Percent_Delivery_Planned,
//           Card_Corner_Status
//         },
//         types: {
//           Key: 'INT64',
//           Delivery_code: 'STRING',
//           DelCode_w_o__: 'STRING',
//           Step_ID: 'INT64',
//           Task_Details: 'STRING',
//           Frequency___Timeline: 'STRING',
//           Client: 'STRING',
//           Short_description: 'STRING',
//           Planned_Start_Timestamp: 'TIMESTAMP',
//           Planned_Delivery_Timestamp: 'TIMESTAMP',
//           Responsibility: 'STRING',
//           Current_Status: 'STRING',
//           Total_Tasks: 'INT64',
//           Completed_Tasks: 'INT64',
//           Planned_Tasks: 'INT64',
//           Percent_Tasks_Completed: 'FLOAT64',
//           Created_at: 'STRING',
//           Updated_at: 'STRING',
//           Time_Left_For_Next_Task_dd_hh_mm_ss: 'STRING',
//           Percent_Delivery_Planned: 'FLOAT64',
//           Card_Corner_Status: 'STRING',
//         }
//       };

//       await bigQueryClient.createQueryJob(updateOptions);
//     } else {
//       // If task doesn't exist, insert it
//       const insertQuery = `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` (Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline, Client, Short_description, Planned_Start_Timestamp, Planned_Delivery_Timestamp, Responsibility, Current_Status, Total_Tasks, Completed_Tasks, Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at, Time_Left_For_Next_Task_dd_hh_mm_ss, Percent_Delivery_Planned, Card_Corner_Status)
//       VALUES (@Key, @Delivery_code, @DelCode_w_o__, @Step_ID, @Task_Details, @Frequency___Timeline, @Client, @Short_description, @Planned_Start_Timestamp, @Planned_Delivery_Timestamp, @Responsibility, @Current_Status, @Total_Tasks, @Completed_Tasks, @Planned_Tasks, @Percent_Tasks_Completed, @Created_at, @Updated_at, @Time_Left_For_Next_Task_dd_hh_mm_ss, @Percent_Delivery_Planned, @Card_Corner_Status)`;

//       const insertOptions = {
//         query: insertQuery,
//         params: {
//           Key,
//           Delivery_code,
//           DelCode_w_o__,
//           Step_ID,
//           Task_Details,
//           Frequency___Timeline,
//           Client,
//           Short_description,
//           Planned_Start_Timestamp,
//           Planned_Delivery_Timestamp,
//           Responsibility,
//           Current_Status,
//           Total_Tasks,
//           Completed_Tasks,
//           Planned_Tasks,
//           Percent_Tasks_Completed,
//           Created_at,
//           Updated_at,
//           Time_Left_For_Next_Task_dd_hh_mm_ss,
//           Percent_Delivery_Planned,
//           Card_Corner_Status
//         },
//         types: {
//           Key: 'INT64',
//           Delivery_code: 'STRING',
//           DelCode_w_o__: 'STRING',
//           Step_ID: 'INT64',
//           Task_Details: 'STRING',
//           Frequency___Timeline: 'STRING',
//           Client: 'STRING',
//           Short_description: 'STRING',
//           Planned_Start_Timestamp: 'TIMESTAMP',
//           Planned_Delivery_Timestamp: 'TIMESTAMP',
//           Responsibility: 'STRING',
//           Current_Status: 'STRING',
//           Total_Tasks: 'INT64',
//           Completed_Tasks: 'INT64',
//           Planned_Tasks: 'INT64',
//           Percent_Tasks_Completed: 'FLOAT64',
//           Created_at: 'STRING',
//           Updated_at: 'STRING',
//           Time_Left_For_Next_Task_dd_hh_mm_ss: 'STRING',
//           Percent_Delivery_Planned: 'FLOAT64',
//           Card_Corner_Status: 'STRING',
//         }
//       };

//       await bigQueryClient.createQueryJob(insertOptions);
//     }

//     // Handle slider values (insert into Per_Key_Per_Day)
//     console.log('Received sliders data:', sliders);

//     // Prepare the insert or update queries for sliders
//     const insertOrUpdateSliderQueries = await Promise.all(sliders.map(async (slider) => {
//       const selectQuery = {
//         query: `SELECT duration FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` WHERE Key = @Key AND day = @day LIMIT 1`,
//         params: {
//           Key: Number(Key),
//           day: slider.day,
//         },
//         types: {
//           Key: 'INT64',
//           day: 'STRING',
//         },
//       };

//       // Check if the slider data already exists
//       const [sliderRows] = await bigQueryClient.query(selectQuery);

//       if (sliderRows.length > 0) {
//         // Update existing slider record
//         return {
//           query: `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` SET duration = @duration WHERE Key = @Key AND day = @day`,
//           params: {
//             Key: Number(Key),
//             day: slider.day,
//             duration: Number(slider.duration),
//           },
//           types: {
//             Key: 'INT64',
//             day: 'STRING',
//             duration: 'INT64',
//           },
//         };
//       } else {
//         // Insert new slider record
//         return {
//           query: `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` (Key, day, duration) VALUES (@Key, @day, @duration)`,
//           params: {
//             Key: Number(Key),
//             day: slider.day,
//             duration: Number(slider.duration),
//           },
//           types: {
//             Key: 'INT64',
//             day: 'STRING',
//             duration: 'INT64',
//           },
//         };
//       }
//     }));

//     // Execute each query (either an INSERT or UPDATE) based on the result
//     await Promise.all(
//       insertOrUpdateSliderQueries.map(async (queryOption) => {
//         await bigQueryClient.createQueryJob(queryOption);
//       })
//     );

//     res.status(200).send({ message: 'Task and slider data stored or updated successfully.' });
//   } catch (error) {
//     console.error('Error processing task and slider data:', error);
//     res.status(500).send({ error: 'Failed to store or update task and slider data.' });
//   }
// });

// // Update Task in BigQuery
// app.put('/api/data/:key', async (req, res) => {
//   const { key } = req.params;
//   const { taskName, startDate, endDate, assignTo, status } = req.body;

//   const query = `
//     UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
//     SET Task = @Task_Details, Start_Date = @Planned_Start_Timestamp, End_Date = @Planned_Delivery_Timestamp, Assign_To = @Responsibility, Status = @Current_Status, Client=@Client, Total_Tasks = @Total_Tasks, Planned_Tasks = @Planned_Tasks, Completed_Tasks =@Completed_Tasks, Created_at = @Created_at, Updated_at = @Updated_at
//     WHERE Key = @key
//   `;

//   const options = {
//     query: query,
//     params: { key: parseInt(key), taskName, startDate, endDate, assignTo, status },
//   };

//   try {
//     const [job] = await bigQueryClient.createQueryJob(options);
//     await job.getQueryResults();
//     res.status(200).send({ message: 'Task updated successfully.' });
//   } catch (error) {
//     console.error('Error updating task in BigQuery:', error);
//     res.status(500).send({ error: 'Failed to update task in BigQuery.' });
//   }
// });

// // Delete Task from BigQuery
// app.delete('/api/data/:key', async (req, res) => {
//   const { key } = req.params;

//   const query = `
//     DELETE FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
//     WHERE Key = @key
//   `;

//   const options = {
//     query: query,
//     params: { key: parseInt(key) },
//   };

//   try {
//     const [job] = await bigQueryClient.createQueryJob(options);
//     await job.getQueryResults();
//     res.status(200).send({ message: 'Task deleted successfully.' });
//   } catch (error) {
//     console.error('Error deleting task from BigQuery:', error);
//     res.status(500).send({ error: 'Failed to delete task from BigQuery.' });
//   }
// });

// const PORT = process.env.PORT || 3001;
// app.listen(PORT, () => {
//   console.log(`Server running on port ${PORT}`);
// });
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const dotenv = require('dotenv');
//const { OAuth2Client } = require("google-auth-library");

dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
const keyFilePath = path.join(__dirname, process.env.GOOGLE_KEY_FILE);
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE;
const bigQueryTable2 = "Per_Key_Per_Day";
const bigQueryTable3 = "Per_Person_Per_Day";

const app = express();

// Middleware setup
app.use(cors());
app.use(express.json());

// Security headers to enable COOP and COEP
app.use((req, res, next) => {
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
  res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp');
  next();
});

const bigQueryClient = new BigQuery({
  keyFilename: keyFilePath,
  projectId: projectId,
  scopes: ['https://www.googleapis.com/auth/drive'],
});

// Middleware setup
app.use(cors());
 // Enable CORS
app.use(express.json()); // To handle JSON requests






app.get('/api/data', async (req, res) => {
  try {
    // Get limit, offset, and email from query parameters
    const limit = parseInt(req.query.limit, 500) || 500; // default to 10 rows
    const offset = parseInt(req.query.offset, 500) || 0; // default to start at the beginning
    const email = req.query.email; // email from query parameters

    // Ensure email is provided
    if (!email) {
      return res.status(400).json({ message: 'Email is required' });
    }

   const query = `
    SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
    WHERE Email = @email
    AND Planned_Start_Timestamp IS NULL
    AND Planned_Delivery_Timestamp IS NULL
    ORDER BY DelCode_w_o__
    LIMIT @limit OFFSET @offset
  `;
    const options = {
      query: query,
      params: { limit, offset, email },
    };

    const [rows] = await bigQueryClient.query(options);
    console.log('Data fetched from BigQuery:', rows);

    // Group the data by DelCode_w_o__
    const groupedData = rows.reduce((acc, item) => {
      const key = item.DelCode_w_o__;
      if (!acc[key]) {
        acc[key] = [];
      }
      acc[key].push(item);
      return acc;
    }, {});

    res.status(200).json(groupedData);
  } catch (err) {
    console.error('Error querying BigQuery:', err.message, err.stack);
    res.status(500).json({ message: err.message, stack: err.stack });
  }
});


// Route to get data from the Per_Key_Per_Day table with summed Duration
app.get('/api/per-key-per-day', async (req, res) => {
  try {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\``;
    const [rows] = await bigQueryClient.query(query);

    // Group the data by Key and sum the Duration
    const groupedData = rows.reduce((acc, item) => {
      const key = item.Key;
      if (!acc[key]) {
        acc[key] = { totalDuration: 0, entries: [] };
      }
      acc[key].totalDuration += parseFloat(item.Duration) || 0; // Add Duration
      acc[key].entries.push(item); // Add the item itself for reference if needed
      return acc;
    }, {});

    console.log("Grouped data with total duration:", groupedData);
    res.status(200).json(groupedData);
  } catch (err) {
    console.error('Error querying Per_Key_Per_Day:', err.message, err.stack);
    res.status(500).json({ message: err.message, stack: err.stack });
  }
});

// Route to get data from the Per_Person_Per_Day table with summed Duration
app.get('/api/per-person-per-day', async (req, res) => {
  try {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable3}\``;
    const [rows] = await bigQueryClient.query(query);

    // Directly return the fetched rows
    console.log("Fetched data:", rows);
    res.status(200).json(rows);
  } catch (err) {
    console.error('Error querying Per_Key_Per_Day:', err.message, err.stack);
    res.status(500).json({ message: err.message, stack: err.stack });
  }
});







// post req
app.post('/api/data', async (req, res) => {
  const {
    Key,
    Delivery_code,
    DelCode_w_o__,
    Step_ID,
    Task_Details,
    Frequency___Timeline,
    Client,
    Short_description,
    Planned_Start_Timestamp,
    Planned_Delivery_Timestamp,
    Responsibility,
    Current_Status,
    email,
    Total_Tasks,
    Completed_Tasks,
    Planned_Tasks,
    Percent_Tasks_Completed,
    Created_at,
    Updated_at,
    Time_Left_For_Next_Task_dd_hh_mm_ss,
    Percent_Delivery_Planned,
    Card_Corner_Status,
    sliders // This is expected to be an array of slider data
  } = req.body;
  
  console.log(req.body);
  
  // Check if sliders data is provided
  if (!sliders || sliders.length === 0) {
    return res.status(400).send({ error: 'Slider data is mandatory.' });
  }

  try {
    // Handle task details (insert or update)
    const checkQuery = `SELECT Key FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` WHERE Key = @Key`;
    const checkOptions = {
      query: checkQuery,
      params: { Key },
      types: { Key: 'INT64' }
    };

    const [existingTasks] = await bigQueryClient.query(checkOptions);

    if (existingTasks.length > 0) {
      // If task exists, update it
      const updateQuery = `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` SET 
        Delivery_code = @Delivery_code,
        DelCode_w_o__ = @DelCode_w_o__,
        Step_ID = @Step_ID,
        Task_Details = @Task_Details,
        Frequency___Timeline = @Frequency___Timeline,
        Client = @Client,
        Short_description = @Short_description,
        Planned_Start_Timestamp = @Planned_Start_Timestamp,
        Planned_Delivery_Timestamp = @Planned_Delivery_Timestamp,
        Responsibility = @Responsibility,
        Current_Status = @Current_Status,
        email =  @email,
        Total_Tasks = @Total_Tasks,
        Completed_Tasks = @Completed_Tasks,
        Planned_Tasks = @Planned_Tasks,
        Percent_Tasks_Completed = @Percent_Tasks_Completed,
        Created_at = @Created_at,
        Updated_at = @Updated_at,
        Time_Left_For_Next_Task_dd_hh_mm_ss = @Time_Left_For_Next_Task_dd_hh_mm_ss,
        Percent_Delivery_Planned = @Percent_Delivery_Planned,
        Card_Corner_Status = @Card_Corner_Status
        WHERE Key = @Key`;

      const updateOptions = {
        query: updateQuery,
        params: {
          Key,
          Delivery_code,
          DelCode_w_o__,
          Step_ID,
          Task_Details,
          Frequency___Timeline,
          Client,
          Short_description,
          Planned_Start_Timestamp,
          Planned_Delivery_Timestamp,
          Responsibility,
          Current_Status,
          email,
          Total_Tasks,
          Completed_Tasks,
          Planned_Tasks,
          Percent_Tasks_Completed,
          Created_at,
          Updated_at,
          Time_Left_For_Next_Task_dd_hh_mm_ss,
          Percent_Delivery_Planned,
          Card_Corner_Status
        },
        types: {
          Key: 'INT64',
          Delivery_code: 'STRING',
          DelCode_w_o__: 'STRING',
          Step_ID: 'INT64',
          Task_Details: 'STRING',
          Frequency___Timeline: 'STRING',
          Client: 'STRING',
          Short_description: 'STRING',
          Planned_Start_Timestamp: 'TIMESTAMP',
          Planned_Delivery_Timestamp: 'TIMESTAMP',
          Responsibility: 'STRING',
          Current_Status: 'STRING',
          email:'STRING',
          Total_Tasks: 'INT64',
          Completed_Tasks: 'INT64',
          Planned_Tasks: 'INT64',
          Percent_Tasks_Completed: 'FLOAT64',
          Created_at: 'STRING',
          Updated_at: 'STRING',
          Time_Left_For_Next_Task_dd_hh_mm_ss: 'STRING',
          Percent_Delivery_Planned: 'FLOAT64',
          Card_Corner_Status: 'STRING',
        }
      };

      await bigQueryClient.createQueryJob(updateOptions);
    } else {
      // If task doesn't exist, insert it
      const insertQuery = `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` (Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline, Client, Short_description, Planned_Start_Timestamp, Planned_Delivery_Timestamp, Responsibility, Current_Status,email, Total_Tasks, Completed_Tasks, Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at, Time_Left_For_Next_Task_dd_hh_mm_ss, Percent_Delivery_Planned, Card_Corner_Status)
      VALUES (@Key, @Delivery_code, @DelCode_w_o__, @Step_ID, @Task_Details, @Frequency___Timeline, @Client, @Short_description, @Planned_Start_Timestamp, @Planned_Delivery_Timestamp, @Responsibility, @Current_Status,@email, @Total_Tasks, @Completed_Tasks, @Planned_Tasks, @Percent_Tasks_Completed, @Created_at, @Updated_at, @Time_Left_For_Next_Task_dd_hh_mm_ss, @Percent_Delivery_Planned, @Card_Corner_Status)`;

      const insertOptions = {
        query: insertQuery,
        params: {
          Key,
          Delivery_code,
          DelCode_w_o__,
          Step_ID,
          Task_Details,
          Frequency___Timeline,
          Client,
          Short_description,
          Planned_Start_Timestamp,
          Planned_Delivery_Timestamp,
          Responsibility,
          Current_Status,
          email,
          Total_Tasks,
          Completed_Tasks,
          Planned_Tasks,
          Percent_Tasks_Completed,
          Created_at,
          Updated_at,
          Time_Left_For_Next_Task_dd_hh_mm_ss,
          Percent_Delivery_Planned,
          Card_Corner_Status
        },
        types: {
          Key: 'INT64',
          Delivery_code: 'STRING',
          DelCode_w_o__: 'STRING',
          Step_ID: 'INT64',
          Task_Details: 'STRING',
          Frequency___Timeline: 'STRING',
          Client: 'STRING',
          Short_description: 'STRING',
          Planned_Start_Timestamp: 'TIMESTAMP',
          Planned_Delivery_Timestamp: 'TIMESTAMP',
          Responsibility: 'STRING',
          Current_Status: 'STRING',
          email:'STRING',
          Total_Tasks: 'INT64',
          Completed_Tasks: 'INT64',
          Planned_Tasks: 'INT64',
          Percent_Tasks_Completed: 'FLOAT64',
          Created_at: 'STRING',
          Updated_at: 'STRING',
          Time_Left_For_Next_Task_dd_hh_mm_ss: 'STRING',
          Percent_Delivery_Planned: 'FLOAT64',
          Card_Corner_Status: 'STRING',
        }
      };

      await bigQueryClient.createQueryJob(insertOptions);
    }

    // Handle slider values (insert into Per_Key_Per_Day)
    console.log('Received sliders data:', sliders);

    // Prepare the insert or update queries for sliders
    const insertOrUpdateSliderQueries = await Promise.all(sliders.map(async (slider) => {
      const selectQuery = {
        query: `SELECT duration FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` WHERE Key = @Key AND day = @day LIMIT 1`,
        params: {
          Key: Number(Key),
          day: slider.day,
        },
        types: {
          Key: 'INT64',
          day: 'STRING',
        },
      };

      // Check if the slider data already exists
      const [sliderRows] = await bigQueryClient.query(selectQuery);

      if (sliderRows.length > 0) {
        // Update existing slider record
        return {
          query: `UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` SET duration = @duration WHERE Key = @Key AND day = @day`,
          params: {
            Key: Number(Key),
            day: slider.day,
            duration: Number(slider.duration),
          },
          types: {
            Key: 'INT64',
            day: 'STRING',
            duration: 'INT64',
          },
        };
      } else {
        // Insert new slider record
        return {
          query: `INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable2}\` (Key, day, duration) VALUES (@Key, @day, @duration)`,
          params: {
            Key: Number(Key),
            day: slider.day,
            duration: Number(slider.duration),
          },
          types: {
            Key: 'INT64',
            day: 'STRING',
            duration: 'INT64',
          },
        };
      }
    }));

    // Execute each query (either an INSERT or UPDATE) based on the result
    await Promise.all(
      insertOrUpdateSliderQueries.map(async (queryOption) => {
        await bigQueryClient.createQueryJob(queryOption);
      })
    );

    res.status(200).send({ message: 'Task and slider data stored or updated successfully.' });
  } catch (error) {
    console.error('Error processing task and slider data:', error);
    res.status(500).send({ error: 'Failed to store or update task and slider data.' });
  }
});

// Update Task in BigQuery
app.put('/api/data/:key', async (req, res) => {
  const { key } = req.params;
  const { taskName, startDate, endDate, assignTo, status } = req.body;

  const query = `
    UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
    SET Task = @Task_Details, Start_Date = @Planned_Start_Timestamp, End_Date = @Planned_Delivery_Timestamp, Assign_To = @Responsibility, Status = @Current_Status, Client=@Client, Total_Tasks = @Total_Tasks, Planned_Tasks = @Planned_Tasks, Completed_Tasks =@Completed_Tasks, Created_at = @Created_at, Updated_at = @Updated_at
    WHERE Key = @key
  `;

  const options = {
    query: query,
    params: { key: parseInt(key), taskName, startDate, endDate, assignTo, status },
  };

  try {
    const [job] = await bigQueryClient.createQueryJob(options);
    await job.getQueryResults();
    res.status(200).send({ message: 'Task updated successfully.' });
  } catch (error) {
    console.error('Error updating task in BigQuery:', error);
    res.status(500).send({ error: 'Failed to update task in BigQuery.' });
  }
});

// Delete Task from BigQuery
app.delete('/api/data/:key', async (req, res) => {
  const { key } = req.params;

  const query = `
    DELETE FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
    WHERE Key = @key
  `;

  const options = {
    query: query,
    params: { key: parseInt(key) },
  };

  try {
    const [job] = await bigQueryClient.createQueryJob(options);
    await job.getQueryResults();
    res.status(200).send({ message: 'Task deleted successfully.' });
  } catch (error) {
    console.error('Error deleting task from BigQuery:', error);
    res.status(500).send({ error: 'Failed to delete task from BigQuery.' });
  }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const cors = require('cors');
const path = require('path');
const dotenv = require('dotenv');
dotenv.config();

const projectId = process.env.GOOGLE_PROJECT_ID;
const keyFilePath = path.join(__dirname, process.env.GOOGLE_KEY_FILE);
const bigQueryDataset = process.env.BIGQUERY_DATASET;
const bigQueryTable = process.env.BIGQUERY_TABLE;

// Initialize express app
const app = express();

console.log('Key file path:', keyFilePath);
console.log(bigQueryDataset);
console.log(bigQueryTable);

const bigQueryClient = new BigQuery({
  keyFilename: keyFilePath, // Use forward slashes
  projectId: projectId, // Your Google Cloud project ID
  scopes: ['https://www.googleapis.com/auth/drive'], // Add Google Drive scope
});

// Middleware setup
app.use(cors()); // Enable CORS
app.use(express.json()); // To handle JSON requests

// Route to get data from BigQuery
app.get('/api/data', async (req, res) => {
  try {
    const query = `SELECT * FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` LIMIT 1000`;
    const [rows] = await bigQueryClient.query(query);
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
    console.error('Error querying BigQuery:', err.message, err.stack); // Detailed error logging
    res.status(500).json({ message: err.message, stack: err.stack });
  }
});

// Assuming the correct data is passed from the frontend:
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
    Total_Tasks,
    Completed_Tasks,
    Planned_Tasks,
    Percent_Tasks_Completed,
    Created_at,
    Updated_at,
    Time_Left_For_Next_Task_dd_hh_mm_ss,
    Percent_Delivery_Planned,
    Card_Corner_Status
  } = req.body;

  // Query to check if the task already exists
  const checkQuery = `SELECT Key FROM \`${projectId}.${bigQueryDataset}.${bigQueryTable}\` WHERE Key = @Key`;
  const checkOptions = {
    query: checkQuery,
    params: { Key },
    types: { Key: 'INT64' }
  };

  try {
    const [existingTasks] = await bigQueryClient.query(checkOptions);

    if (existingTasks.length > 0) {
      // If task exists, update it
      const updateQuery = `
        UPDATE \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        SET 
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
          Total_Tasks = @Total_Tasks, 
          Completed_Tasks = @Completed_Tasks, 
          Planned_Tasks = @Planned_Tasks, 
          Percent_Tasks_Completed = @Percent_Tasks_Completed, 
          Created_at = @Created_at, 
          Updated_at = @Updated_at, 
          Time_Left_For_Next_Task_dd_hh_mm_ss = @Time_Left_For_Next_Task_dd_hh_mm_ss, 
          Percent_Delivery_Planned = @Percent_Delivery_Planned, 
          Card_Corner_Status = @Card_Corner_Status
        WHERE Key = @Key
      `;

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

      const [updateJob] = await bigQueryClient.createQueryJob(updateOptions);
      await updateJob.getQueryResults();

      res.status(200).send({ message: 'Task updated successfully.' });
    } else {
      // If task doesn't exist, insert it
      const insertQuery = `
        INSERT INTO \`${projectId}.${bigQueryDataset}.${bigQueryTable}\`
        (Key, Delivery_code, DelCode_w_o__, Step_ID, Task_Details, Frequency___Timeline, Client, Short_description, 
        Planned_Start_Timestamp, Planned_Delivery_Timestamp, Responsibility, Current_Status, Total_Tasks, 
        Completed_Tasks, Planned_Tasks, Percent_Tasks_Completed, Created_at, Updated_at, 
        Time_Left_For_Next_Task_dd_hh_mm_ss, Percent_Delivery_Planned, Card_Corner_Status)
        VALUES 
        (@Key, @Delivery_code, @DelCode_w_o__, @Step_ID, @Task_Details, @Frequency___Timeline, @Client, 
        @Short_description, @Planned_Start_Timestamp, @Planned_Delivery_Timestamp, @Responsibility, 
        @Current_Status, @Total_Tasks, @Completed_Tasks, @Planned_Tasks, @Percent_Tasks_Completed, 
        @Created_at, @Updated_at, @Time_Left_For_Next_Task_dd_hh_mm_ss, @Percent_Delivery_Planned, 
        @Card_Corner_Status)
      `;

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

      const [insertJob] = await bigQueryClient.createQueryJob(insertOptions);
      await insertJob.getQueryResults();

      res.status(200).send({ message: 'Task inserted successfully.' });
    }
  } catch (error) {
    console.error('Error processing task:', error);
    res.status(500).send({ error: 'Failed to process task.' });
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

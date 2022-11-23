using Google.Apis.Auth.OAuth2;
using System.IO;
using System.Threading;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using System.Data;
using Google.Apis.Services;
using System;
using Google.Cloud.BigQuery.V2;
using System.Configuration;
using System.Data.SqlClient;
using ConsoleAppBQTest;


Console.WriteLine("Hello, World!");

try
{
    string _initialCatalog = ConfigurationManager.AppSettings["database"];
    string _dataSource = ConfigurationManager.AppSettings["databaseserver"];
    string _userID = ConfigurationManager.AppSettings["userid"];
    string _password = ConfigurationManager.AppSettings["password"];

    SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

    builder.DataSource = _dataSource;
    builder.UserID = _userID;
    builder.Password = _password;
    builder.InitialCatalog = _initialCatalog;


    string projectId = "get-into-teaching";
    //string datasetId = "transactions";


    var credentials = GoogleCredential.FromFile("C:\\ETL\\BIGQUERY\\get-into-teaching-0e0c87a8ac3e.json");

    BigQueryClient client = BigQueryClient.Create(projectId, credentials);

    Console.WriteLine($"StartTime:{DateTime.Now.ToString()}");

    //BigQueryTable table = client.GetTable(projectId, "school_experience", "school_experience_requests");
    //string sql = $"SELECT * from school_experience.school_experience_requests";
    //BigQueryParameter[] parameters = null;
    //BigQueryResults results = client.ExecuteQuery(sql, parameters);
    //foreach (BigQueryRow row in results)
    //{
     //   Console.WriteLine($"{results.Count()}");
    //}
    //return;
    var dataSet = DataLogic.GetSchoolExperienceRecordsFromCRM(builder.ConnectionString);
    if (dataSet != null && dataSet.Tables.Count > 0)
    {
        int errorCount = 0;
        foreach (DataRow row in dataSet.Tables[0].Rows)
        {
            if (row is not null)
            {
                try
                {
                    var insert = new BigQueryInsertRow();

                    if (row.ItemArray[0] != null && row.ItemArray[0] != System.DBNull.Value)
                    {
                        insert.Add("contact_id", row.ItemArray[0].ToString());
                    }
                    if (row.ItemArray[1] != null && row.ItemArray[1] != System.DBNull.Value)
                    {
                        insert.Add("urn", row.ItemArray[1]);
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("subject", row.ItemArray[2]);
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("placement_date", Convert.ToDateTime(row.ItemArray[3]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("duration", row.ItemArray[4]);
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("status", row.ItemArray[5]);
                    }
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        //insert.Add("event_occurred_at", Convert.ToDateTime(row.ItemArray[6]).ToString("yyyy-MM-dd HH:mm:ss"));
                        insert.Add("event_occurred_at", Convert.ToDateTime(row.ItemArray[6]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[7] != null && row.ItemArray[7] != System.DBNull.Value)
                    {
                        insert.Add("event_occurred_on", Convert.ToDateTime(row.ItemArray[7]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[8] != null && row.ItemArray[8] != System.DBNull.Value)
                    {
                        insert.Add("id", row.ItemArray[8].ToString());
                    }

                    client.InsertRow("school_experience", "school_experience_requests", insert);
                }
                catch (Exception ex)
                {

                    errorCount++;
                    Console.WriteLine($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()}");
                }
            }

        }

    }  

    Console.WriteLine($"EndTime:{DateTime.Now.ToString()}");
}
catch (Exception ex)
{
    throw;
}


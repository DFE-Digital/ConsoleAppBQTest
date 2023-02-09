using Google.Apis.Auth.OAuth2;
using System.Data;
using Google.Cloud.BigQuery.V2;
using System.Configuration;
using System.Data.SqlClient;
using ConsoleAppBQTest;
using System.Diagnostics;



try
{
    string _initialCatalog = ConfigurationManager.AppSettings["database"];
    string _dataSource = ConfigurationManager.AppSettings["databaseserver"];
    string _userID = ConfigurationManager.AppSettings["userid"];
    string _password = ConfigurationManager.AppSettings["password"];
    string projectId = ConfigurationManager.AppSettings["projectid"];
    string datasetId = ConfigurationManager.AppSettings["datasetid"];
    string seDatasetId = ConfigurationManager.AppSettings["sedatasetid"];

    SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

    builder.DataSource = _dataSource;
    builder.UserID = _userID;
    builder.Password = _password;
    builder.InitialCatalog = _initialCatalog;


    var credentials = GoogleCredential.FromFile("C:\\ETL\\BIGQUERY\\get-into-teaching-0e0c87a8ac3e.json");
    BigQueryClient client = BigQueryClient.Create(projectId, credentials);


    DeleteDataFromBQTables(client);
    Thread.Sleep(120000);

    InsertEventRecordsToBQInBatches(builder, client, datasetId);
    InsertEventRegistrationRecordsToBQInBatches(builder, client, datasetId);
    InsertMLSubscriptionRecordsToBQInBatches(builder, client, datasetId);
    InsertTTASignupRecordsToBQInBatches(builder, client, datasetId);
    InsertApplicationRecordsToBQInBatches(builder, client, datasetId);
    InsertProfileRecordsToBQInBatches(builder, client, datasetId);
    InsertSchoolExperienceRecordsToBQInBatches(builder, client, seDatasetId);
}
catch (Exception ex)
{
    LogErrorMessage(ex.ToString());    
}


void DeleteDataFromBQTables(BigQueryClient client)
{    
    BigQueryParameter[] parameters = null;

    string sql = @"TRUNCATE TABLE `get-into-teaching.transactions.events`";    
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Event Records deleted - End Time:{DateTime.Now.ToString()}");

    sql = @"TRUNCATE TABLE `get-into-teaching.transactions.event_registrations`";
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Event Registration Records deleted - End Time:{DateTime.Now.ToString()}");

    sql = @"TRUNCATE TABLE `get-into-teaching.transactions.mailing_list_subscriptions`";
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Mailing List Subscription Records deleted - End Time:{DateTime.Now.ToString()}");

    sql = @"TRUNCATE TABLE `get-into-teaching.transactions.teacher_training_adviser_signups`";
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Teacher Training Adviser Signup Records deleted - End Time:{DateTime.Now.ToString()}");

    sql = @"TRUNCATE TABLE `get-into-teaching.transactions.applications`";
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Applications Records deleted - End Time:{DateTime.Now.ToString()}");

    sql = @"TRUNCATE TABLE `get-into-teaching.transactions.profile`";
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Profile Records deleted - End Time:{DateTime.Now.ToString()}");

    sql = @"TRUNCATE TABLE `get-into-teaching.school_experience.school_experience_requests`";
    client.ExecuteQuery(sql, parameters);
    LogMessage($"School Experience Requests Records deleted - End Time:{DateTime.Now.ToString()}");    
}

//Batches code

static void InsertEventRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string datasetId)
{
    LogMessage($"Event Records - StartTime:{DateTime.Now.ToString()}");
    var dataSet = DataLogic.GetEventRecordsFromCRM(builder.ConnectionString);
    if (dataSet != null && dataSet.Tables.Count > 0)
    {
        int errorCount = 0;
        int counter = 0;
        int totalRecordsCount = dataSet.Tables[0].Rows.Count;
        List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();
        foreach (DataRow row in dataSet.Tables[0].Rows)
        {
            if (row is not null)
            {
                try
                {
                    //string sQuery = $"SELECT [id],[venue],[starts_at],[finishes_at],[date],[name],[status],[partial_url],[event_type],[online],[virtual] FROM [git].[events]";
                    var insert = new BigQueryInsertRow();
                    if (row.ItemArray[0] != null && row.ItemArray[0] != System.DBNull.Value)
                    {
                        insert.Add("id", row.ItemArray[0].ToString());
                    }
                    if (row.ItemArray[1] != null && row.ItemArray[1] != System.DBNull.Value)
                    {
                        insert.Add("venue", row.ItemArray[1].ToString());
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("starts_at", Convert.ToDateTime(row.ItemArray[2]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("finishes_at", Convert.ToDateTime(row.ItemArray[3]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("date", Convert.ToDateTime(row.ItemArray[4]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("name", row.ItemArray[5].ToString());
                    }
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        insert.Add("status", row.ItemArray[6].ToString());
                    }
                    if (row.ItemArray[7] != null && row.ItemArray[7] != System.DBNull.Value)
                    {
                        insert.Add("partial_url", row.ItemArray[7].ToString());
                    }
                    if (row.ItemArray[8] != null && row.ItemArray[8] != System.DBNull.Value)
                    {
                        insert.Add("event_type", row.ItemArray[8].ToString());
                    }
                    if (row.ItemArray[9] != null && row.ItemArray[9] != System.DBNull.Value)
                    {
                        insert.Add("online", Convert.ToBoolean(row.ItemArray[9]));
                    }
                    if (row.ItemArray[10] != null && row.ItemArray[10] != System.DBNull.Value)
                    {
                        insert.Add("virtual", Convert.ToBoolean(row.ItemArray[10]));
                    }

                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        client.InsertRows(datasetId, "events", rows.ToArray());
                        rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()}");
                }
            }
        }
        LogMessage($"Event Records - Total Records Processed:{counter}");
    }
    LogMessage($"Event Records - EndTime:{DateTime.Now.ToString()}");
}

static void InsertEventRegistrationRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string datasetId)
{
    LogMessage($"Event Registration Records - StartTime:{DateTime.Now.ToString()}");
    var dataSet = DataLogic.GetEventRegistrationRecordsFromCRM(builder.ConnectionString);
    if (dataSet != null && dataSet.Tables.Count > 0)
    {
        int errorCount = 0;
        int counter = 0;
        int totalRecordsCount = dataSet.Tables[0].Rows.Count;
        List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();
        foreach (DataRow row in dataSet.Tables[0].Rows)
        {
            if (row is not null)
            {
                try
                {
                    var insert = new BigQueryInsertRow();
                    if (row.ItemArray[0] != null && row.ItemArray[0] != System.DBNull.Value)
                    {
                        insert.Add("id", row.ItemArray[0].ToString());
                    }
                    if (row.ItemArray[1] != null && row.ItemArray[1] != System.DBNull.Value)
                    {
                        insert.Add("event_id", row.ItemArray[1].ToString());
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("contact_id", row.ItemArray[2].ToString());
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("attended", row.ItemArray[3].ToString());
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("registered_at", Convert.ToDateTime(row.ItemArray[4]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("registered_on", Convert.ToDateTime(row.ItemArray[5]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        insert.Add("attendance_confirmed_at", Convert.ToDateTime(row.ItemArray[6]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[7] != null && row.ItemArray[7] != System.DBNull.Value)
                    {
                        insert.Add("attendance_confirmed_on", Convert.ToDateTime(row.ItemArray[7]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[8] != null && row.ItemArray[8] != System.DBNull.Value)
                    {
                        insert.Add("creation_channel", row.ItemArray[8].ToString());
                    }

                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        client.InsertRows(datasetId, "event_registrations", rows.ToArray());
                        rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()}");
                }
            }
        }
        LogMessage($"Event Registration  Records - Total Records Processed:{counter}");
    }
    LogMessage($"Event Registration Records - EndTime:{DateTime.Now.ToString()}");
}

static void InsertMLSubscriptionRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string datasetId)
{
    LogMessage($"ML Subscription Records - StartTime:{DateTime.Now.ToString()}");
    var dataSet = DataLogic.GetMLSubscriptionRecordsFromCRM(builder.ConnectionString);
    if (dataSet != null && dataSet.Tables.Count > 0)
    {
        int errorCount = 0;
        int counter = 0;
        int totalRecordsCount = dataSet.Tables[0].Rows.Count;
        List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();
        foreach (DataRow row in dataSet.Tables[0].Rows)
        {
            if (row is not null)
            {
                try
                {
                    //SELECT [contact_id],[subscribed_at],[subscribed_on],[subscription_channel],[still_subscribed],[opted_out_of_all_emails],[opted_out_of_bulk_emails],[opted_out_of_post] FROM [git].[mailing_list_subscriptions]
                    var insert = new BigQueryInsertRow();
                    if (row.ItemArray[0] != null && row.ItemArray[0] != System.DBNull.Value)
                    {
                        insert.Add("contact_id", row.ItemArray[0].ToString());
                    }
                    if (row.ItemArray[1] != null && row.ItemArray[1] != System.DBNull.Value)
                    {
                        insert.Add("subscribed_at", Convert.ToDateTime(row.ItemArray[1]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("subscribed_on", Convert.ToDateTime(row.ItemArray[2]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("subscription_channel", row.ItemArray[3].ToString());
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("still_subscribed", Convert.ToBoolean(row.ItemArray[4]));
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("opted_out_of_all_emails", Convert.ToBoolean(row.ItemArray[5]));
                    }
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        insert.Add("opted_out_of_bulk_emails", Convert.ToBoolean(row.ItemArray[6]));
                    }
                    if (row.ItemArray[7] != null && row.ItemArray[7] != System.DBNull.Value)
                    {
                        insert.Add("opted_out_of_post", Convert.ToBoolean(row.ItemArray[7]));
                    }

                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        client.InsertRows(datasetId, "mailing_list_subscriptions", rows.ToArray());
                        rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    LogMessage($"Error:{errorCount} - Contact Id: {row.ItemArray[0].ToString()}");
                }
            }
        }
        LogMessage($"ML Subscription Records - Total Records Processed:{counter}");
    }
    LogMessage($"ML Subscription Records - EndTime:{DateTime.Now.ToString()}");
}

static void InsertTTASignupRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string datasetId)
{
    LogMessage($"TTA Signup Records - StartTime:{DateTime.Now.ToString()}");
    var dataSet = DataLogic.GetTTASignupRecordsFromCRM(builder.ConnectionString);
    if (dataSet != null && dataSet.Tables.Count > 0)
    {
        int errorCount = 0;
        int counter = 0;
        int totalRecordsCount = dataSet.Tables[0].Rows.Count;
        List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();
        foreach (DataRow row in dataSet.Tables[0].Rows)
        {
            if (row is not null)
            {
                try
                {
                    //SELECT [contact_id],[signed_up_at],[signed_up_on],[subscription_channel],[opted_out_of_all_emails],[opted_out_of_bulk_emails],[opted_out_of_post] FROM [git].[teacher_training_adviser_signups]
                    var insert = new BigQueryInsertRow();
                    if (row.ItemArray[0] != null && row.ItemArray[0] != System.DBNull.Value)
                    {
                        insert.Add("contact_id", row.ItemArray[0].ToString());
                    }
                    if (row.ItemArray[1] != null && row.ItemArray[1] != System.DBNull.Value)
                    {
                        insert.Add("signed_up_at", Convert.ToDateTime(row.ItemArray[1]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("signed_up_on", Convert.ToDateTime(row.ItemArray[2]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("subscription_channel", row.ItemArray[3].ToString());
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("opted_out_of_all_emails", Convert.ToBoolean(row.ItemArray[4]));
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("opted_out_of_bulk_emails", Convert.ToBoolean(row.ItemArray[5]));
                    }
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        insert.Add("opted_out_of_post", Convert.ToBoolean(row.ItemArray[6]));
                    }                    

                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        client.InsertRows(datasetId, "teacher_training_adviser_signups", rows.ToArray());
                        rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    LogMessage($"Error:{errorCount} - Contact Id: {row.ItemArray[0].ToString()}");
                }
            }
        }
        LogMessage($"TTA Signup Records - Total Records Processed:{counter}");
    }
    LogMessage($"TTA Signup Records - EndTime:{DateTime.Now.ToString()}");
}

static void InsertApplicationRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string datasetId)
{
    LogMessage($"Application Records - StartTime:{DateTime.Now.ToString()}");
    var dataSet = DataLogic.GetApplicationRecordsFromCRM(builder.ConnectionString);
    if (dataSet != null && dataSet.Tables.Count > 0)
    {
        int errorCount = 0;
        int counter = 0;
        int totalRecordsCount = dataSet.Tables[0].Rows.Count;
        List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();
        foreach (DataRow row in dataSet.Tables[0].Rows)
        {
            if (row is not null)
            {
                try
                {
                    //SELECT [contact_id],[applied_at],[applied_on],[phase],[status],[id],[recruitment_year],[application_complete],[success],[application_form_id] FROM [git].[applications]
                    var insert = new BigQueryInsertRow();
                    if (row.ItemArray[0] != null && row.ItemArray[0] != System.DBNull.Value)
                    {
                        insert.Add("contact_id", row.ItemArray[0].ToString());
                    }
                    if (row.ItemArray[1] != null && row.ItemArray[1] != System.DBNull.Value)
                    {
                        insert.Add("applied_at", Convert.ToDateTime(row.ItemArray[1]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("applied_on", Convert.ToDateTime(row.ItemArray[2]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("phase", row.ItemArray[3].ToString());
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("status", row.ItemArray[4].ToString());
                    }
                    //if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    //{
                    //    insert.Add("id", row.ItemArray[5].ToString());
                    //}
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        insert.Add("recruitment_year", Convert.ToInt64(row.ItemArray[6]));
                    }
                    if (row.ItemArray[7] != null && row.ItemArray[7] != System.DBNull.Value)
                    {
                        insert.Add("application_complete", Convert.ToBoolean(row.ItemArray[7]));
                    }
                    if (row.ItemArray[8] != null && row.ItemArray[8] != System.DBNull.Value)
                    {
                        insert.Add("success", Convert.ToBoolean(row.ItemArray[8]));
                    }
                    if (row.ItemArray[9] != null && row.ItemArray[9] != System.DBNull.Value)
                    {
                        insert.Add("application_form_id", Convert.ToInt64(row.ItemArray[9]));
                    }
                    if (row.ItemArray[10] != null && row.ItemArray[10] != System.DBNull.Value)
                    {
                        insert.Add("id", row.ItemArray[10].ToString());
                    }

                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        client.InsertRows(datasetId, "applications", rows.ToArray());
                        rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[5].ToString()}");
                }
            }
        }
        LogMessage($"Application Records - Total Records Processed:{counter}");
    }
    LogMessage($"Application Records - EndTime:{DateTime.Now.ToString()}");
}

static void InsertProfileRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string dataSetId)
{
    LogMessage($"Profile Records - StartTime:{DateTime.Now.ToString()}");
    var dataSet = DataLogic.GetProfileRecordsFromCRM(builder.ConnectionString);
    if (dataSet != null && dataSet.Tables.Count > 0)
    {
        int errorCount = 0;
        int counter = 0;
        int totalRecordsCount = dataSet.Tables[0].Rows.Count;
        List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();
        foreach (DataRow row in dataSet.Tables[0].Rows)
        {
            if (row is not null)
            {
                try
                {
                    var insert = new BigQueryInsertRow();
                    if (row.ItemArray[0] != null && row.ItemArray[0] != System.DBNull.Value)
                    {
                        insert.Add("id", row.ItemArray[0].ToString());
                    }

                    if (row.ItemArray[1] != null && row.ItemArray[1] != System.DBNull.Value)
                    {
                        insert.Add("journey_stage", row.ItemArray[1].ToString());
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("degree_status", row.ItemArray[2].ToString());
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("chosen_subject", row.ItemArray[3].ToString());
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("fallback_subject", row.ItemArray[4].ToString());
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("has_postcode", Convert.ToBoolean(row.ItemArray[5]));
                    }
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        insert.Add("has_date_of_birth", Convert.ToBoolean(row.ItemArray[6]));
                    }
                    if (row.ItemArray[7] != null && row.ItemArray[7] != System.DBNull.Value)
                    {
                        insert.Add("adviser_assigned_at", Convert.ToDateTime(row.ItemArray[7]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[8] != null && row.ItemArray[8] != System.DBNull.Value)
                    {
                        insert.Add("adviser_assigned_on", Convert.ToDateTime(row.ItemArray[8]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[9] != null && row.ItemArray[9] != System.DBNull.Value)
                    {
                        insert.Add("has_adviser", Convert.ToBoolean(row.ItemArray[9]));
                    }
                    if (row.ItemArray[10] != null && row.ItemArray[10] != System.DBNull.Value)
                    {
                        insert.Add("international", Convert.ToBoolean(row.ItemArray[10]));
                    }
                    if (row.ItemArray[11] != null && row.ItemArray[11] != System.DBNull.Value)
                    {
                        insert.Add("returner", Convert.ToBoolean(row.ItemArray[11]));
                    }
                    if (row.ItemArray[12] != null && row.ItemArray[12] != System.DBNull.Value)
                    {
                        insert.Add("country", row.ItemArray[12].ToString());
                    }
                    if (row.ItemArray[13] != null && row.ItemArray[13] != System.DBNull.Value)
                    {
                        insert.Add("creation_channel", row.ItemArray[13].ToString());
                    }
                    if (row.ItemArray[14] != null && row.ItemArray[14] != System.DBNull.Value)
                    {
                        insert.Add("created_via_git_bat_sync", Convert.ToBoolean(row.ItemArray[14]));
                    }
                    if (row.ItemArray[15] != null && row.ItemArray[15] != System.DBNull.Value)
                    {
                        insert.Add("duplicate", Convert.ToBoolean(row.ItemArray[15]));
                    }
                    if (row.ItemArray[16] != null && row.ItemArray[16] != System.DBNull.Value)
                    {
                        insert.Add("created_at", Convert.ToDateTime(row.ItemArray[16]).ToString("yyyy-MM-dd HH:MM:ss"));
                    }
                    if (row.ItemArray[17] != null && row.ItemArray[17] != System.DBNull.Value)
                    {
                        insert.Add("created_on", Convert.ToDateTime(row.ItemArray[17]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[18] != null && row.ItemArray[18] != System.DBNull.Value)
                    {
                        insert.Add("recruitment_stage", row.ItemArray[18].ToString());
                    }

                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        var results = client.InsertRows(dataSetId, "profile", rows.ToArray());                      
                        rows.Clear();                        
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()}");
                }
            }

        }
        LogMessage($"Profile Records - Total Records Processed:{counter}");
    }
    LogMessage($"Profile Records - EndTime:{DateTime.Now.ToString()}");
}

static void InsertSchoolExperienceRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string datasetId)
{
    LogMessage($"School Experience Records - StartTime:{DateTime.Now.ToString()}");
    var dataSet = DataLogic.GetSchoolExperienceRecordsFromCRM(builder.ConnectionString);
    if (dataSet != null && dataSet.Tables.Count > 0)
    {
        int errorCount = 0;
        int counter = 0;
        int totalRecordsCount = dataSet.Tables[0].Rows.Count;
        List<BigQueryInsertRow> rows = new List<BigQueryInsertRow>();
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
                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        client.InsertRows(datasetId, "school_experience_requests", rows.ToArray());
                        rows.Clear();
                    }                    
                }
                catch (Exception ex)
                {
                    errorCount++;
                    LogMessage($"Error:{errorCount} - Contact Id: {row.ItemArray[0].ToString()} - Id: {row.ItemArray[8].ToString()}");
                }
            }

        }
        LogMessage($"School Experience Records - Total Records Processed:{counter}");

    }
    LogMessage($"School Experience Records - EndTime:{DateTime.Now.ToString()}");
}


static void LogMessage(string message)
{
    Console.WriteLine(message);
    Trace.TraceInformation(message);
    Trace.Flush();
}

void LogErrorMessage(string message)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine(message);
    Console.ForegroundColor = ConsoleColor.White;
}






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
    string seTableName = ConfigurationManager.AppSettings["setablename"];

    SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

    builder.DataSource = _dataSource;
    builder.UserID = _userID;
    builder.Password = _password;
    builder.InitialCatalog = _initialCatalog;


    var credentials = GoogleCredential.FromFile("C:\\ETL\\BIGQUERY\\get-into-teaching-0e0c87a8ac3e.json");
    BigQueryClient client = BigQueryClient.Create(projectId, credentials);


    DeleteDataFromBQTables(client, datasetId, seDatasetId, seTableName);
    Thread.Sleep(120000);

    InsertEventRecordsToBQInBatches(builder, client, datasetId);
    InsertEventRegistrationRecordsToBQInBatches(builder, client, datasetId);
    InsertMLSubscriptionRecordsToBQInBatches(builder, client, datasetId);
    InsertTTASignupRecordsToBQInBatches(builder, client, datasetId);
    InsertApplicationRecordsToBQInBatches(builder, client, datasetId);
    InsertProfileRecordsToBQInBatches(builder, client, datasetId);
    InsertSchoolExperienceRecordsToBQInBatches(builder, client, seDatasetId, seTableName);
    InsertCandidateWorkexpErienceRecordsToBQInBatches(builder, client, datasetId);
    InsertDegreeQualificationRecordsToBQInBatches(builder, client, datasetId);
    InsertTTAStagesRecordsToBQInBatches(builder, client, datasetId);

}
catch (Exception ex)
{
    LogErrorMessage(ex.ToString());    
}

void DeleteDataFromBQTables(BigQueryClient client, string dataSetId, string seDataSetId, string seTableName)
{    
    BigQueryParameter[] parameters = null;
    
    //string sql = @"TRUNCATE TABLE `get-into-teaching.transactions.events`";
    string sql = $"TRUNCATE TABLE `get-into-teaching.{dataSetId}.events`";
    LogMessage($"Event Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Event Records deleted - End Time:{DateTime.Now.ToString()}");

    //sql = @"TRUNCATE TABLE `get-into-teaching.transactions.event_registrations`";
    sql = $"TRUNCATE TABLE `get-into-teaching.{dataSetId}.event_registrations`";
    LogMessage($"Event Registration Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Event Registration Records deleted - End Time:{DateTime.Now.ToString()}");

    //sql = @"TRUNCATE TABLE `get-into-teaching.transactions.mailing_list_subscriptions`";
    sql = $"TRUNCATE TABLE `get-into-teaching.{dataSetId}.mailing_list_subscriptions`";
    LogMessage($"Mailing List Subscription Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Mailing List Subscription Records deleted - End Time:{DateTime.Now.ToString()}");

    //sql = @"TRUNCATE TABLE `get-into-teaching.transactions.teacher_training_adviser_signups`";
    sql = $"TRUNCATE TABLE `get-into-teaching.{dataSetId}.teacher_training_adviser_signups`";
    LogMessage($"Teacher Training Adviser Signup Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Teacher Training Adviser Signup Records deleted - End Time:{DateTime.Now.ToString()}");

    //sql = @"TRUNCATE TABLE `get-into-teaching.transactions.applications`";
    sql = $"TRUNCATE TABLE `get-into-teaching.{dataSetId}.applications`";
    LogMessage($"Applications Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Applications Records deleted - End Time:{DateTime.Now.ToString()}");

    //sql = @"TRUNCATE TABLE `get-into-teaching.transactions.profile`";
    sql = $"TRUNCATE TABLE `get-into-teaching.{dataSetId}.profile`";
    LogMessage($"Profile Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Profile Records deleted - End Time:{DateTime.Now.ToString()}");

    //sql = @"TRUNCATE TABLE `get-into-teaching.school_experience.school_experience_requests`";
    sql = $"TRUNCATE TABLE `get-into-teaching.{seDataSetId}.{seTableName}`";
    LogMessage($"School Experience Requests Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"School Experience Requests Records deleted - End Time:{DateTime.Now.ToString()}");

    //sql = @"TRUNCATE TABLE `get-into-teaching.transactions.work_experience`";
    sql = $"TRUNCATE TABLE `get-into-teaching.{dataSetId}.work_experience`";
    LogMessage($"work_experience Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Candidate Work experience Records deleted - End Time:{DateTime.Now.ToString()}");    

    //sql = @"TRUNCATE TABLE `get-into-teaching.transactions.degree_qualifications`";
    sql = $"TRUNCATE TABLE `get-into-teaching.{dataSetId}.degree_qualifications`";
    LogMessage($"Degree Qualification Requests Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"Degree Qualification Requests Records deleted - End Time:{DateTime.Now.ToString()}");

    //sql = @"TRUNCATE TABLE `get-into-teaching.transactions.tta_stages`";
    sql = $"TRUNCATE TABLE `get-into-teaching.{dataSetId}.tta_stages`";
    LogMessage($"TTA Stages Records delete - SQL:{sql}");
    client.ExecuteQuery(sql, parameters);
    LogMessage($"TTA Stages Records deleted - End Time:{DateTime.Now.ToString()}");
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
                        insert.Add("starts_at", Convert.ToDateTime(row.ItemArray[2]).ToString("yyyy-MM-dd HH:mm:ss"));
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("finishes_at", Convert.ToDateTime(row.ItemArray[3]).ToString("yyyy-MM-dd HH:mm:ss"));
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
                  //  LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()}");
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()} - Error Message:{ex.Message}");
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
                        insert.Add("registered_at", Convert.ToDateTime(row.ItemArray[4]).ToString("yyyy-MM-dd HH:mm:ss"));
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("registered_on", Convert.ToDateTime(row.ItemArray[5]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        insert.Add("attendance_confirmed_at", Convert.ToDateTime(row.ItemArray[6]).ToString("yyyy-MM-dd HH:mm:ss"));
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
                    // LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()}");
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()} - Error Message:{ex.Message}");
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
                        insert.Add("subscribed_at", Convert.ToDateTime(row.ItemArray[1]).ToString("yyyy-MM-dd HH:mm:ss"));
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
                    if (row.ItemArray[8] != null && row.ItemArray[8] != System.DBNull.Value)
                    {
                        insert.Add("unsubscribe_reason", row.ItemArray[8].ToString());
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
                   // LogMessage($"Error:{errorCount} - Contact Id: {row.ItemArray[0].ToString()}");
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()} - Error Message:{ex.Message}");
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
                        insert.Add("signed_up_at", Convert.ToDateTime(row.ItemArray[1]).ToString("yyyy-MM-dd HH:mm:ss"));
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
                  //  LogMessage($"Error:{errorCount} - Contact Id: {row.ItemArray[0].ToString()}");
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()} - Error Message:{ex.Message}");
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
                        insert.Add("applied_at", Convert.ToDateTime(row.ItemArray[1]).ToString("yyyy-MM-dd HH:mm:ss"));
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
                   // LogMessage($"Error:{errorCount} - Id: {row.ItemArray[5].ToString()}");
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()} - Error Message:{ex.Message}");
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
                        insert.Add("adviser_assigned_at", Convert.ToDateTime(row.ItemArray[7]).ToString("yyyy-MM-dd HH:mm:ss"));
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
                        insert.Add("created_at", Convert.ToDateTime(row.ItemArray[16]).ToString("yyyy-MM-dd HH:mm:ss"));
                    }
                    if (row.ItemArray[17] != null && row.ItemArray[17] != System.DBNull.Value)
                    {
                        insert.Add("created_on", Convert.ToDateTime(row.ItemArray[17]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[18] != null && row.ItemArray[18] != System.DBNull.Value)
                    {
                        insert.Add("recruitment_stage", row.ItemArray[18].ToString());
                    }
                    if (row.ItemArray[19] != null && row.ItemArray[19] != System.DBNull.Value)
                    {
                        insert.Add("date_of_birth", Convert.ToDateTime(row.ItemArray[19]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[20] != null && row.ItemArray[20] != System.DBNull.Value)
                    {
                        insert.Add("itt_start_year", row.ItemArray[20].ToString());
                    }
                    if (row.ItemArray[21] != null && row.ItemArray[21] != System.DBNull.Value)
                    {
                        insert.Add("has_qts", row.ItemArray[21].ToString());
                    }
                    if (row.ItemArray[22] != null && row.ItemArray[22] != System.DBNull.Value)
                    {
                        insert.Add("preferred_region_1", row.ItemArray[22].ToString());
                    }
                    if (row.ItemArray[23] != null && row.ItemArray[23] != System.DBNull.Value)
                    {
                        insert.Add("preferred_region_2", row.ItemArray[23].ToString());
                    }
                    if (row.ItemArray[24] != null && row.ItemArray[24] != System.DBNull.Value)
                    {
                        insert.Add("consideration_stage", row.ItemArray[24].ToString());
                    }
                    //start new fields 

                    if (row.ItemArray[25] != null && row.ItemArray[25] != System.DBNull.Value)
                    {
                        insert.Add("candidate_type", row.ItemArray[25].ToString());
                    }
                    //TODO this is not bool
                    if (row.ItemArray[26] != null && row.ItemArray[26] != System.DBNull.Value)
                    {
                        insert.Add("is_candidate_eligible_for_an_adviser", row.ItemArray[26].ToString());
                    }
                    if (row.ItemArray[27] != null && row.ItemArray[27] != System.DBNull.Value)
                    {
                        insert.Add("adviser_status", row.ItemArray[27].ToString());
                    }

                    if (row.ItemArray[28] != null && row.ItemArray[28] != System.DBNull.Value)
                    {
                        insert.Add("adviser_status_reason", row.ItemArray[28].ToString());
                    }
                    if (row.ItemArray[29] != null && row.ItemArray[29] != System.DBNull.Value)
                    {
                        insert.Add("re_register_status", row.ItemArray[29].ToString());
                    }
                    if (row.ItemArray[30] != null && row.ItemArray[30] != System.DBNull.Value)
                    {
                        insert.Add("current_adviser", row.ItemArray[30].ToString());
                    }                   

                    if (row.ItemArray[31] != null && row.ItemArray[31] != System.DBNull.Value)
                    {
                        insert.Add("current_adviser_team", row.ItemArray[31].ToString());
                    }

                    if (row.ItemArray[32] != null && row.ItemArray[32] != System.DBNull.Value)
                    {
                        insert.Add("previous_adviser", row.ItemArray[32].ToString());
                    }
                    if (row.ItemArray[33] != null && row.ItemArray[33] != System.DBNull.Value)
                    {
                        insert.Add("date_assigned_to_previous_adviser", Convert.ToDateTime(row.ItemArray[33]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[34] != null && row.ItemArray[34] != System.DBNull.Value)
                    {
                        insert.Add("date_released", Convert.ToDateTime(row.ItemArray[34]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[35] != null && row.ItemArray[35] != System.DBNull.Value)
                    {
                        insert.Add("postcode", row.ItemArray[35].ToString());
                    }
                    if (row.ItemArray[36] != null && row.ItemArray[36] != System.DBNull.Value)
                    {
                        insert.Add("right_to_study_in_the_uk", row.ItemArray[36].ToString());
                    }
                    if (row.ItemArray[37] != null && row.ItemArray[37] != System.DBNull.Value)
                    {
                        insert.Add("preferred_training_region", row.ItemArray[37].ToString());
                    }
                    if (row.ItemArray[38] != null && row.ItemArray[38] != System.DBNull.Value)
                    {
                        insert.Add("preferred_teaching_course", row.ItemArray[38].ToString());
                    }
                    if (row.ItemArray[39] != null && row.ItemArray[39] != System.DBNull.Value)
                    {
                        insert.Add("preferred_education_phase", row.ItemArray[39].ToString());
                    }
                   
                    if (row.ItemArray[40] != null && row.ItemArray[40] != System.DBNull.Value)
                    {
                        insert.Add("has_GCSE_english", row.ItemArray[40].ToString());
                    }
                   
                    if (row.ItemArray[41] != null && row.ItemArray[41] != System.DBNull.Value)
                    {
                        insert.Add("has_GCSE_maths", row.ItemArray[41].ToString());
                    }
                   
                    if (row.ItemArray[42] != null && row.ItemArray[42] != System.DBNull.Value)
                    {
                        insert.Add("has_GCSE_science", row.ItemArray[42].ToString());
                    }
                    if (row.ItemArray[43] != null && row.ItemArray[43] != System.DBNull.Value)
                    {
                        insert.Add("has_dbs_certificate", Convert.ToBoolean(row.ItemArray[43]));
                    }
                    if (row.ItemArray[44] != null && row.ItemArray[44] != System.DBNull.Value)
                    {
                        insert.Add("issue_date_of_DBS_certificate", Convert.ToDateTime(row.ItemArray[44]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[45] != null && row.ItemArray[45] != System.DBNull.Value)
                    {
                        insert.Add("days_since_triaged", row.ItemArray[45]);
                    }

                    // new fields end
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
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()} - Error Message:{ex.Message}");
                }
            }

        }
        LogMessage($"Profile Records - Total Records Processed:{counter}");
    }
    LogMessage($"Profile Records - EndTime:{DateTime.Now.ToString()}");
}

static void InsertSchoolExperienceRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string datasetId, string tableName)
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
                        insert.Add("event_occurred_at", Convert.ToDateTime(row.ItemArray[6]).ToString("yyyy-MM-dd HH:mm:ss"));
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
                        client.InsertRows(datasetId, tableName, rows.ToArray());
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
 
static void InsertCandidateWorkexpErienceRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string dataSetId)
{
    LogMessage($"Candidate Workexp Erience Records - StartTime:{DateTime.Now.ToString()}");
    var dataSet = DataLogic.GetCandidateWorkexpErienceFromCRM(builder.ConnectionString);
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
                        insert.Add("job_title", row.ItemArray[1].ToString());
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("organisation", row.ItemArray[2].ToString());
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("country", row.ItemArray[3].ToString());
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("start_year", row.ItemArray[4].ToString());
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("end_year", row.ItemArray[5].ToString());                       
                    }
                   
                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        var results = client.InsertRows(dataSetId, "work_experience", rows.ToArray());
                        rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    // LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()}");
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()} - Error Message:{ex.Message}");
                }
            }

        }
        LogMessage($"Candidate Workexp Erience Records - Total Records Processed:{counter}");
    }
    LogMessage($"Candidate Workexp Erience Records - EndTime:{DateTime.Now.ToString()}");
}

static void InsertDegreeQualificationRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string datasetId)
{
    LogMessage($"Degree Qualification Records - StartTime:{DateTime.Now}");
    var dataSet = DataLogic.GetDegreeQualificationRecordsFromCRM(builder.ConnectionString);
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
                        insert.Add("qualification_type", row.ItemArray[1]);
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("subject_name", row.ItemArray[2]);
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("uk_degree_grade", row.ItemArray[3]);
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("degree_status", row.ItemArray[4]);
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("start_year", Convert.ToInt16(row.ItemArray[5]));
                    }
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        insert.Add("end_year", Convert.ToInt16(row.ItemArray[6]));
                    }
                    if (row.ItemArray[7] != null && row.ItemArray[7] != System.DBNull.Value)
                    {
                        insert.Add("organisation_name", row.ItemArray[7]);
                    }
                    if (row.ItemArray[8] != null && row.ItemArray[8] != System.DBNull.Value)
                    {
                        insert.Add("country", row.ItemArray[8]);
                    }
                    if (row.ItemArray[9] != null && row.ItemArray[9] != System.DBNull.Value)
                    {
                        insert.Add("category", row.ItemArray[9]);
                    }

                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        client.InsertRows(datasetId, "degree_qualifications", rows.ToArray());
                        rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    // LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()}");
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()} - Error Message:{ex.Message}");
                }
            }
        }
        LogMessage($"Degree Qualification  Records - Total Records Processed:{counter}");
    }
    LogMessage($"Degree Qualification Records - EndTime:{DateTime.Now}");
}

static void InsertTTAStagesRecordsToBQInBatches(SqlConnectionStringBuilder builder, BigQueryClient client, string datasetId)
{
    LogMessage($"TTA Stages Records - StartTime:{DateTime.Now}");
    var dataSet = DataLogic.GetTTAStagesRecordsFromCRM(builder.ConnectionString);
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
                        insert.Add("stage_created_on", Convert.ToDateTime(row.ItemArray[1]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[2] != null && row.ItemArray[2] != System.DBNull.Value)
                    {
                        insert.Add("date", Convert.ToDateTime(row.ItemArray[2]).ToString("yyyy-MM-dd"));
                    }
                    if (row.ItemArray[3] != null && row.ItemArray[3] != System.DBNull.Value)
                    {
                        insert.Add("stage_name", row.ItemArray[3].ToString());
                    }
                    if (row.ItemArray[4] != null && row.ItemArray[4] != System.DBNull.Value)
                    {
                        insert.Add("current_owner", row.ItemArray[4].ToString());
                    }
                    if (row.ItemArray[5] != null && row.ItemArray[5] != System.DBNull.Value)
                    {
                        insert.Add("current_team", row.ItemArray[5].ToString());
                    }
                    if (row.ItemArray[6] != null && row.ItemArray[6] != System.DBNull.Value)
                    {
                        insert.Add("data_assigned_to_current_owner", row.ItemArray[6]);
                    }
                    if (row.ItemArray[7] != null && row.ItemArray[7] != System.DBNull.Value)
                    {
                        insert.Add("previous_owner", row.ItemArray[7].ToString());
                    }
                    if (row.ItemArray[8] != null && row.ItemArray[8] != System.DBNull.Value)
                    {
                        insert.Add("previous_team", row.ItemArray[8].ToString());
                    }
                    if (row.ItemArray[9] != null && row.ItemArray[9] != System.DBNull.Value)
                    {
                        insert.Add("date_assigned_to_previous_owner", row.ItemArray[9].ToString());
                    }

                    rows.Add(insert);
                    counter++;
                    if (counter % 1000 == 0 || counter == totalRecordsCount)
                    {
                        client.InsertRows(datasetId, "tta_stages", rows.ToArray());
                        rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    // LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()}");
                    LogMessage($"Error:{errorCount} - Id: {row.ItemArray[0].ToString()} - Error Message:{ex.Message}");
                }
            }
        }
        LogMessage($"TTA Stages Records - Total Records Processed:{counter}");
    }
    LogMessage($"TTA Stages Records - EndTime:{DateTime.Now}");
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






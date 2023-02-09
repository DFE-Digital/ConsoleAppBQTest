using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace ConsoleAppBQTest
{
    internal static class DataLogic
    {
        internal static DataSet GetSchoolExperienceRecordsFromCRM(string connectionString)
        {
            DataSet dataSet = new DataSet();
            try
            {              

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string sQuery = $"SELECT [contact_id],[urn],[subject],[placement_date],[duration],[status],[event_occurred_at],[event_occurred_on],[id] FROM [git].[school_experience_requests]";

                    using (SqlDataAdapter adapter = new SqlDataAdapter(sQuery, connection))
                    {
                        adapter.Fill(dataSet);
                    
                    }

                    //using (SqlCommand command = new SqlCommand(sQuery, connection))
                    //{
                    //    var data = command.ExecuteReader();
                    //}
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return dataSet;
        }

        internal static DataSet GetProfileRecordsFromCRM(string connectionString)
        {
            DataSet dataSet = new DataSet();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string sQuery = $"SELECT [id],[journey_stage],[degree_status],[preferred_teaching_subject_1],[preferred_teaching_subject_2],[has_postcode],[has_date_of_birth],[adviser_assigned_at],[adviser_assigned_on],[has_adviser],[international],[returner],[country],[creation_channel],[created_via_git_bat_sync],[duplicate],[created_at],[created_on],[recruitment_stage] FROM [git].[profile]";

                    using (SqlDataAdapter adapter = new SqlDataAdapter(sQuery, connection))
                    {
                        adapter.Fill(dataSet);
                    }                    
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return dataSet;
        }

        internal static DataSet GetEventRecordsFromCRM(string connectionString)
        {
            DataSet dataSet = new DataSet();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string sQuery = $"SELECT [id],[venue],[starts_at],[finishes_at],[date],[name],[status],[partial_url],[event_type],[online],[virtual] FROM [git].[events]";

                    using (SqlDataAdapter adapter = new SqlDataAdapter(sQuery, connection))
                    {
                        adapter.Fill(dataSet);
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return dataSet;            
        }

        internal static DataSet GetEventRegistrationRecordsFromCRM(string connectionString)
        {
            DataSet dataSet = new DataSet();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string sQuery = $"SELECT [id],[event_id],[contact_id],[attended],[registered_at],[registered_on],[attendance_confirmed_at],[attendance_confirmed_on],[creation_channel] FROM [git].[event_registrations]";

                    using (SqlDataAdapter adapter = new SqlDataAdapter(sQuery, connection))
                    {
                        adapter.Fill(dataSet);
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return dataSet;
        }

        internal static DataSet GetMLSubscriptionRecordsFromCRM(string connectionString)
        {
            DataSet dataSet = new DataSet();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string sQuery = $"SELECT [contact_id],[subscribed_at],[subscribed_on],[subscription_channel],[still_subscribed],[opted_out_of_all_emails],[opted_out_of_bulk_emails],[opted_out_of_post] FROM [git].[mailing_list_subscriptions]";

                    using (SqlDataAdapter adapter = new SqlDataAdapter(sQuery, connection))
                    {
                        adapter.Fill(dataSet);
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return dataSet;
        }

        internal static DataSet GetTTASignupRecordsFromCRM(string connectionString)
        {
            DataSet dataSet = new DataSet();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string sQuery = $"SELECT [contact_id],[signed_up_at],[signed_up_on],[subscription_channel],[opted_out_of_all_emails],[opted_out_of_bulk_emails],[opted_out_of_post] FROM [git].[teacher_training_adviser_signups]";

                    using (SqlDataAdapter adapter = new SqlDataAdapter(sQuery, connection))
                    {
                        adapter.Fill(dataSet);
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return dataSet;
        }

        internal static DataSet GetApplicationRecordsFromCRM(string connectionString)
        {
            DataSet dataSet = new DataSet();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    string sQuery = $"SELECT [contact_id],[applied_at],[applied_on],[phase],[status],[id],[recruitment_year],[application_complete],[success],[application_form_id],[dfe_applyid] FROM [git].[applications]";

                    using (SqlDataAdapter adapter = new SqlDataAdapter(sQuery, connection))
                    {
                        adapter.Fill(dataSet);
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return dataSet;
        }
    }
}

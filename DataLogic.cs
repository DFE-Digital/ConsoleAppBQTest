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

        public static DataSet GetSchoolExperienceRecordsFromCRM(string connectionString)
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
    }
}

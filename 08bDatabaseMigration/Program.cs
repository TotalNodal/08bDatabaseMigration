using System;
using System.Data;
using System.Data.SqlClient;
using MongoDB.Driver;
using MongoDB.Bson;

class Program
{
    static void Main()
    {
        // SQL Server connection string
        string sqlConnectionString = "Server=DESKTOP-TITA3TC\\VE_SERVER;Database=AirlineWebDB;Integrated Security=True;";

        // MongoDB connection string
        string mongoConnectionString = "mongodb://localhost:27017";
        MongoClient mongoClient = new MongoClient(mongoConnectionString);
        var mongoDatabase = mongoClient.GetDatabase("AirlineWebDB");

        // Tables to migrate
        string[] tables = { "FlightDetails", "WebhookSubscriptions" };

        foreach (var table in tables)
        {
            // Fetch data from SQL Server
            DataTable dt = FetchDataFromSqlServer(sqlConnectionString, table);

            // Transform and insert data into MongoDB
            InsertDataIntoMongoDB(mongoDatabase, table, dt);
        }

        Console.WriteLine("Migration completed!");
    }

    static DataTable FetchDataFromSqlServer(string connectionString, string tableName)
    {
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            conn.Open();
            SqlCommand cmd = new SqlCommand($"SELECT * FROM {tableName}", conn);
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();
            da.Fill(dt);
            return dt;
        }
    }

    static void InsertDataIntoMongoDB(IMongoDatabase db, string collectionName, DataTable dt)
    {
        var collection = db.GetCollection<BsonDocument>(collectionName);

        foreach (DataRow row in dt.Rows)
        {
            var document = new BsonDocument();

            foreach (DataColumn col in dt.Columns)
            {
                // Convert DBNull to null for MongoDB
                var value = row[col] == DBNull.Value ? BsonNull.Value : BsonValue.Create(row[col]);
                document.Add(new BsonElement(col.ColumnName, value));
            }

            collection.InsertOne(document);
        }
    }
}

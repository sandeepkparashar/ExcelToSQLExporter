using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using ExcelDataReader;
using System.Text;


namespace ExcelToSQLExporter
{
    internal class Program
    {
        static string connectionString = "Server=SANDEEP-HOME\\SQL2022;Database=pulseIQ_20250226;Integrated Security=True;";

        static void Main()
        {
            string directoryPath = @"D:\PulseIq\20250226-CCBTrx\input\New folder"; // Change to your directory path

            if (!Directory.Exists(directoryPath))
            {
                Console.WriteLine("Directory does not exist.");
                return;
            }

            string[] files = Directory.GetFiles(directoryPath, "*.*", SearchOption.AllDirectories);

            foreach (var file in files)
            {
                string extension = Path.GetExtension(file).ToLower();
                if (extension == ".csv")
                {
                    ProcessCsvFile(file);
                }
                else if (extension == ".xlsx" || extension == ".xls")
                {
                    ProcessExcelFile(file);
                }
            }

            Console.WriteLine("All files processed successfully.");
        }

        static void ProcessCsvFile(string filePath)
        {
            string tableName = "[CSV_" + Path.GetFileNameWithoutExtension(filePath).Replace(" ", "_")+"]";// Enclose table name in square brackets to make it safe in case there are special characters in the name

            using (var reader = new StreamReader(filePath))
            {
                string headerLine = reader.ReadLine();
                if (headerLine == null) return;

                string[] columns = headerLine.Split(',');

                CreateSqlTable(tableName, columns);

                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();

                    string insertQuery = $"INSERT INTO {tableName} ({string.Join(",", columns.Select(col => $"[{col.Replace("\"", string.Empty)}]"))}) VALUES ({string.Join(",", new string[columns.Length].Select((_, i) => $"@param{i}"))})";

                    using (var command = new SqlCommand(insertQuery, sqlConnection))
                    {
                        for (int i = 0; i < columns.Length; i++)
                        {
                            command.Parameters.Add(new SqlParameter($"@param{i}", SqlDbType.NVarChar));
                        }

                        while (!reader.EndOfStream)
                        {
                            string[] values = reader.ReadLine()?.Split(',');

                            if (values == null || values.Length != columns.Length) continue;

                            for (int i = 0; i < columns.Length; i++)
                            {
                                command.Parameters[$"@param{i}"].Value = values[i];
                            }

                            command.ExecuteNonQuery();
                        }
                    }
                }
            }

            Console.WriteLine($"CSV file '{filePath}' processed successfully.");
        }

        static void ProcessExcelFile(string filePath)
        {
            string tableName = "[Excel_" + Path.GetFileNameWithoutExtension(filePath).Replace(" ", "_")+"]";  // Enclose table name in square brackets to make it safe in case there are special characters in the name

            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

            using (var stream = File.Open(filePath, FileMode.Open, FileAccess.Read))
            using (var reader = ExcelReaderFactory.CreateReader(stream))
            {
                var result = reader.AsDataSet();
                DataTable table = result.Tables[0];

                string[] columns = new string[table.Columns.Count];
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    columns[i] = table.Rows[0][i].ToString();
                }

                CreateSqlTable(tableName, columns);

                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();

                    string insertQuery = $"INSERT INTO {tableName} ({string.Join(", ", columns.Select(col => $"[{col.Replace("\"", string.Empty)}]"))}) VALUES ({string.Join(",", new string[columns.Length].Select((_, i) => $"@param{i}"))})";

                    using (var command = new SqlCommand(insertQuery, sqlConnection))
                    {
                        for (int i = 0; i < columns.Length; i++)
                        {
                            command.Parameters.Add(new SqlParameter($"@param{i}", SqlDbType.NVarChar));
                        }

                        for (int i = 1; i < table.Rows.Count; i++)
                        {
                            DataRow row = table.Rows[i];

                            for (int j = 0; j < columns.Length; j++)
                            {
                                command.Parameters[$"@param{j}"].Value = row[j].ToString();
                            }

                            command.ExecuteNonQuery();
                        }
                    }
                }
            }

            Console.WriteLine($"Excel file '{filePath}' processed successfully.");
        }

        static void CreateSqlTable(string tableName, string[] columns)
        {
            

            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();

                string checkTableExistsQuery = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName};";
                using (var checkCommand = new SqlCommand(checkTableExistsQuery, sqlConnection))
                {
                    checkCommand.ExecuteNonQuery();
                }

                string createTableQuery = $"CREATE TABLE {tableName} ({string.Join(", ", columns.Select(col => $"[{col.Replace("\"",string.Empty)}] NVARCHAR(MAX)"))});";
                using (var command = new SqlCommand(createTableQuery, sqlConnection))
                {
                    command.ExecuteNonQuery();
                }
            }

            Console.WriteLine($"Table '{tableName}' created successfully.");
        }

    }
}


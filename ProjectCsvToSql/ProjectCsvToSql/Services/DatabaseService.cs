using Microsoft.Data.SqlClient;
using ProjectCsvToSql.Models;
using ProjectCsvToSql.Services.Interfaces;
using System.Data;

namespace ProjectCsvToSql.Services
{
	public class DatabaseService : IDatabaseService
	{
		private readonly string _connectionString;
		private DataTable _dataTable = new();

		public DatabaseService(string connectionString)
		{
			_connectionString = connectionString;
			InitializeDataTable();
		}

		private void InitializeDataTable()
		{
			_dataTable = new DataTable();
			_dataTable.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
			_dataTable.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
			_dataTable.Columns.Add("passenger_count", typeof(int));
			_dataTable.Columns.Add("trip_distance", typeof(decimal));
			_dataTable.Columns.Add("store_and_fwd_flag", typeof(string));
			_dataTable.Columns.Add("pu_location_id", typeof(int));
			_dataTable.Columns.Add("do_location_id", typeof(int));
			_dataTable.Columns.Add("fare_amount", typeof(decimal));
			_dataTable.Columns.Add("tip_amount", typeof(decimal));
		}

		public async Task InsertDataAsync(List<CsvData> data)
		{
			using var connection = await CreateAndOpenConnectionAsync();

			_dataTable.Rows.Clear();

			foreach (var record in data)
			{
				_dataTable.Rows.Add(record.TpepPickupDatetime, 
					record.TpepDropoffDatetime, 
					record.PassengerCount, 
					record.TripDistance, 
					record.StoreAndFwdFlag ?? string.Empty, 
					record.PULocationId, 
					record.DOLocationId, 
					record.FareAmount, 
					record.TipAmount);
			}

			using var bulkCopy = new SqlBulkCopy(connection)
			{
				DestinationTableName = "csv_data"
			};

			bulkCopy.ColumnMappings.Add("tpep_pickup_datetime", "tpep_pickup_datetime");
			bulkCopy.ColumnMappings.Add("tpep_dropoff_datetime", "tpep_dropoff_datetime");
			bulkCopy.ColumnMappings.Add("passenger_count", "passenger_count");
			bulkCopy.ColumnMappings.Add("trip_distance", "trip_distance");
			bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
			bulkCopy.ColumnMappings.Add("pu_location_id", "pu_location_id");
			bulkCopy.ColumnMappings.Add("do_location_id", "do_location_id");
			bulkCopy.ColumnMappings.Add("fare_amount", "fare_amount");
			bulkCopy.ColumnMappings.Add("tip_amount", "tip_amount");

			await bulkCopy.WriteToServerAsync(_dataTable);
		}

		public async Task<(bool Success, int DuplicatesFound, int RemainingRecords, string Message)> ExecuteDuplicationProcedureAsync()
		{
			using var connection = await CreateAndOpenConnectionAsync();

			using var command = new SqlCommand("dbo.ExtractAndRemoveDuplicates", connection)
			{
				CommandType = CommandType.StoredProcedure
			};

			try
			{
				using var reader = await command.ExecuteReaderAsync();
				if (await reader.ReadAsync())
				{
					bool success = reader.GetInt32(0) == 1;
					int duplicatesFound = await reader.IsDBNullAsync(1) ? 0 : reader.GetInt32(1);
					int remainingRecords = await reader.IsDBNullAsync(2) ? 0 : reader.GetInt32(2);

					string message;
					if (success)
					{
						message = "Success";
					}
					else
					{
						var errorCode = await reader.IsDBNullAsync(3) ? 0 : reader.GetInt32(3);
						var errorMessage = await reader.IsDBNullAsync(4) ? "Unknown error" : reader.GetString(4);
						var lineNumber = await reader.IsDBNullAsync(5) ? 0 : reader.GetInt32(5);
						message = $"Error {errorCode}: {errorMessage} (Line {lineNumber})";
					}

					return (success, duplicatesFound, remainingRecords, message);
				}
				return (false, 0, 0, "No results returned from procedure");
			}
			catch (Exception ex)
			{
				return (false, 0, 0, $"Procedure execution failed: {ex.Message}");
			}
		}

		public async Task<List<CsvData>> GetDuplicateBatchAsync(int offset, int batchSize)
		{
			var batch = new List<CsvData>();

			using var connection = await CreateAndOpenConnectionAsync();

			string query = @"
                SELECT 
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    store_and_fwd_flag,
                    pu_location_id,
                    do_location_id,
                    fare_amount,
                    tip_amount
                FROM dbo.duplicates
                ORDER BY id
                OFFSET @Offset ROWS FETCH NEXT @BatchSize ROWS ONLY";

			using var command = new SqlCommand(query, connection);
			command.Parameters.AddWithValue("@Offset", offset);
			command.Parameters.AddWithValue("@BatchSize", batchSize);

			using var reader = await command.ExecuteReaderAsync();
			while (await reader.ReadAsync())
			{
				batch.Add(new CsvData
				{
					TpepPickupDatetime = await reader.IsDBNullAsync(0) ? null : reader.GetDateTime(0),
					TpepDropoffDatetime = await reader.IsDBNullAsync(1) ? null : reader.GetDateTime(1),
					PassengerCount = await reader.IsDBNullAsync(2) ? null : (uint?)reader.GetInt32(2),
					TripDistance = await reader.IsDBNullAsync(3) ? null : reader.GetDecimal(3),
					StoreAndFwdFlag = await reader.IsDBNullAsync(4) ? null : reader.GetString(4),
					PULocationId = await reader.IsDBNullAsync(5) ? null : (uint?)reader.GetInt32(5),
					DOLocationId = await reader.IsDBNullAsync(6) ? null : (uint?)reader.GetInt32(6),
					FareAmount = await reader.IsDBNullAsync(7) ? null : reader.GetDecimal(7),
					TipAmount = await reader.IsDBNullAsync(8) ? null : reader.GetDecimal(8)
				});
			}

			return batch;
		}

		private async Task<SqlConnection> CreateAndOpenConnectionAsync()
		{
			var connection = new SqlConnection(_connectionString);
			await connection.OpenAsync();
			return connection;
		}
	}
}
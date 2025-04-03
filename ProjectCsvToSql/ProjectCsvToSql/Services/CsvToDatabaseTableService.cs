using CsvHelper;
using Microsoft.Extensions.Logging;
using ProjectCsvToSql.Mapping;
using ProjectCsvToSql.Models;
using ProjectCsvToSql.Services.Interfaces;
using System.Globalization;

namespace ProjectCsvToSql.Services
{
	public class CsvToDatabaseTableService : ICsvToDatabaseTableService
	{
		private readonly IDatabaseService _databaseService;
		private readonly ILogger<CsvToDatabaseTableService> _logger;

		public CsvToDatabaseTableService(IDatabaseService databaseService, ILogger<CsvToDatabaseTableService> logger)
		{
			_databaseService = databaseService;
			_logger = logger;
		}

		public async Task ProcessCsvInChunksAsync(string filePath, int chunkSize)
		{
			using var reader = new StreamReader(filePath);
			using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

			csv.Context.RegisterClassMap<CsvMapData>();

			var records = new List<CsvData>();

			await foreach (var record in csv.GetRecordsAsync<CsvData>())
			{

				try
				{
					if (record.StoreAndFwdFlag != null)
					{
						record.StoreAndFwdFlag = ConvertYToYesNToNo(record.StoreAndFwdFlag);
					}
					records.Add(record);
				}
				catch (Exception ex)
				{
					_logger.LogError(ex, "Error processing a csv record.");
				}

				// If we've reached the chunk size, insert and reset the list
				if (records.Count >= chunkSize)
				{
					await _databaseService.InsertDataAsync(records);

					records.Clear();
				}
			}

			// Insert any remaining records
			if (records.Count > 0)
			{
				await _databaseService.InsertDataAsync(records);
			}
		}

		private static string ConvertYToYesNToNo(string value)
		{
			if (value == "Y")
			{
				return "Yes";
			}
			
			if (value == "N")
			{
				return "No";
			}

			return value;
		}
	}
}
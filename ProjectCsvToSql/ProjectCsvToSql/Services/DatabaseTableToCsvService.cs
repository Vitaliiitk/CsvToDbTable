using CsvHelper;
using Microsoft.Extensions.Logging;
using ProjectCsvToSql.Mapping;
using ProjectCsvToSql.Models;
using ProjectCsvToSql.Services.Interfaces;
using System.Globalization;

namespace ProjectCsvToSql.Services
{
	public class DatabaseTableToCsvService : IDatabaseTableToCsvService
	{
		private readonly IDatabaseService _databaseService;
		private readonly ILogger<DatabaseTableToCsvService> _logger;

		public DatabaseTableToCsvService(IDatabaseService databaseService, ILogger<DatabaseTableToCsvService> logger)
		{
			_databaseService = databaseService;
			_logger = logger;
		}

		public async Task ProcessAndExportDuplicatesAsync(string csvFilePath, int chunkSize)
		{
			var directory = Path.GetDirectoryName(csvFilePath);
			if (string.IsNullOrWhiteSpace(directory)) 
			{
				throw new ArgumentNullException(nameof(csvFilePath), "The file path does not contain a valid directory");
			}

			if (!Directory.Exists(directory))
			{
				Directory.CreateDirectory(directory);
			}

			await ExportDuplicatesToCsvAsync(csvFilePath, chunkSize);
		}

		public async Task<bool> CheckForDuplicatesAsync()
		{
			// Execute the stored SQL procedure.
			var procedureResult = await _databaseService.ExecuteDuplicationProcedureAsync();
			if (!procedureResult.Success)
			{
				throw new InvalidOperationException($"Duplication procedure failed: {procedureResult.Message}");
			}

			if (procedureResult.DuplicatesFound == 0)
			{
				_logger.LogInformation("No duplicates found - nothing to export");
				return false;
			}

			_logger.LogInformation("Found {DuplicatesFound} duplicates. {RemainingRecords} records remain.", procedureResult.DuplicatesFound, procedureResult.RemainingRecords);
			return true;
		}

		private async Task ExportDuplicatesToCsvAsync(string filePath, int chunkSize)
		{
			bool isNewFile = !File.Exists(filePath);

			var config = new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture)
			{
				Delimiter = ",",
				HasHeaderRecord = isNewFile
			};

			// If csv file exists append, otherwise - create
			var fileMode = isNewFile ? FileMode.Create : FileMode.Append;

			using (var stream = new FileStream(filePath, fileMode, FileAccess.Write, FileShare.Read))
			using (var writer = new StreamWriter(stream))
			using (var csv = new CsvWriter(writer, config))
			{
				csv.Context.RegisterClassMap<CsvMapData>();

				// Write header for new file
				if (isNewFile || stream.Length == 0)
				{
					csv.WriteHeader<CsvData>();
					await csv.NextRecordAsync();
					await writer.FlushAsync();
				}

				int offset = 0;
				bool hasMoreRecords = true;

				while (hasMoreRecords)
				{
					var batch = await _databaseService.GetDuplicateBatchAsync(offset, chunkSize);
					if (batch.Count == 0)
					{
						hasMoreRecords = false;
						continue;
					}

					foreach (var record in batch)
					{
						csv.WriteRecord(record);
						await csv.NextRecordAsync();
					}

					offset += batch.Count;
				}
				_logger.LogInformation("Exported {Offset} duplicates...", offset);
			}

			_logger.LogInformation("Successfully exported duplicates to {FilePath}", filePath);
		}
	}
}
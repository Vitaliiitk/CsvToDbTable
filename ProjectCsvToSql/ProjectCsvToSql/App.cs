using Microsoft.Extensions.Logging;
using ProjectCsvToSql.Services.Interfaces;
using System.IO;

namespace ProjectCsvToSql
{
	public class App
	{
		private readonly ICsvToDatabaseTableService _csvToDatabaseTableService;
		private readonly IDatabaseTableToCsvService _databaseTableToCsvService;
		private readonly ILogger<App> _logger;

		public App(ICsvToDatabaseTableService csvToDatabaseTableService, IDatabaseTableToCsvService databaseTableToCsvService, ILogger<App> logger)
		{
			_csvToDatabaseTableService = csvToDatabaseTableService;
			_databaseTableToCsvService = databaseTableToCsvService;
			_logger = logger;
		}

		public async Task RunAsync()
		{
			_logger.LogInformation("Starting CSV processing...");
			_logger.LogInformation("Enter a path to csv file (example, C:\\\\Folder1\\\\mycsvfile.csv):");
			string? csvFilePath = Console.ReadLine();

			if (string.IsNullOrWhiteSpace(csvFilePath))
			{
				_logger.LogError("Invalid file path.");
				return;
			}

			try
			{
				// Work on data in chunks
				ushort chunkSize = 10000;
				_logger.LogInformation("Processing CSV file in chunks of {ChunkSize} rows...", chunkSize);

				await _csvToDatabaseTableService.ProcessCsvInChunksAsync(csvFilePath, chunkSize);
				_logger.LogInformation("CSV processing and data insertion completed.");

				if (await _databaseTableToCsvService.CheckForDuplicatesAsync())
				{
					_logger.LogInformation("Enter a path to save duplicates (example, C:\\\\Folder1\\\\duplicates.csv):");
					string? pathForDuplicatesSave = Console.ReadLine();

					if (string.IsNullOrWhiteSpace(pathForDuplicatesSave))
					{
						_logger.LogError("Invalid file path.");
						return;
					}

					await _databaseTableToCsvService.ProcessAndExportDuplicatesAsync(pathForDuplicatesSave, chunkSize);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex,"Error occurred:");
			}
		}
	}
}
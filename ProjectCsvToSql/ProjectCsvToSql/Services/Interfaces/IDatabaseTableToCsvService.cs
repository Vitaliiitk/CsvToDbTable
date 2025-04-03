namespace ProjectCsvToSql.Services.Interfaces
{
	public interface IDatabaseTableToCsvService
	{
		Task ProcessAndExportDuplicatesAsync(string csvFilePath, int chunkSize);
		Task<bool> CheckForDuplicatesAsync();
	}
}
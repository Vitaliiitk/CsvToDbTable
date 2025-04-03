using ProjectCsvToSql.Models;

namespace ProjectCsvToSql.Services.Interfaces
{
	public interface IDatabaseService
	{
		Task InsertDataAsync(List<CsvData> data);
		Task<List<CsvData>> GetDuplicateBatchAsync(int offset, int batchSize);
		Task<(bool Success, int DuplicatesFound, int RemainingRecords, string Message)> ExecuteDuplicationProcedureAsync();
	}
}
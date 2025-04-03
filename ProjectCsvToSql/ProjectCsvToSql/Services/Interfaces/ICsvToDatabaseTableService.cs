namespace ProjectCsvToSql.Services.Interfaces
{
	public interface ICsvToDatabaseTableService
	{
		Task ProcessCsvInChunksAsync(string filePath, int chunkSize);
	}
}
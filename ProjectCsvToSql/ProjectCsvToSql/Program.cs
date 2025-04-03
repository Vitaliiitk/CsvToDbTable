using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ProjectCsvToSql;
using ProjectCsvToSql.Services;
using ProjectCsvToSql.Services.Interfaces;

var builder = Host.CreateDefaultBuilder(args);

builder.ConfigureServices((context, services) =>
{
	var connectionString = context.Configuration.GetConnectionString("MyDbConnection");
	if (connectionString == null)
	{
		throw new InvalidOperationException("Some database connection string problem.");
	}

	services.AddScoped<ICsvToDatabaseTableService, CsvToDatabaseTableService>();
	services.AddScoped<IDatabaseTableToCsvService, DatabaseTableToCsvService>();
	services.AddScoped<IDatabaseService>(_ => new DatabaseService(connectionString));

	services.AddSingleton<App>();
});

var app = builder.Build();
var application = app.Services.GetRequiredService<App>();
await application.RunAsync();
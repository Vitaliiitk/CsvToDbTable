namespace ProjectCsvToSql.Models
{
	public class CsvData
	{
		public DateTime? TpepPickupDatetime { get; set; }
		public DateTime? TpepDropoffDatetime { get; set; }
		public uint? PassengerCount { get; set; }
		public decimal? TripDistance { get; set; }
		public string? StoreAndFwdFlag { get; set; }
		public uint? PULocationId { get; set; }
		public uint? DOLocationId { get; set; }
		public decimal? FareAmount { get; set; }
		public decimal? TipAmount { get; set; }
	}
}
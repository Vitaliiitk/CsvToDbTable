using CsvHelper.Configuration;
using ProjectCsvToSql.Models;

namespace ProjectCsvToSql.Mapping
{
	public sealed class CsvMapData : ClassMap<CsvData>
	{
		public CsvMapData()
		{
			Map(m => m.TpepPickupDatetime).Name("tpep_pickup_datetime").TypeConverterOption.Format("MM/dd/yyyy hh:mm:ss tt");
			Map(m => m.TpepDropoffDatetime).Name("tpep_dropoff_datetime").TypeConverterOption.Format("MM/dd/yyyy hh:mm:ss tt");
			Map(m => m.PassengerCount).Name("passenger_count");
			Map(m => m.TripDistance).Name("trip_distance");
			Map(m => m.StoreAndFwdFlag).Name("store_and_fwd_flag");
			Map(m => m.PULocationId).Name("PULocationID");
			Map(m => m.DOLocationId).Name("DOLocationID");
			Map(m => m.FareAmount).Name("fare_amount");
			Map(m => m.TipAmount).Name("tip_amount");
		}
	}
}
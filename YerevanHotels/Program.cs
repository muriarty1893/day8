using System;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;
using Nest;

class Program
{
    public class Hotel
    {
        public string? HotelNames { get; set; }
        public string? FreeParking { get; set; }
        public double PricePerDay { get; set; }
    }

    public sealed class HotelMap : ClassMap<Hotel>
    {
        public HotelMap()
        {
            Map(m => m.HotelNames).Name("Hotel Names");
            Map(m => m.FreeParking).Name("Free Parking");
            Map(m => m.PricePerDay).Name("Price Per Day($)");
        }
    }

    static void Main(string[] args)
    {
        var settings = new ConnectionSettings(new Uri("http://localhost:9200"))
            .DefaultIndex("hotels");

        var client = new ElasticClient(settings);

        // Mevcut indeksi sil
        var deleteIndexResponse = client.Indices.Delete("hotels");

        if (!deleteIndexResponse.IsValid && !deleteIndexResponse.ApiCall.Success)
        {
            Console.WriteLine($"ElasticSearch Error: {deleteIndexResponse.ServerError?.Error?.Reason}");
            return;
        }
        Console.WriteLine("Index deleted.");

        // Yeni bir indeks oluştur
        var createIndexResponse = client.Indices.Create("hotels", c => c
            .Map<Hotel>(m => m
                .AutoMap()
            )
        );

        if (!createIndexResponse.IsValid)
        {
            Console.WriteLine($"ElasticSearch Error: {createIndexResponse.ServerError?.Error?.Reason}");
            return;
        }
        Console.WriteLine("Index created.");

        // CSV dosyasını okuma ve Elasticsearch'e yükleme
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            MissingFieldFound = null, // Eksik alanları yok say
            BadDataFound = context => { /* Hatalı verileri işleme */ },
        };

        using (var reader = new StreamReader(@"C:\Users\Murat Eker\Desktop\day8-3\YerevanHotels\oteller.csv"))
        using (var csv = new CsvReader(reader, config))
        {
            csv.Context.RegisterClassMap<HotelMap>(); // ClassMap'i kaydet

            var records = csv.GetRecords<Hotel>().ToList();  // Tüm kayıtları listeye al

            foreach (var record in records)
            {
                var response = client.IndexDocument(record);
                if (!response.IsValid)
                {
                    Console.WriteLine($"ElasticSearch Error: {response.ServerError?.Error?.Reason}");
                }
            }
        }

        // Verilerin Elasticsearch'e yüklenip yüklenmediğini kontrol et
        var countResponse = client.Count<Hotel>(c => c
            .Index("hotels")
        );

        Console.WriteLine($"Total Documents in Index: {countResponse.Count}");

        // Basit bir arama sorgusu örneği
        var searchResponse = client.Search<Hotel>(s => s
            .Query(q => q
                .Match(m => m
                    .Field(f => f.HotelNames)
                    .Query("hostel")
                )
            )       
        );

        Console.WriteLine($"Total Hits: {searchResponse.Total}");

        foreach (var hit in searchResponse.Hits)
        {
            Console.WriteLine($"Hotel: {hit.Source.HotelNames}, Free Parking: {hit.Source.FreeParking}, Price Per Day: ${hit.Source.PricePerDay}");
        }
    }
}

using System;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;
using Nest;

class Program
{
    // Hotel sınıfını CSV dosyasına uygun şekilde güncelleyelim
    public class Hotel
    {
        public string? HotelNames { get; set; }
        public string? FreeParking { get; set; }
        public double PricePerDay { get; set; }
    }

    // Hotel sınıfı için CSV yapılandırması
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
        // Elasticsearch yapılandırması
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

        // CSV dosyasını okuma ve Elasticsearch'e yükleme
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            MissingFieldFound = null, // Eksik alanları yok say
            BadDataFound = context => { /* Hatalı verileri işleme */ },
        };

        using (var reader = new StreamReader("oteller.csv"))
        using (var csv = new CsvReader(reader, config))
        {
            csv.Context.RegisterClassMap<HotelMap>(); // ClassMap'i kaydet

            var records = csv.GetRecords<Hotel>();

            foreach (var record in records)
            {
                var response = client.IndexDocument(record);
                if (!response.IsValid)
                {
                    Console.WriteLine($"ElasticSearch Error: {response.ServerError?.Error?.Reason}");
                }
            }
        }

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

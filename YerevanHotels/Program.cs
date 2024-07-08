using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;
using Nest;

public class Hotel
{
    public string? HotelName { get; set; }
    public string? FreeParking { get; set; }
    public decimal PricePerDay { get; set; }
}

public class HotelMap : ClassMap<Hotel>
{
    public HotelMap()
    {
        // CSV dosyasındaki sütun adlarını sınıfın alanlarıyla eşleştir
        Map(m => m.HotelName).Name("HotelName");
        Map(m => m.FreeParking).Name("FreeParking");
        Map(m => m.PricePerDay).Name("PricePerDay");
    }
}

public class Program
{
    // Elasticsearch istemcisini oluşturur
    private static ElasticClient CreateElasticClient()
    {
        var settings = new ConnectionSettings(new Uri("http://localhost:9200"))
            .DefaultIndex("hotels"); // Varsayılan indeks adı
        return new ElasticClient(settings);
    }

    // CSV dosyasını okuyarak otel bilgilerini bir listeye atar
    private static List<Hotel> ReadCsv(string filePath)
    {
        using (var reader = new StreamReader(filePath))
        using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
        {
            csv.Context.RegisterClassMap<HotelMap>();
            return new List<Hotel>(csv.GetRecords<Hotel>());
        }
    }

    // Otel bilgilerini Elasticsearch'e yazar veya günceller
    private static void IndexHotels(ElasticClient client, List<Hotel> hotels)
    {
        foreach (var hotel in hotels)
        {
            var response = client.IndexDocument(hotel);
            if (!response.IsValid)
            {
                Console.WriteLine($"Error indexing hotel: {hotel.HotelName}, Reason: {response.ServerError}");
            }
        }
    }

    // Otel bilgilerini Elasticsearch'ten siler
    private static void DeleteHotels(ElasticClient client)
    {
        var deleteResponse = client.DeleteByQuery<Hotel>(q => q
            .Query(rq => rq
                .MatchAll()
            )
        );
    }

    // Elasticsearch'te bir arama sorgusu çalıştırır ve sonuçları ekrana yazar
    private static void SearchHotels(ElasticClient client, string searchText)
    {
        var searchResponse = client.Search<Hotel>(s => s
            .Query(q => q
                .Match(m => m
                    .Field(f => f.HotelName)
                    .Query(searchText)
                )
            )
        );

        foreach (var hotel in searchResponse.Documents)
        {
            Console.WriteLine($"Hotel: {hotel.HotelName}, Price: {hotel.PricePerDay}, Free Parking: {hotel.FreeParking}");
        }
    }

    public static void Main(string[] args)
    {
        var filePath = "oteller.csv";
        var hotels = ReadCsv(filePath); // CSV dosyasını oku

        var client = CreateElasticClient(); // Elasticsearch istemcisini oluştur

        // Mevcut otel verilerini sil
        DeleteHotels(client);

        // Yeni otel verilerini Elasticsearch'e yaz
        IndexHotels(client, hotels);

        // Otelleri ara ve sonuçları ekrana yazdır
        SearchHotels(client, "Hostel");
    }
}

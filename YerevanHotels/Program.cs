using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;
using Nest;

public class Product
{
    public double? ProductId { get; set; }
    public string? ProductName { get; set; }
    public string? StockCode { get; set; }
    public double? StockUnit { get; set; }
    public DateTime? CreatedDate { get; set; }
    public DateTime? ReleaseDate { get; set; }
    public string? ProductJson { get; set; }
    public double? ProductStateId { get; set; }
    public string? Barcode { get; set; }
    public bool? FlagStockOption { get; set; }
    public double? StockLimitId { get; set; }
    public double? ProductSizeId { get; set; }
    public string? Description { get; set; }
    public string? ShortDescription { get; set; }
    public bool? FlagPriceOption { get; set; }
    public string? ProductContent { get; set; }
    public double? ProductTypeId { get; set; }
    public double? MetaId { get; set; }
    public string? ImportName { get; set; }
    public double? TaxId { get; set; }
    public string? SerialNo { get; set; }
    public double? ProductBranchId { get; set; }
    public double? CompanyId { get; set; }
    public double? StokQuantity { get; set; }
    public decimal? TaxTotal { get; set; }
    public decimal? ItemPrice { get; set; }
    public decimal? RegularPrice { get; set; }
    public bool? SaleFlag { get; set; }
    public decimal? OldPrice { get; set; }
    public bool? FlagStockManage { get; set; }
    public string? SaleTag { get; set; }
    public bool? FlagStockOnline { get; set; }
    public bool? FlagOnStock { get; set; }
    public bool? FlagActive { get; set; }
    public bool? FlagLock { get; set; }
    public double? CurrencyId { get; set; }
    public decimal? PurchasePrice { get; set; }
}

public class ProductMap : ClassMap<Product>
{
    public ProductMap()
    {
        Map(m => m.ProductId).Name("productId");
        Map(m => m.ProductName).Name("productName");
        Map(m => m.StockCode).Name("stockCode");
        Map(m => m.StockUnit).Name("stockUnit");
        Map(m => m.CreatedDate).Name("createdDate");
        Map(m => m.ReleaseDate).Name("releaseDate");
        Map(m => m.ProductJson).Name("productJson");
        Map(m => m.ProductStateId).Name("productStateId");
        Map(m => m.Barcode).Name("barcode");
        Map(m => m.FlagStockOption).Name("flagStockOption");
        Map(m => m.StockLimitId).Name("stockLimitId");
        Map(m => m.ProductSizeId).Name("productSizeId");
        Map(m => m.Description).Name("description");
        Map(m => m.ShortDescription).Name("shortDescription");
        Map(m => m.FlagPriceOption).Name("flagPriceOption");
        Map(m => m.ProductContent).Name("productContent");
        Map(m => m.ProductTypeId).Name("productTypeId");
        Map(m => m.MetaId).Name("metaId");
        Map(m => m.ImportName).Name("importName");
        Map(m => m.TaxId).Name("taxId");
        Map(m => m.SerialNo).Name("serialNo");
        Map(m => m.ProductBranchId).Name("productBranchId");
        Map(m => m.CompanyId).Name("companyId");
        Map(m => m.StokQuantity).Name("stokQuantity");
        Map(m => m.TaxTotal).Name("taxTotal");
        Map(m => m.ItemPrice).Name("itemPrice");
        Map(m => m.RegularPrice).Name("regularPrice");
        Map(m => m.SaleFlag).Name("saleFlag");
        Map(m => m.OldPrice).Name("oldPrice");
        Map(m => m.FlagStockManage).Name("flagStockManage");
        Map(m => m.SaleTag).Name("saleTag");
        Map(m => m.FlagStockOnline).Name("flagStockOnline");
        Map(m => m.FlagOnStock).Name("flagOnStock");
        Map(m => m.FlagActive).Name("flagActive");
        Map(m => m.FlagLock).Name("flagLock");
        Map(m => m.CurrencyId).Name("currencyId");
        Map(m => m.PurchasePrice).Name("purchasePrice");
    }
}

public class Program
{
    private static ElasticClient CreateElasticClient()
    {
        // Elasticsearch bağlantı ayarlarını yapılandırır ve bir ElasticClient döndürür.
        var settings = new ConnectionSettings(new Uri("http://localhost:9200"))
            .DefaultIndex("products");
        return new ElasticClient(settings);
    }

    private static List<Product> ReadCsv(string filePath)
    {
        // Verilen CSV dosyasını okur ve Product nesnelerini içeren bir liste döndürür.
        using (var reader = new StreamReader(filePath))
        using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
        {
            csv.Context.RegisterClassMap<ProductMap>();
            return new List<Product>(csv.GetRecords<Product>());
        }
    }

    private static void IndexProducts(ElasticClient client, List<Product> products)
    {
        // Elasticsearch'e ürünleri indeksler.
        foreach (var product in products)
        {
            var response = client.IndexDocument(product);
            if (!response.IsValid)
            {
                Console.WriteLine($"Error indexing product: {product.ProductName}, Reason: {response.ServerError}");
            }
        }
    }

    private static void DeleteProducts(ElasticClient client)
    {
        // Elasticsearch'ten tüm ürünleri siler.
        var deleteResponse = client.DeleteByQuery<Product>(q => q
            .Query(rq => rq
                .MatchAll()
            )
        );
    }

    private static void SearchProducts(ElasticClient client, string searchText)
    {
    
        // Verilen metinle eşleşen ürünleri Elasticsearch'te arar.
        var searchResponse = client.Search<Product>(s => s
            .Query(q => q
                .MultiMatch(mm => mm
                    .Query(searchText)
                    .Fields(f => f
                        .Field(p => p.ProductName, 3.0) // Ürün adına ağırlık verir.
                        .Field(p => p.Description)     // Açıklamaya göre arar.
                    )
                    .Fuzziness(Fuzziness.Auto)       // Otomatik bulanıklık ayarı.
                )
            )
            .Sort(srt => srt
                .Descending(SortSpecialField.Score) // Sonuçları puan sırasına göre sıralar.
            )
        );

        int counter = 0;
        foreach (var product in searchResponse.Documents)
        {
            if (counter >= 6) { break; } // En fazla 6 ürünü yazdırması için.
            Console.WriteLine($"Product: {product.ProductName},\nPrice: {product.RegularPrice},\nStock Quantity: {product.StokQuantity}");
            counter++;
        }

    }

    public static void Main(string[] args)
    {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();

        var filePath = "products50.csv"; // CSV dosyasının yolu
        var products = ReadCsv(filePath); // CSV dosyasını okur
        
        var client = CreateElasticClient(); // Elasticsearch istemcisini oluşturur

        DeleteProducts(client); // Elasticsearch'ten mevcut tüm ürünleri siler
        IndexProducts(client, products); // CSV'den okunan ürünleri Elasticsearch'e indeksler
        
        SearchProducts(client, "toz"); // Elasticsearch'te girilen kelimeyi arar -------------------------------------------------------------
        stopwatch.Stop();
        Console.WriteLine($"Search completed in {stopwatch.ElapsedMilliseconds} ms");

    }
}
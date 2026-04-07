using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TradingBot.Models;

public class GateApiClient
{
    private readonly string _apiKey;
    private readonly string _apiSecret;
    private readonly HttpClient _httpClient;

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    public GateApiClient(string apiKey, string apiSecret)
    {
        _apiKey = apiKey;
        _apiSecret = apiSecret;
        _httpClient = new HttpClient();
    }

    // Подпись для REST API
    private string SignRestRequest(string method, string path, string query, string body, long ts)
    {
        using var sha512 = SHA512.Create();
        byte[] bodyHash = sha512.ComputeHash(Encoding.UTF8.GetBytes(body ?? ""));
        string bodyHex = Convert.ToHexString(bodyHash).ToLower();
        string message = $"{method}\n{path}\n{query}\n{bodyHex}\n{ts}";
        using var hmac = new HMACSHA512(Encoding.UTF8.GetBytes(_apiSecret));
        byte[] hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(message));
        return Convert.ToHexString(hash).ToLower();
    }

    // Получить баланс
    public async Task<decimal> GetBalanceAsync(string asset = "USDT")
    {
        string path = "/api/v4/futures/usdt/accounts";
        long ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        string sign = SignRestRequest("GET", path, "", "", ts);

        var request = new HttpRequestMessage(HttpMethod.Get, $"https://api.gateio.ws{path}");
        request.Headers.Add("KEY", _apiKey);
        request.Headers.Add("SIGN", sign);
        request.Headers.Add("Timestamp", ts.ToString());

        var response = await _httpClient.SendAsync(request);
        if (!response.IsSuccessStatusCode)
            throw new Exception($"Gate balance request failed: {response.StatusCode}");

        var json = await response.Content.ReadAsStringAsync();
        Log.Info($"[Gate Balance JSON] {json}");
        var data = JsonSerializer.Deserialize<GateBalanceResponse>(json, _jsonOptions);
        return data?.Total ?? 0;
    }

    // Открыть маркет ордер
    public async Task<GateOrderResult> PlaceMarketOrderAsync(string symbol, bool isBuy, decimal quantity)
    {
        string path = "/api/v4/futures/usdt/orders";
        long ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        var order = new
        {
            contract = symbol,
            size = isBuy ? (long)quantity : -(long)quantity,
            price = "0",
            tif = "ioc"
        };

        string body = JsonSerializer.Serialize(order);
        string sign = SignRestRequest("POST", path, "", body, ts);

        var request = new HttpRequestMessage(HttpMethod.Post, $"https://api.gateio.ws{path}")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        request.Headers.Add("KEY", _apiKey);
        request.Headers.Add("SIGN", sign);
        request.Headers.Add("Timestamp", ts.ToString());

        var response = await _httpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();

        if (response.IsSuccessStatusCode)
        {
            var result = JsonSerializer.Deserialize<GateOrderResponse>(json, _jsonOptions);
            return new GateOrderResult
            {
                Success = true,
                OrderId = result?.Id.ToString(),
                Symbol = symbol,
                Status = result?.Status
            };
        }
        else
        {
            return new GateOrderResult
            {
                Success = false,
                ErrorMessage = json
            };
        }
    }

    // Закрыть позицию
    public async Task<GateOrderResult> ClosePositionAsync(string symbol, bool isLong)
    {
        string path = "/api/v4/futures/usdt/orders";
        long ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        var order = new
        {
            contract = symbol,
            size = 0,
            price = "0",
            tif = "ioc",
            close = true,
            auto_size = isLong ? "close_long" : "close_short"
        };

        string body = JsonSerializer.Serialize(order);
        string sign = SignRestRequest("POST", path, "", body, ts);

        var request = new HttpRequestMessage(HttpMethod.Post, $"https://api.gateio.ws{path}")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        request.Headers.Add("KEY", _apiKey);
        request.Headers.Add("SIGN", sign);
        request.Headers.Add("Timestamp", ts.ToString());

        var response = await _httpClient.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();

        if (response.IsSuccessStatusCode)
        {
            var result = JsonSerializer.Deserialize<GateOrderResponse>(json, _jsonOptions);
            return new GateOrderResult
            {
                Success = true,
                OrderId = result?.Id.ToString(),
                Symbol = symbol,
                Status = result?.Status
            };
        }
        else
        {
            return new GateOrderResult
            {
                Success = false,
                ErrorMessage = json
            };
        }
    }

    // Получить статус ордера
    public async Task<OrderStatusInfo> GetOrderStatusAsync(string symbol, string orderId)
    {
        string path = $"/api/v4/futures/usdt/orders/{orderId}";
        long ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        string sign = SignRestRequest("GET", path, "", "", ts);

        var request = new HttpRequestMessage(HttpMethod.Get, $"https://api.gateio.ws{path}");
        request.Headers.Add("KEY", _apiKey);
        request.Headers.Add("SIGN", sign);
        request.Headers.Add("Timestamp", ts.ToString());

        var response = await _httpClient.SendAsync(request);
        if (!response.IsSuccessStatusCode)
            return new OrderStatusInfo { Status = "Unknown" };

        var json = await response.Content.ReadAsStringAsync();
        var order = JsonSerializer.Deserialize<GateOrderResponse>(json, _jsonOptions);

        return new OrderStatusInfo
        {
            Status = order?.Status ?? "Unknown",
            IsFilled = order?.Status == "finished",
            ExecutedQty = order?.SizeExecuted ?? 0
        };
    }

    // Модели ответов
    public void Dispose()
    {
        _httpClient?.Dispose();
    }

    // Модели ответов
    private class GateBalanceResponse
    {
        public decimal Total { get; set; }
    }

    private class GateOrderResponse
    {
        public long Id { get; set; }
        public string Status { get; set; } = string.Empty;
        public decimal SizeExecuted { get; set; }
    }
}
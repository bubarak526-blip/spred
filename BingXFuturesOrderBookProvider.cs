using BingX.Net.Clients;
using BingX.Net.Objects.Models;
using CryptoExchange.Net.Objects.Sockets;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TradingBot.Models;

public sealed class BingXFuturesOrderBookProvider : BaseOrderBookProvider
{
    private readonly List<BingXWebSocketClient> _wsClients = new();
    // ✅ ПРАВИЛЬНО: Словарь для отслеживания подписок на каждом клиенте
    private readonly Dictionary<BingXWebSocketClient, int> _clientSubscriptionCounts = new();
    private readonly SemaphoreSlim _clientCreationLock = new(1);
    
    // 🔥 Увеличено с 50 до 100 (безопасно согласно API тестам)
    private const int MaxSubscriptionsPerClient = 100;
    private const int MaxClients = 15;  // Максимум 15 клиентов (15*100 = 1500+ подписок)
    
    public BingXFuturesOrderBookProvider()
    {
    }

    protected override Task<(bool Success, string? ErrorMessage)> SubscribeToSymbolAsync(string symbol, CancellationToken ct) 
        => Task.FromResult<(bool, string?)>((true, null));

    public override async Task StartAsync(IEnumerable<string> symbols, CancellationToken ct)
    {
        // 1. Сохраняем список символов
        var symbolList = symbols.ToList();
        _expectedSymbolCount = symbolList.Count;  // 🔥 КРИТИЧНО: устанавливаем количество ожидаемых символов для отслеживания готовности
        _desiredSymbols.Clear();
        foreach(var s in symbolList) 
        {
            var normalized = s.Replace("-", "").ToUpperInvariant();
            _desiredSymbols[normalized] = 0;
        }

        Log.Info($"[BingX] Начинаем подписку на {symbolList.Count} символов...");
        
        // 2. Рассчитываем требуемое количество клиентов
        int requiredClients = (int)Math.Ceiling((double)symbolList.Count / MaxSubscriptionsPerClient);
        Log.Info($"[BingX] Требуется {requiredClients} WebSocket клиентов (макс {MaxSubscriptionsPerClient} подписок на клиент)");
        
        // 3. Создаем и подключаем клиентов (асинхронно)
        var clientCreationTasks = new List<Task>();
        for (int i = 0; i < Math.Min(requiredClients, MaxClients); i++)
        {
            clientCreationTasks.Add(CreateAndConnectClientAsync(ct));
        }
        await Task.WhenAll(clientCreationTasks);
        
        Log.Info($"[BingX] ✓ Создано {_wsClients.Count} WebSocket клиентов, начинаем подписку на символы...");
        
        // 4. Подписываемся на символы с правильным load-balancing
        int successCount = 0;
        int errorCount = 0;
        var tasks = new List<Task>();
        var semaphore = new SemaphoreSlim(10);  // 10 параллельных подписок (было 15, снижаем для стабильности)
        
        foreach (var symbol in symbolList)
        {
            string currentSymbol = symbol;
            var task = SubscribeToSymbolWithLoadBalanceAsync(currentSymbol, semaphore, ct);
            tasks.Add(task);
        }
        
        await Task.WhenAll(tasks);
        
        // Подсчитываем результаты из стаканов
        foreach (var symbol in symbolList)
        {
            var normalized = symbol.Replace("-", "").ToUpperInvariant();
            if (_orderBooks.ContainsKey(normalized))
                successCount++;
            else
                errorCount++;
        }
        
        Log.Info($"[BingX] ✓ Подписка завершена: {successCount} получают данные, {errorCount} ошибок (всего {_wsClients.Count} клиентов активно)");
        
        // 🔥 Ждём 3 секунды, чтобы дать WebSocket время получить первые данные
        // Если за это время не получим все данные, CheckAndSetReady не установит _ready,
        // но это нормально - Program ждёт с timeout (Config.ProviderReadyTimeoutSec)
        await Task.Delay(3000, ct);
        
        // Если до сих пор не получили некоторые символы, устанавливаем _ready = true в любом случае
        // (частичные данные лучше, чем зависание)
        _ready.TrySetResult(true);
    }

    private async Task CreateAndConnectClientAsync(CancellationToken ct)
    {
        try
        {
            var client = new BingXWebSocketClient();
            await client.ConnectAsync(ct);
            
            lock (_clientSubscriptionCounts)
            {
                _wsClients.Add(client);
                _clientSubscriptionCounts[client] = 0;
            }
            
            Log.Info($"[BingX WS] Создан и подключен клиент #{_wsClients.Count}");
        }
        catch (Exception ex)
        {
            Log.Error($"[BingX WS] Ошибка подключения клиента: {ex.Message}", ex);
        }
    }

    private async Task SubscribeToSymbolWithLoadBalanceAsync(
        string symbol, 
        SemaphoreSlim semaphore,
        CancellationToken ct)
    {
        await semaphore.WaitAsync(ct);
        try
        {
            // 🔥 Небольшая задержка для избежания rate-limit на BingX сервере
            await Task.Delay(10, ct);
            
            BingXWebSocketClient chosenClient;
            
            // ✅ ПРАВИЛЬНО: Выбираем клиент с минимум подписок
            lock (_clientSubscriptionCounts)
            {
                if (_wsClients.Count == 0)
                {
                    Log.Error("[BingX] Нет доступных клиентов для подписки");
                    return;
                }
                
                // Выбираем клиент с минимум подписок
                var minClient = _clientSubscriptionCounts
                    .OrderBy(c => c.Value)
                    .First();
                
                chosenClient = minClient.Key;
                
                // Проверяем, не нужно ли создать новый клиент
                if (minClient.Value >= MaxSubscriptionsPerClient && _wsClients.Count < MaxClients)
                {
                    // Сигнал на создание нового клиента в фоне
                    _ = Task.Run(() => CreateAndConnectClientAsync(ct));
                }
                
                _clientSubscriptionCounts[chosenClient]++;
            }
            
            // Преобразуем в формат BingX: BTC-USDT
            string bingxSymbol = symbol.Contains("-") ? symbol : symbol + "-USDT";
            
            var result = await chosenClient.SubscribeAsync(bingxSymbol, orderBook =>
            {
                var normalizedSymbol = orderBook.Symbol.Replace("-", "").ToUpperInvariant();
                
                if (_desiredSymbols.ContainsKey(normalizedSymbol) && orderBook.Bids.Count > 0 && orderBook.Asks.Count > 0)
                {
                    var bidLevels = orderBook.Bids.Select(b => new OrderBookLevel { Price = b.Price, Quantity = b.Quantity }).ToList();
                    var askLevels = orderBook.Asks.Select(a => new OrderBookLevel { Price = a.Price, Quantity = a.Quantity }).ToList();
                    _orderBooks[normalizedSymbol] = new OrderBookData(bidLevels, askLevels, DateTime.UtcNow);
                    CheckAndSetReady(normalizedSymbol);
                }
            }, ct);
            
            if (!result.Success)
            {
                // Уменьшаем счетчик если ошибка
                lock (_clientSubscriptionCounts)
                {
                    _clientSubscriptionCounts[chosenClient]--;
                }
                
                Log.Warn($"[BingX] Ошибка подписки на {bingxSymbol}: {result.Error}");
            }
        }
        catch (Exception ex)
        {
            Log.Error($"[BingX] Исключение при подписке на {symbol}: {ex.Message}", ex);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private void HandleOrderBookUpdate(string symbol, DataEvent<BingXOrderBook> data)
    {
        if (data.Data is null)
            return;

        var bidLevels = data.Data.Bids?.Select(b => new OrderBookLevel { Price = b.Price, Quantity = b.Quantity }).ToList() ?? new();
        var askLevels = data.Data.Asks?.Select(a => new OrderBookLevel { Price = a.Price, Quantity = a.Quantity }).ToList() ?? new();

        _orderBooks[symbol] = new OrderBookData(bidLevels, askLevels, DateTime.UtcNow);
        CheckAndSetReady(symbol);
        RaiseOrderBookUpdated(symbol);
    }

    private void MarkSymbolUnavailable(string symbol)
    {
        lock (_receivedSymbols)
        {
            if (_desiredSymbols.TryRemove(symbol, out _))
            {
                if (_expectedSymbolCount > 0)
                    _expectedSymbolCount--;

                if (_receivedSymbols.Count >= _expectedSymbolCount)
                    _ready.TrySetResult(true);
            }
        }
    }

    public override void Dispose()
    {
        foreach (var wsClient in _wsClients)
            wsClient?.Dispose();
    }
}

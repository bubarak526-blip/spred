using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TradingBot.External;
using TradingBot.Models;

public class SpreadAnalyzer
{
    // Провайдеры стаканов
    private readonly IEnumerable<BingXFuturesOrderBookProvider> _bingXProviders;
    private readonly GateFuturesOrderBookProvider _gateProvider;
    
    // Клиенты API
    private readonly BingXApiClient _bingXApiClient;
    private readonly GateApiClient _gateApiClient;
    
    // Состояние открытых позиций и спредов
    private readonly ConcurrentDictionary<string, SpreadState> _activePositions;
    private readonly ConcurrentDictionary<string, PendingSpread> _pendingSpreads;
    
    // Параметры
    private readonly decimal _totalOrderSize;
    private readonly decimal _minimumSpreadPercent;
    private readonly decimal _feePercent;
    private readonly int _maxOpenSpreads;

    public SpreadAnalyzer(
        IEnumerable<BingXFuturesOrderBookProvider> bingXProviders,
        GateFuturesOrderBookProvider gateProvider,
        BingXApiClient bingXApiClient,
        GateApiClient gateApiClient,
        decimal totalOrderSize,
        decimal minimumSpreadPercent,
        decimal feePercent,
        int maxOpenSpreads)
    {
        _bingXProviders = bingXProviders;
        _gateProvider = gateProvider;
        _bingXApiClient = bingXApiClient;
        _gateApiClient = gateApiClient;
        
        _totalOrderSize = totalOrderSize;
        _minimumSpreadPercent = minimumSpreadPercent;
        _feePercent = feePercent;
        _maxOpenSpreads = maxOpenSpreads;
        
        _activePositions = new ConcurrentDictionary<string, SpreadState>(StringComparer.OrdinalIgnoreCase);
        _pendingSpreads = new ConcurrentDictionary<string, PendingSpread>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Анализирует спреды между всеми парами бирж для all symbols
    /// </summary>
    public Task<List<SpreadAnalysis>> AnalyzeSpreadsAsync()
    {
        return Task.FromResult(new List<SpreadAnalysis>());
    }

    private List<SpreadAnalysis> AnalyzeSpreadsForSymbol(CommonSymbol symbol)
    {
        var results = new List<SpreadAnalysis>();

        // BingX vs Gate
        var bingXGate = AnalyzePairAsync(symbol, ExchangeType.BingX, ExchangeType.Gate);
        if (bingXGate != null) results.Add(bingXGate);

        return results;
    }

    /// <summary>
    /// Анализирует спреды между всеми парами бирж для данного символа
    /// </summary>
    public Task<List<SpreadAnalysis>> AnalyzeSpreadsAsync(CommonSymbol symbol)
    {
        return Task.FromResult(AnalyzeSpreadsForSymbol(symbol));
    }

    private SpreadAnalysis? AnalyzePairAsync(CommonSymbol symbol, ExchangeType buyExchange, ExchangeType sellExchange)
    {
        try
        {
            // Получаем стаканы
            var buyOrderBook = GetOrderBook(buyExchange, symbol);
            var sellOrderBook = GetOrderBook(sellExchange, symbol);

            if (buyOrderBook == null || sellOrderBook == null ||
                buyOrderBook.Asks.Count == 0 || sellOrderBook.Bids.Count == 0)
                return null;

            // Проверяем свежесть данных
            var now = DateTime.UtcNow;
            var buyAge = (now - buyOrderBook.Timestamp).TotalMilliseconds;
            var sellAge = (now - sellOrderBook.Timestamp).TotalMilliseconds;

            if (buyAge > 1500 || sellAge > 1500)
                return null;

            // Рассчитываем спред: купить на buyExchange (ask), продать на sellExchange (bid)
            decimal buyPrice = buyOrderBook.Asks[0].Price;
            decimal sellPrice = sellOrderBook.Bids[0].Price;
            decimal spread = (sellPrice - buyPrice) / buyPrice * 100;

            if (spread < _minimumSpreadPercent)
                return null;

            // Рассчитываем средневзвешенные цены
            var buyAvgPrice = GetAveragePriceFromBook(buyOrderBook.Asks, _totalOrderSize);
            var sellAvgPrice = GetAveragePriceFromBook(sellOrderBook.Bids, _totalOrderSize);

            if (buyAvgPrice <= 0 || sellAvgPrice <= 0)
                return null;

            // Определяем направление
            var direction = (buyExchange, sellExchange) switch
            {
                (ExchangeType.BingX, ExchangeType.Gate) => SpreadDirection.BingXToGate,
                (ExchangeType.Gate, ExchangeType.BingX) => SpreadDirection.GateToBingX,
                _ => SpreadDirection.BingXToGate
            };

            // Проверяем ликвидность
            var buyLiquidity = GetLiquidity(buyOrderBook.Asks, _totalOrderSize);
            var sellLiquidity = GetLiquidity(sellOrderBook.Bids, _totalOrderSize);

            bool hasLiquidity = buyLiquidity >= _totalOrderSize && sellLiquidity >= _totalOrderSize;

            return new SpreadAnalysis
            {
                Symbol = symbol.Name,
                BingXSymbol = symbol.BingXSymbol,
                GateSymbol = symbol.GateSymbol,
                Direction = direction,
                BuyPrice = buyAvgPrice,
                SellPrice = sellAvgPrice,
                BingXPrice = buyExchange == ExchangeType.BingX ? buyAvgPrice : sellExchange == ExchangeType.BingX ? sellAvgPrice : 0,
                GatePrice = buyExchange == ExchangeType.Gate ? buyAvgPrice : sellExchange == ExchangeType.Gate ? sellAvgPrice : 0,
                RawSpread = spread,
                RawPercent = spread,
                NetPercent = spread - _feePercent * 2,
                OrderQtyCoins = _totalOrderSize / buyAvgPrice,
                HasLiquidity = hasLiquidity,
                Timestamp = DateTime.UtcNow
            };
        }
        catch (Exception ex)
        {
            Log.Debug($"[ANALYZE] Error analyzing {buyExchange} vs {sellExchange} for {symbol.Name}: {ex.Message}");
            return null;
        }
    }

    private OrderBookData? GetOrderBook(ExchangeType exchange, CommonSymbol symbol)
    {
        return exchange switch
        {
            ExchangeType.BingX => _bingXProviders.Select(p => p.TryGetOrderBook(symbol.BingXSymbol.Replace("-", "").ToUpperInvariant())).FirstOrDefault(ob => ob != null),
            ExchangeType.Gate => _gateProvider.TryGetOrderBook(symbol.GateSymbol),
            _ => null
        };
    }

    /// <summary>
    /// Вычисляет среднюю цену из стакана с учетом размера заказа
    /// Алгоритм из Rust версии: get_average_price_from_book
    /// </summary>
    private decimal GetAveragePriceFromBook(List<OrderBookLevel> levels, decimal totalSize)
    {
        if (levels == null || levels.Count == 0)
            return 0m;

        decimal totalCost = 0m;
        decimal totalQuantity = 0m;

        foreach (var level in levels)
        {
            if (totalQuantity >= totalSize)
                break;

            decimal quantityToTake = Math.Min(level.Quantity, totalSize - totalQuantity);
            totalCost += level.Price * quantityToTake;
            totalQuantity += quantityToTake;
        }

        return totalQuantity > 0 ? totalCost / totalQuantity : 0m;
    }

    private decimal GetLiquidity(List<OrderBookLevel> levels, decimal totalSize)
    {
        if (levels == null || levels.Count == 0)
            return 0m;

        decimal totalQuantity = 0m;
        foreach (var level in levels)
        {
            totalQuantity += level.Quantity;
            if (totalQuantity >= totalSize)
                return totalSize;
        }

        return totalQuantity;
    }

    /// <summary>
    /// Анализирует спред в одном направлении
    /// </summary>
    private SpreadAnalysis AnalyzeSpreadDirection(
        CommonSymbol symbol,
        SpreadDirection direction,
        decimal buyPrice,
        decimal sellPrice,
        decimal orderQty)
    {
        var rawSpreadAmount = sellPrice - buyPrice;
        var rawPercent = buyPrice != 0m ? rawSpreadAmount / buyPrice * 100m : 0m;

        // Комиссии стоят в обе стороны (покупка и продажа)
        var commissionPercent = _feePercent * 2;
        
        // Чистый спред
        var netPercent = rawPercent - commissionPercent;

        return new SpreadAnalysis
        {
            Symbol = symbol.Name,
            BingXSymbol = symbol.BingXSymbol,
            Direction = direction,
            BuyPrice = buyPrice,
            SellPrice = sellPrice,
            BingXPrice = direction == SpreadDirection.BingXToGate ? sellPrice : buyPrice,
            RawSpread = rawSpreadAmount,
            RawPercent = rawPercent,
            NetPercent = netPercent,
            OrderQtyCoins = orderQty,
            HasLiquidity = true,
            Timestamp = DateTime.UtcNow
        };
    }

    /// <summary>
    /// Получает активные позиции
    /// </summary>
    public IEnumerable<SpreadState> GetActivePositions() => _activePositions.Values;

    /// <summary>
    /// Получает ожидающие спреды
    /// </summary>
    public IEnumerable<PendingSpread> GetPendingSpreads() => _pendingSpreads.Values;

    /// <summary>
    /// Закрывает позицию
    /// </summary>
    public Task<bool> CloseSpreadAsync(string symbol)
    {
        if (_activePositions.TryRemove(symbol, out var state))
        {
        // Log.Debug($"SpreadAnalyzer: Удаление позиции {symbol} из активных");
            return Task.FromResult(true);
        }

        return Task.FromResult(false);
    }

    /// <summary>
    /// Регистрирует открытую позицию для отслеживания
    /// Вызывается из Program.cs после успешного открытия в ExecutionWorker
    /// </summary>
    public void RegisterOpenPosition(SpreadAnalysis spread)
    {
        var state = new SpreadState
        {
            Spread = spread,
            OpenedAt = DateTime.UtcNow
        };
        _activePositions.TryAdd(spread.Symbol, state);
        // Log.Debug($"SpreadAnalyzer: Позиция {spread.Symbol} зарегистрирована в активных");
    }

    /// <summary>
    /// Удаляет позицию из отслеживания
    /// Вызывается из Program.cs после успешного закрытия в ExecutionWorker
    /// </summary>
    public void UnregisterPosition(string symbol)
    {
        if (_activePositions.TryRemove(symbol, out _))
        {
            // Log.Debug($"SpreadAnalyzer: Позиция {symbol} удалена из активных, готова к переанализу");
        }
    }
}

/// <summary>
/// Состояние открытой позиции
/// </summary>
public class SpreadState
{
    public SpreadAnalysis Spread { get; set; } = null!;
    public DateTime OpenedAt { get; set; }
    public string BingXOrderId { get; set; } = string.Empty;
    public string GateOrderId { get; set; } = string.Empty;
}

/// <summary>
/// Ожидающий спред
/// </summary>
public class PendingSpread
{
    public SpreadAnalysis Spread { get; set; } = null!;
    public DateTime CreatedAt { get; set; }
}

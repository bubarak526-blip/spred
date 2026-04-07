using System;
using System.Collections.Generic;

namespace TradingBot.Models
{
    // Типы бирж
    public enum ExchangeType
    {
        BingX,
        Gate
    }

    // Направление арбитража
    public enum SpreadDirection
    {
        BingXToGate,     // Купили на BingX, Продали на Gate
        GateToBingX      // Купили на Gate, Продали на BingX
    }

    // Общая модель торговой пары
    public class CommonSymbol
    {
        public string Name { get; set; } = string.Empty;
        public string BingXSymbol { get; set; } = string.Empty;
        public string GateSymbol { get; set; } = string.Empty;
        
        // Правила округления (из Rust-версии)
        public decimal? BingXStepSize { get; set; }
        public decimal? BingXTickSize { get; set; }
        public decimal? GateStepSize { get; set; }
        public decimal? GateTickSize { get; set; }
    }

    // Данные стакана (OrderBook)
    public class OrderBookLevel
    {
        public decimal Price { get; set; }
        public decimal Quantity { get; set; }
    }

    public sealed record OrderBookData(List<OrderBookLevel> Bids, List<OrderBookLevel> Asks, DateTime Timestamp);

    // Результат анализа спреда
    public class SpreadAnalysis
    {
        public string Symbol { get; set; } = string.Empty;
        public string BingXSymbol { get; set; } = string.Empty;
        public string GateSymbol { get; set; } = string.Empty;
        public SpreadDirection Direction { get; set; }
        
        public decimal BuyPrice { get; set; }
        public decimal SellPrice { get; set; }
        public decimal BingXPrice { get; set; }
        public decimal GatePrice { get; set; }
        
        public decimal RawSpread { get; set; }
        public decimal RawPercent { get; set; }
        public decimal NetPercent { get; set; } // Чистая прибыль %
        
        public decimal OrderQtyCoins { get; set; }
        public bool HasLiquidity { get; set; }
        
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;

        // Для удобства отображения в таблице Terminal.Gui
        public string DirectionLabel => Direction switch
        {
            SpreadDirection.BingXToGate => "X -> G",
            SpreadDirection.GateToBingX => "G -> X",
            _ => "Unknown"
        };
    }

    // Состояние баланса
    public class WalletBalance
    {
        public ExchangeType Exchange { get; set; }
        public decimal Total { get; set; }
        public decimal Available { get; set; }
    }

    // Информация о выполненном ордере
    public class OrderExecution
    {
        public string OrderId { get; set; } = string.Empty;
        public ExchangeType Exchange { get; set; }
        public string Symbol { get; set; } = string.Empty;
        
        public bool IsBuy { get; set; }
        public decimal Price { get; set; }
        public decimal Quantity { get; set; }
        
        public decimal Commission { get; set; }
        public DateTime ExecutedTime { get; set; } = DateTime.UtcNow;
        
        public OrderStatus Status { get; set; } = OrderStatus.Filled;
    }

    // Статус заказа
    public enum OrderStatus
    {
        Pending,
        Filled,
        PartiallyFilled,
        Cancelled,
        Rejected
    }

    // Статистика позиции для закрытия
    public class PositionMetrics
    {
        public decimal EntryPrice { get; set; }
        public decimal CurrentPrice { get; set; }
        public decimal Quantity { get; set; }
        
        public decimal UnrealizedPnL { get; set; }
        public decimal UnrealizedPnLPercent { get; set; }
        
        public TimeSpan DurationOpen { get; set; }
        
        public bool IsBreakeven => Math.Abs(UnrealizedPnLPercent) < 0.01m;
        public bool IsProfit => UnrealizedPnLPercent > 0;
    }

    // Статистика по выполненным спредам
    public class ClosedSpreadStats
    {
        public string Symbol { get; set; } = string.Empty;
        public SpreadDirection Direction { get; set; }
        
        public DateTime OpenTime { get; set; }
        public DateTime CloseTime { get; set; }
        
        public decimal EntrySpread { get; set; }
        public decimal ExitSpread { get; set; }
        public decimal RealizePnL { get; set; }
        
        public TimeSpan Duration => CloseTime - OpenTime;
        public string CloseReason { get; set; } = string.Empty;
    }

    // Результат выставления маркет ордера на BingX
    public class BingXOrderResult
    {
        public bool Success { get; set; }
        public string? OrderId { get; set; }        // Реальный ID от BingX
        public string? Symbol { get; set; }
        public string? Status { get; set; }         // NEW, FILLED, PARTIALLY_FILLED, CANCELED, REJECTED
        public string? ErrorMessage { get; set; }
    }

    // Результат выставления маркет ордера на Gate
    public class GateOrderResult
    {
        public bool Success { get; set; }
        public string? OrderId { get; set; }        // Реальный ID от Gate
        public string? Symbol { get; set; }
        public string? Status { get; set; }         // NEW, FILLED, PARTIALLY_FILLED, CANCELED, REJECTED
        public string? ErrorMessage { get; set; }
    }

    // 🔥 НОВОЕ: Критическая позиция требующая отката (deadlock recovery)
    public class CriticalPosition
    {
        public string Symbol { get; set; } = string.Empty;
        public string BingXOrderId { get; set; } = string.Empty;
        public string FailedExchange { get; set; } = string.Empty;          // BINGX или GATE
        public decimal Quantity { get; set; }
        public SpreadDirection OriginalDirection { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        public int RetryCount { get; set; } = 0;
        public const int MaxRollbackRetries = 10;
    }

}

/// <summary>
/// Информация о статусе ордера
/// </summary>
public class OrderStatusInfo
{
    public string Status { get; set; } = "";
    public bool IsFilled { get; set; }
    public bool IsPartiallyFilled { get; set; }
    public decimal ExecutedQty { get; set; }
}

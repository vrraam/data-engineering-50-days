import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

# Sample stock symbols
stocks = ['AAPL', 'GOOGL', 'TSLA', 'MSFT', 'AMZN']

# Base prices for each stock
base_prices = {
    'AAPL': 180.00,
    'GOOGL': 2750.00,
    'TSLA': 250.00,
    'MSFT': 380.00,
    'AMZN': 3300.00
}

def generate_stock_price(symbol, base_price):
    """Generate a realistic stock price update"""
    # Random price change between -2% and +2%
    change_percent = random.uniform(-0.02, 0.02)
    new_price = base_price * (1 + change_percent)
    
    return {
        "eventId": f"price-update-{symbol}-{int(time.time())}",
        "eventType": "stock.price.update",
        "timestamp": datetime.now().isoformat(),
        "symbol": symbol,
        "price": {
            "current": round(new_price, 2),
            "change": round(new_price - base_price, 2),
            "changePercent": round(change_percent * 100, 2)
        },
        "market": {
            "session": "regular",
            "status": "open"
        }
    }

def main():
    print("🚀 Starting Stock Price Producer...")
    print("📊 Sending stock price updates every 2 seconds")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            # Pick a random stock
            symbol = random.choice(stocks)
            base_price = base_prices[symbol]
            
            # Generate price update
            price_update = generate_stock_price(symbol, base_price)
            
            # Send to Kafka (using symbol as key for partitioning)
            producer.send(
                topic='stock-prices',
                key=symbol,
                value=price_update
            )
            
            print(f"📈 Sent: {symbol} @ ${price_update['price']['current']} "
                  f"({price_update['price']['changePercent']:+.2f}%)")
            
            # Wait 2 seconds before next update
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    finally:
        producer.close()
        print("✅ Producer closed successfully")

if __name__ == "__main__":
    main()

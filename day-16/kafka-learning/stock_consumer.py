import json
from kafka import KafkaConsumer
from collections import defaultdict
import signal
import sys

class StockPriceConsumer:
    def __init__(self):
        # Create Kafka consumer
        self.consumer = KafkaConsumer(
            'stock-prices',
            bootstrap_servers=['localhost:9092'],
            group_id='stock-analyzer',  # Consumer group
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest'  # Start from latest messages
        )
        
        # Track statistics
        self.stock_stats = defaultdict(lambda: {
            'count': 0,
            'total_change': 0.0,
            'last_price': 0.0,
            'min_price': float('inf'),
            'max_price': 0.0
        })
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        
    def process_message(self, message):
        """Process a single stock price update"""
        try:
            # Extract data
            symbol = message.key
            data = message.value
            current_price = data['price']['current']
            change_percent = data['price']['changePercent']
            
            # Update statistics
            stats = self.stock_stats[symbol]
            stats['count'] += 1
            stats['total_change'] += change_percent
            stats['last_price'] = current_price
            stats['min_price'] = min(stats['min_price'], current_price)
            stats['max_price'] = max(stats['max_price'], current_price)
            
            # Calculate average change
            avg_change = stats['total_change'] / stats['count']
            
            # Print update
            print(f"📊 {symbol}: ${current_price:,.2f} "
                  f"({change_percent:+.2f}%) | "
                  f"Updates: {stats['count']} | "
                  f"Avg Change: {avg_change:+.2f}% | "
                  f"Range: ${stats['min_price']:.2f}-${stats['max_price']:.2f}")
            
            # Alert for significant changes
            if abs(change_percent) > 1.5:
                print(f"🚨 ALERT: Large price movement in {symbol}! "
                      f"{change_percent:+.2f}%")
                
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def run(self):
        """Start consuming messages"""
        print("🔍 Starting Stock Price Consumer...")
        print("📈 Analyzing real-time stock price updates")
        print("🚨 Will alert on price changes > 1.5%")
        print("Press Ctrl+C to stop\n")
        
        try:
            for message in self.consumer:
                self.process_message(message)
                
        except Exception as e:
            print(f"❌ Consumer error: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self, signum=None, frame=None):
        """Gracefully shutdown consumer"""
        print("\n🛑 Shutting down consumer...")
        
        # Print final statistics
        if self.stock_stats:
            print("\n📊 Final Statistics:")
            print("-" * 60)
            for symbol, stats in self.stock_stats.items():
                avg_change = stats['total_change'] / stats['count'] if stats['count'] > 0 else 0
                print(f"{symbol:6}: "
                      f"${stats['last_price']:7.2f} | "
                      f"Updates: {stats['count']:3} | "
                      f"Avg: {avg_change:+6.2f}% | "
                      f"Range: ${stats['min_price']:.2f}-${stats['max_price']:.2f}")
        
        self.consumer.close()
        print("✅ Consumer closed successfully")
        sys.exit(0)

if __name__ == "__main__":
    consumer = StockPriceConsumer()
    consumer.run()

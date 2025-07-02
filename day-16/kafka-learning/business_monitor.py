import json
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from collections import defaultdict, deque

class BusinessMonitor:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'stock-prices',
            bootstrap_servers=['localhost:9092'],
            group_id='business-monitor',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest'
        )
        
        # Business metrics tracking
        self.portfolio_value = {
            'AAPL': {'shares': 100, 'last_value': 0},
            'GOOGL': {'shares': 50, 'last_value': 0},
            'TSLA': {'shares': 75, 'last_value': 0},
            'MSFT': {'shares': 120, 'last_value': 0},
            'AMZN': {'shares': 30, 'last_value': 0}
        }
        
        # Risk monitoring
        self.price_history = defaultdict(lambda: deque(maxlen=10))  # Last 10 prices
        self.alerts_sent = set()
        
        # Performance tracking
        self.start_time = time.time()
        self.messages_processed = 0
        
    def calculate_portfolio_value(self):
        """Calculate total portfolio value"""
        total = sum(stock['shares'] * stock['last_value'] 
                   for stock in self.portfolio_value.values())
        return total
    
    def detect_risk_events(self, symbol, price_data):
        """Detect potential risk events"""
        current_price = price_data['current']
        change_percent = price_data['changePercent']
        
        # Add to price history
        self.price_history[symbol].append(current_price)
        
        alerts = []
        
        # Alert 1: Large single price movement
        if abs(change_percent) > 1.5:
            alert_key = f"{symbol}_large_move_{int(time.time()/60)}"  # Once per minute
            if alert_key not in self.alerts_sent:
                alerts.append(f"🚨 LARGE MOVE: {symbol} {change_percent:+.2f}%")
                self.alerts_sent.add(alert_key)
        
        # Alert 2: Volatility detection (if we have enough history)
        if len(self.price_history[symbol]) >= 5:
            prices = list(self.price_history[symbol])
            price_range = max(prices) - min(prices)
            avg_price = sum(prices) / len(prices)
            volatility = (price_range / avg_price) * 100
            
            if volatility > 3.0:  # High volatility
                alert_key = f"{symbol}_volatility_{int(time.time()/300)}"  # Once per 5 minutes
                if alert_key not in self.alerts_sent:
                    alerts.append(f"⚡ HIGH VOLATILITY: {symbol} {volatility:.1f}% range")
                    self.alerts_sent.add(alert_key)
        
        return alerts
    
    def update_dashboard(self, symbol, price_data):
        """Update business dashboard"""
        current_price = price_data['current']
        change_percent = price_data['changePercent']
        
        # Update portfolio
        if symbol in self.portfolio_value:
            self.portfolio_value[symbol]['last_value'] = current_price
        
        # Calculate metrics
        portfolio_total = self.calculate_portfolio_value()
        processing_rate = self.messages_processed / (time.time() - self.start_time)
        
        # Risk analysis
        alerts = self.detect_risk_events(symbol, price_data)
        
        # Print dashboard
        print(f"\n{'='*80}")
        print(f"📊 REAL-TIME BUSINESS DASHBOARD - {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*80}")
        
        # Latest update
        print(f"📈 Latest: {symbol} @ ${current_price:,.2f} ({change_percent:+.2f}%)")
        
        # Portfolio summary
        print(f"\n💰 PORTFOLIO VALUE: ${portfolio_total:,.2f}")
        print("   Stock    | Shares |   Price   |    Value   ")
        print("-" * 45)
        for stock, data in self.portfolio_value.items():
            if data['last_value'] > 0:
                value = data['shares'] * data['last_value']
                print(f"   {stock:8} | {data['shares']:6} | ${data['last_value']:7.2f} | ${value:8,.0f}")
        
        # Performance metrics
        print(f"\n⚡ SYSTEM PERFORMANCE:")
        print(f"   Messages Processed: {self.messages_processed:,}")
        print(f"   Processing Rate: {processing_rate:.1f} msg/sec")
        print(f"   Uptime: {int(time.time() - self.start_time)}s")
        
        # Risk alerts
        if alerts:
            print(f"\n🚨 RISK ALERTS:")
            for alert in alerts:
                print(f"   {alert}")
        else:
            print(f"\n✅ No active risk alerts")
    
    def run(self):
        """Start the business monitor"""
        print("🏢 Starting Real-Time Business Monitor...")
        print("📊 Tracking portfolio value and risk metrics")
        print("🚨 Monitoring for business-critical events")
        print("Press Ctrl+C to stop\n")
        
        try:
            for message in self.consumer:
                self.messages_processed += 1
                
                symbol = message.key
                data = message.value
                price_data = data['price']
                
                # Update dashboard every message
                self.update_dashboard(symbol, price_data)
                
                # Brief pause for readability
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n\n🛑 Shutting down Business Monitor...")
            final_value = self.calculate_portfolio_value()
            print(f"📊 Final Portfolio Value: ${final_value:,.2f}")
            print(f"📈 Total Messages Processed: {self.messages_processed:,}")
            print("✅ Monitor closed successfully")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    monitor = BusinessMonitor()
    monitor.run()

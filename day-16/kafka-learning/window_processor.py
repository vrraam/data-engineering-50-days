import json
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from collections import defaultdict, deque
import threading

class TimeWindowProcessor:
    def __init__(self, window_size_seconds=30):
        self.consumer = KafkaConsumer(
            'stock-prices',
            bootstrap_servers=['localhost:9092'],
            group_id='window-processor',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest'
        )
        
        self.window_size = window_size_seconds
        
        # Tumbling Windows: Non-overlapping time windows
        self.tumbling_windows = defaultdict(lambda: {
            'messages': [],
            'start_time': None,
            'end_time': None
        })
        
        # Sliding Windows: Overlapping windows (last N messages)
        self.sliding_windows = defaultdict(lambda: deque(maxlen=10))
        
        # Session Windows: Activity-based windows
        self.session_windows = defaultdict(lambda: {
            'messages': [],
            'last_activity': None,
            'session_start': None
        })
        
        self.session_timeout = 15  # 15 seconds of inactivity ends session
        
        # Start background window processor
        self.running = True
        self.window_thread = threading.Thread(target=self._process_windows)
        self.window_thread.daemon = True
        self.window_thread.start()
    
    def _get_window_key(self, timestamp):
        """Get window key for tumbling windows"""
        epoch = int(timestamp.timestamp())
        window_start = (epoch // self.window_size) * self.window_size
        return window_start
    
    def _process_tumbling_window(self, symbol, message, timestamp):
        """Process tumbling window (30-second non-overlapping windows)"""
        window_key = self._get_window_key(timestamp)
        window = self.tumbling_windows[f"{symbol}_{window_key}"]
        
        if window['start_time'] is None:
            window['start_time'] = datetime.fromtimestamp(window_key)
            window['end_time'] = window['start_time'] + timedelta(seconds=self.window_size)
        
        window['messages'].append(message)
        
        # Check if window is complete
        if timestamp >= window['end_time']:
            self._emit_tumbling_results(symbol, window, window_key)
            # Clear the window
            del self.tumbling_windows[f"{symbol}_{window_key}"]
    
    def _process_sliding_window(self, symbol, message):
        """Process sliding window (last 10 messages)"""
        self.sliding_windows[symbol].append(message)
        
        if len(self.sliding_windows[symbol]) >= 5:  # Calculate when we have enough data
            self._emit_sliding_results(symbol, list(self.sliding_windows[symbol]))
    
    def _process_session_window(self, symbol, message, timestamp):
        """Process session window (activity-based)"""
        session = self.session_windows[symbol]
        
        # Check if this starts a new session
        if (session['last_activity'] is None or 
            (timestamp - session['last_activity']).seconds > self.session_timeout):
            
            # Emit previous session if it exists
            if session['messages']:
                self._emit_session_results(symbol, session)
            
            # Start new session
            session['messages'] = []
            session['session_start'] = timestamp
        
        session['messages'].append(message)
        session['last_activity'] = timestamp
    
    def _emit_tumbling_results(self, symbol, window, window_key):
        """Emit results for completed tumbling window"""
        messages = window['messages']
        if not messages:
            return
            
        prices = [msg['price']['current'] for msg in messages]
        changes = [msg['price']['changePercent'] for msg in messages]
        
        avg_price = sum(prices) / len(prices)
        total_volume = len(messages)  # Number of updates as proxy for volume
        price_volatility = max(prices) - min(prices)
        avg_change = sum(changes) / len(changes)
        
        print(f"\n🕒 TUMBLING WINDOW COMPLETE ({self.window_size}s)")
        print(f"📊 {symbol} | {window['start_time'].strftime('%H:%M:%S')} - {window['end_time'].strftime('%H:%M:%S')}")
        print(f"   Avg Price: ${avg_price:.2f}")
        print(f"   Updates: {total_volume}")
        print(f"   Volatility: ${price_volatility:.2f}")
        print(f"   Avg Change: {avg_change:+.2f}%")
    
    def _emit_sliding_results(self, symbol, messages):
        """Emit results for sliding window"""
        prices = [msg['price']['current'] for msg in messages]
        
        # Calculate moving average
        moving_avg = sum(prices) / len(prices)
        latest_price = prices[-1]
        
        # Trend detection
        if len(prices) >= 2:
            recent_trend = "📈 UP" if prices[-1] > prices[-2] else "📉 DOWN"
        else:
            recent_trend = "➡️ FLAT"
        
        print(f"🔄 SLIDING WINDOW ({len(messages)} msgs): {symbol}")
        print(f"   Current: ${latest_price:.2f} | Moving Avg: ${moving_avg:.2f} | {recent_trend}")
    
    def _emit_session_results(self, symbol, session):
        """Emit results for completed session"""
        messages = session['messages']
        duration = (session['last_activity'] - session['session_start']).seconds
        
        prices = [msg['price']['current'] for msg in messages]
        session_change = ((prices[-1] - prices[0]) / prices[0]) * 100
        
        print(f"\n📅 SESSION COMPLETE: {symbol}")
        print(f"   Duration: {duration}s")
        print(f"   Updates: {len(messages)}")
        print(f"   Session Change: {session_change:+.2f}%")
        print(f"   Price Range: ${min(prices):.2f} - ${max(prices):.2f}")
    
    def _process_windows(self):
        """Background thread to process expired windows"""
        while self.running:
            current_time = datetime.now()
            
            # Check for expired sessions
            for symbol, session in list(self.session_windows.items()):
                if (session['last_activity'] and 
                    (current_time - session['last_activity']).seconds > self.session_timeout):
                    
                    if session['messages']:
                        self._emit_session_results(symbol, session)
                        session['messages'] = []
                        session['last_activity'] = None
            
            time.sleep(5)  # Check every 5 seconds
    
    def process_message(self, message):
        """Process incoming message through all window types"""
        symbol = message.key
        data = message.value
        timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')).replace(tzinfo=None)
        
        print(f"📨 Processing: {symbol} @ ${data['price']['current']:.2f}")
        
        # Process through different window types
        self._process_tumbling_window(symbol, data, timestamp)
        self._process_sliding_window(symbol, data)
        self._process_session_window(symbol, data, timestamp)
    
    def run(self):
        """Start the window processor"""
        print("🕒 Starting Time Window Stream Processor...")
        print(f"📊 Tumbling Windows: {self.window_size}s non-overlapping")
        print(f"🔄 Sliding Windows: Last 10 messages")
        print(f"📅 Session Windows: {self.session_timeout}s timeout")
        print("Press Ctrl+C to stop\n")
        
        try:
            for message in self.consumer:
                self.process_message(message)
                
        except KeyboardInterrupt:
            print("\n🛑 Shutting down Window Processor...")
            self.running = False
            print("✅ Processor closed successfully")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    processor = TimeWindowProcessor(window_size_seconds=30)
    processor.run()

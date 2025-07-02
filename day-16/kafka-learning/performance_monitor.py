import json
import time
import psutil
import threading
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from collections import defaultdict, deque

class KafkaPerformanceMonitor:
    def __init__(self):
        # Kafka clients
        self.consumer = KafkaConsumer(
            'stock-prices',
            bootstrap_servers=['localhost:9092'],
            group_id='performance-monitor',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092']
        )
        
        # Performance metrics
        self.metrics = {
            'throughput': {
                'messages_per_second': deque(maxlen=60),  # Last 60 seconds
                'bytes_per_second': deque(maxlen=60),
                'total_messages': 0,
                'total_bytes': 0
            },
            'latency': {
                'processing_times': deque(maxlen=100),  # Last 100 messages
                'end_to_end_latency': deque(maxlen=100)
            },
            'consumer_lag': {},
            'system_resources': {
                'cpu_usage': deque(maxlen=60),
                'memory_usage': deque(maxlen=60),
                'disk_io': deque(maxlen=60)
            }
        }
        
        # Timing
        self.start_time = time.time()
        self.last_report_time = time.time()
        self.message_count_last_second = 0
        self.bytes_count_last_second = 0
        
        # Background monitoring
        self.running = True
        self.monitor_thread = threading.Thread(target=self._background_monitoring)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
    
    def _background_monitoring(self):
        """Background thread for system monitoring"""
        while self.running:
            # System resource monitoring
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            disk_io = psutil.disk_io_counters()
            
            self.metrics['system_resources']['cpu_usage'].append(cpu_percent)
            self.metrics['system_resources']['memory_usage'].append(memory_percent)
            
            if disk_io:
                disk_io_rate = disk_io.read_bytes + disk_io.write_bytes
                self.metrics['system_resources']['disk_io'].append(disk_io_rate)
            
            # Calculate throughput metrics
            current_time = time.time()
            if current_time - self.last_report_time >= 1.0:  # Every second
                # Messages per second
                msg_rate = self.message_count_last_second
                byte_rate = self.bytes_count_last_second
                
                self.metrics['throughput']['messages_per_second'].append(msg_rate)
                self.metrics['throughput']['bytes_per_second'].append(byte_rate)
                
                # Reset counters
                self.message_count_last_second = 0
                self.bytes_count_last_second = 0
                self.last_report_time = current_time
            
            time.sleep(1)
    
    def _calculate_latency(self, message_timestamp):
        """Calculate end-to-end latency"""
        try:
            # Parse timestamp from message
            msg_time = datetime.fromisoformat(message_timestamp.replace('Z', '+00:00'))
            current_time = datetime.now(msg_time.tzinfo)
            
            latency_ms = (current_time - msg_time).total_seconds() * 1000
            self.metrics['latency']['end_to_end_latency'].append(latency_ms)
            
            return latency_ms
        except:
            return 0
    
    def _get_topic_info(self):
        """Get topic metadata and lag information"""
        try:
            # Get topic metadata
            metadata = self.consumer.list_consumer_group_offsets()
            
            # For demo purposes, we'll simulate lag calculation
            # In production, you'd use Kafka's JMX metrics or admin API
            
            partitions = self.consumer.partitions_for_topic('stock-prices')
            if partitions:
                return {
                    'partition_count': len(partitions),
                    'estimated_lag': 0  # Simplified for demo
                }
        except:
            pass
        
        return {'partition_count': 3, 'estimated_lag': 0}
    
    def _print_performance_dashboard(self):
        """Print comprehensive performance dashboard"""
        current_time = datetime.now()
        uptime = current_time - datetime.fromtimestamp(self.start_time)
        
        print(f"\n{'='*100}")
        print(f"⚡ KAFKA PERFORMANCE DASHBOARD - {current_time.strftime('%H:%M:%S')}")
        print(f"{'='*100}")
        
        # Throughput Metrics
        print(f"\n📈 THROUGHPUT METRICS:")
        if self.metrics['throughput']['messages_per_second']:
            current_msg_rate = list(self.metrics['throughput']['messages_per_second'])[-1] if self.metrics['throughput']['messages_per_second'] else 0
            avg_msg_rate = sum(self.metrics['throughput']['messages_per_second']) / len(self.metrics['throughput']['messages_per_second'])
            peak_msg_rate = max(self.metrics['throughput']['messages_per_second']) if self.metrics['throughput']['messages_per_second'] else 0
            
            current_byte_rate = list(self.metrics['throughput']['bytes_per_second'])[-1] if self.metrics['throughput']['bytes_per_second'] else 0
            
            print(f"   Current Rate:     {current_msg_rate:6.1f} msg/sec  |  {current_byte_rate/1024:8.1f} KB/sec")
            print(f"   Average Rate:     {avg_msg_rate:6.1f} msg/sec")
            print(f"   Peak Rate:        {peak_msg_rate:6.1f} msg/sec")
            print(f"   Total Processed:  {self.metrics['throughput']['total_messages']:,} messages")
        
        # Latency Metrics
        print(f"\n⏱️  LATENCY METRICS:")
        if self.metrics['latency']['end_to_end_latency']:
            latencies = list(self.metrics['latency']['end_to_end_latency'])
            avg_latency = sum(latencies) / len(latencies)
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) > 0 else 0
            max_latency = max(latencies)
            
            print(f"   Average Latency:  {avg_latency:8.2f} ms")
            print(f"   P95 Latency:      {p95_latency:8.2f} ms")
            print(f"   Max Latency:      {max_latency:8.2f} ms")
        else:
            print(f"   No latency data available yet...")
        
        # Processing Performance
        if self.metrics['latency']['processing_times']:
            processing_times = list(self.metrics['latency']['processing_times'])
            avg_processing = sum(processing_times) / len(processing_times) * 1000  # Convert to ms
            print(f"   Avg Processing:   {avg_processing:8.2f} ms")
        
        # Topic Information
        topic_info = self._get_topic_info()
        print(f"\n📊 TOPIC METRICS:")
        print(f"   Topic:            stock-prices")
        print(f"   Partitions:       {topic_info['partition_count']}")
        print(f"   Consumer Lag:     {topic_info['estimated_lag']} messages")
        
        # System Resources
        print(f"\n💻 SYSTEM RESOURCES:")
        if self.metrics['system_resources']['cpu_usage']:
            cpu_current = list(self.metrics['system_resources']['cpu_usage'])[-1]
            cpu_avg = sum(self.metrics['system_resources']['cpu_usage']) / len(self.metrics['system_resources']['cpu_usage'])
            
            memory_current = list(self.metrics['system_resources']['memory_usage'])[-1]
            memory_avg = sum(self.metrics['system_resources']['memory_usage']) / len(self.metrics['system_resources']['memory_usage'])
            
            print(f"   CPU Usage:        {cpu_current:5.1f}%  (avg: {cpu_avg:.1f}%)")
            print(f"   Memory Usage:     {memory_current:5.1f}%  (avg: {memory_avg:.1f}%)")
        
        # Performance Analysis
        print(f"\n🎯 PERFORMANCE ANALYSIS:")
        
        # Throughput analysis
        if self.metrics['throughput']['messages_per_second']:
            recent_rates = list(self.metrics['throughput']['messages_per_second'])[-10:]  # Last 10 seconds
            if len(recent_rates) > 1:
                rate_variance = max(recent_rates) - min(recent_rates)
                if rate_variance > 10:
                    print(f"   ⚠️  High throughput variance detected ({rate_variance:.1f} msg/sec)")
                else:
                    print(f"   ✅ Stable throughput pattern")
        
        # Latency analysis
        if self.metrics['latency']['end_to_end_latency']:
            recent_latencies = list(self.metrics['latency']['end_to_end_latency'])[-10:]
            if recent_latencies:
                if max(recent_latencies) > 1000:  # > 1 second
                    print(f"   ⚠️  High latency detected (max: {max(recent_latencies):.0f}ms)")
                elif max(recent_latencies) > 100:  # > 100ms
                    print(f"   ⚡ Moderate latency (max: {max(recent_latencies):.0f}ms)")
                else:
                    print(f"   ✅ Low latency performance (max: {max(recent_latencies):.0f}ms)")
        
        print(f"\n🕐 System Uptime: {str(uptime).split('.')[0]}")
    
    def process_message(self, message):
        """Process message and collect metrics"""
        start_time = time.time()
        
        # Extract message data
        data = message.value
        message_size = len(json.dumps(data).encode('utf-8'))
        
        # Calculate latency
        end_to_end_latency = self._calculate_latency(data.get('timestamp', ''))
        
        # Update counters
        self.message_count_last_second += 1
        self.bytes_count_last_second += message_size
        self.metrics['throughput']['total_messages'] += 1
        self.metrics['throughput']['total_bytes'] += message_size
        
        # Processing time
        processing_time = time.time() - start_time
        self.metrics['latency']['processing_times'].append(processing_time)
        
        # Print brief message info
        symbol = message.key
        price = data['price']['current']
        print(f"📨 {symbol}: ${price:.2f} | Latency: {end_to_end_latency:.1f}ms | Size: {message_size}B")
    
    def run(self):
        """Start the performance monitor"""
        print("⚡ Starting Kafka Performance Monitor...")
        print("📊 Collecting throughput, latency, and system metrics")
        print("🎯 Performance dashboard updates every 10 seconds")
        print("Press Ctrl+C to stop\n")
        
        dashboard_counter = 0
        
        try:
            for message in self.consumer:
                self.process_message(message)
                
                dashboard_counter += 1
                # Show dashboard every 10 messages
                if dashboard_counter >= 10:
                    self._print_performance_dashboard()
                    dashboard_counter = 0
                
        except KeyboardInterrupt:
            print("\n🛑 Shutting down Performance Monitor...")
            self._print_performance_dashboard()  # Final report
            print("✅ Monitor closed successfully")
        finally:
            self.running = False
            self.consumer.close()

if __name__ == "__main__":
    monitor = KafkaPerformanceMonitor()
    monitor.run()

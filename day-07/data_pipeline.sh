#!/bin/bash
# Complete Data Processing Pipeline

set -e
trap 'echo "Pipeline failed at line $LINENO"' ERR

# Configuration
SOURCE_DIR="./raw_data"
PROCESSED_DIR="./processed_data"
REPORTS_DIR="./reports"
LOG_FILE="pipeline.log"

# Create directories
mkdir -p "$SOURCE_DIR" "$PROCESSED_DIR" "$REPORTS_DIR"

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Generate sample e-commerce data
generate_sample_data() {
    log "Generating sample data"
    
    # Create transactions file
    cat > "$SOURCE_DIR/transactions.csv" << 'DATA'
date,transaction_id,customer_id,product,amount,status
2025-01-15,TXN001,CUST001,Laptop,999.99,completed
2025-01-15,TXN002,CUST002,Mouse,29.99,completed
2025-01-15,TXN003,CUST003,Keyboard,79.99,failed
2025-01-15,TXN004,CUST001,Monitor,299.99,completed
2025-01-15,TXN005,CUST004,Headphones,149.99,pending
DATA

    # Create customers file
    cat > "$SOURCE_DIR/customers.csv" << 'DATA'
customer_id,name,email,city,signup_date
CUST001,John Doe,john@email.com,New York,2024-01-01
CUST002,Jane Smith,jane@email.com,Los Angeles,2024-02-15
CUST003,Bob Johnson,bob@email.com,Chicago,2024-03-10
CUST004,Alice Brown,alice@email.com,Houston,2024-04-05
DATA

    log "Sample data generated"
}

# Data validation
validate_data() {
    log "Validating data files"
    
    for file in "$SOURCE_DIR"/*.csv; do
        if [ ! -f "$file" ]; then
            log "ERROR: File $file not found"
            exit 1
        fi
        
        line_count=$(wc -l < "$file")
        log "File $(basename "$file"): $line_count lines"
    done
    
    log "Data validation complete"
}

# Process transactions
process_transactions() {
    log "Processing transactions"
    
    # Filter completed transactions only
    (head -1 "$SOURCE_DIR/transactions.csv"; \
     awk -F',' '$6=="completed"' "$SOURCE_DIR/transactions.csv") > \
     "$PROCESSED_DIR/completed_transactions.csv"
    
    # Calculate daily totals
    awk -F',' 'NR>1 && $6=="completed" {
        sum += $5
        count++
    } END {
        printf "Total completed transactions: %d\n", count
        printf "Total revenue: $%.2f\n", sum
    }' "$SOURCE_DIR/transactions.csv" > "$REPORTS_DIR/daily_summary.txt"
    
    log "Transaction processing complete"
}

# Generate customer report
generate_customer_report() {
    log "Generating customer report"
    
    echo "=== Customer Analysis Report ===" > "$REPORTS_DIR/customer_report.txt"
    echo "Generated: $(date)" >> "$REPORTS_DIR/customer_report.txt"
    echo >> "$REPORTS_DIR/customer_report.txt"
    
    # Customer count by city
    echo "Customers by city:" >> "$REPORTS_DIR/customer_report.txt"
    awk -F',' 'NR>1 {print $4}' "$SOURCE_DIR/customers.csv" | \
        sort | uniq -c >> "$REPORTS_DIR/customer_report.txt"
    
    log "Customer report generated"
}

# Main execution
main() {
    log "Starting data pipeline"
    
    generate_sample_data
    validate_data
    process_transactions
    generate_customer_report
    
    log "Data pipeline completed successfully"
    echo "Check $REPORTS_DIR/ for results"
}

# Run pipeline
main

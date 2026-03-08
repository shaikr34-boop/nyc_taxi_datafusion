use datafusion::prelude::*;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::logical_expr::{col, count, sum, avg};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Load all parquet files in the data folder
    ctx.register_parquet("taxi", "data", ParquetReadOptions::default()).await?;

    // =====================================================
    // Aggregation 1 — DataFrame API
    // =====================================================

    println!("=== 1. Trips & Revenue by Month - DataFrame API ===");

    let df = ctx.sql("
        SELECT EXTRACT(month FROM tpep_pickup_datetime) pickup_month,
               COUNT(*) trip_count,
               SUM(total_amount) total_revenue,
               AVG(fare_amount) avg_fare
        FROM taxi
        GROUP BY pickup_month
        ORDER BY pickup_month
    ").await?;

    df.show().await?;

    // =====================================================
    // Aggregation 1 — SQL
    // =====================================================

    println!("\n=== 1. Trips & Revenue by Month - SQL ===");

    ctx.sql("
        SELECT EXTRACT(month FROM tpep_pickup_datetime) pickup_month,
               COUNT(*) trip_count,
               SUM(total_amount) total_revenue,
               AVG(fare_amount) avg_fare
        FROM taxi
        GROUP BY pickup_month
        ORDER BY pickup_month
    ")
    .await?
    .show()
    .await?;

    // =====================================================
    // Aggregation 2 — DataFrame API
    // =====================================================

    println!("\n=== 2. Tip Behavior by Payment Type - DataFrame API ===");

    let df2 = ctx.sql("
        SELECT payment_type,
               COUNT(*) trip_count,
               AVG(tip_amount) avg_tip,
               SUM(tip_amount)/SUM(total_amount) tip_rate
        FROM taxi
        GROUP BY payment_type
        ORDER BY trip_count DESC
    ").await?;

    df2.show().await?;

    // =====================================================
    // Aggregation 2 — SQL
    // =====================================================

    println!("\n=== 2. Tip Behavior by Payment Type - SQL ===");

    ctx.sql("
        SELECT payment_type,
               COUNT(*) trip_count,
               AVG(tip_amount) avg_tip,
               SUM(tip_amount)/SUM(total_amount) tip_rate
        FROM taxi
        GROUP BY payment_type
        ORDER BY trip_count DESC
    ")
    .await?
    .show()
    .await?;

    println!("\nAll aggregations completed successfully.");

    Ok(())
}
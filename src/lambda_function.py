from etl_pipeline import run_etl
from sagemaker_pipeline import invoke_endpoint
from notifications import send_notification

def lambda_handler(event, context):
    # Run ETL process
    run_etl()
    
    # Sample data for prediction (e.g., a new transaction data)
    input_data = '1001,2002,3,50.00,2024-01-15'  # Example in CSV format

    # Make prediction
    prediction = invoke_endpoint("ecommerce-prediction-endpoint", input_data)
    
    # Send notification with ETL and prediction result
    send_notification(
        message=f"ETL pipeline executed successfully! Prediction result: {prediction}",
        subject="ETL and Prediction Result",
        topic_arn="arn:aws:sns:your-region:your-account-id:YourSNSTopic"
    )
    
    return {
        'statusCode': 200,
        'body': f'ETL pipeline executed successfully! Prediction: {prediction}'
    }

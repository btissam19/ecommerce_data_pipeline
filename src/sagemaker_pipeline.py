import boto3
import json
import time

# Load SageMaker configurations
with open('../config/sagemaker_config.json') as f:
    sm_config = json.load(f)

sagemaker = boto3.client('sagemaker')

def create_training_job(job_name):
    response = sagemaker.create_training_job(
        TrainingJobName=job_name,
        AlgorithmSpecification={
            'TrainingImage': sm_config['training_image_uri'],
            'TrainingInputMode': 'File'
        },
        RoleArn=sm_config['role_arn'],
        InputDataConfig=[
            {
                'ChannelName': 'train',
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': sm_config['s3_training_data_uri'],
                        'S3DataDistributionType': 'FullyReplicated'
                    }
                },
                'ContentType': 'text/csv'
            }
        ],
        OutputDataConfig={'S3OutputPath': sm_config['s3_model_output_uri']},
        ResourceConfig={
            'InstanceType': 'ml.m4.xlarge',
            'InstanceCount': 1,
            'VolumeSizeInGB': 10
        },
        StoppingCondition={'MaxRuntimeInSeconds': 3600}
    )
    return response

def deploy_model(endpoint_name, model_name):
    response = sagemaker.create_endpoint_config(
        EndpointConfigName=model_name,
        ProductionVariants=[{
            'InstanceType': 'ml.m4.xlarge',
            'InitialInstanceCount': 1,
            'ModelName': model_name,
            'VariantName': 'AllTraffic'
        }]
    )
    sagemaker.create_endpoint(
        EndpointName=endpoint_name,
        EndpointConfigName=model_name
    )
    return response

def invoke_endpoint(endpoint_name, input_data):
    runtime = boto3.client('sagemaker-runtime')
    response = runtime.invoke_endpoint(
        EndpointName=endpoint_name,
        ContentType='text/csv',
        Body=input_data
    )
    return response['Body'].read().decode('utf-8')

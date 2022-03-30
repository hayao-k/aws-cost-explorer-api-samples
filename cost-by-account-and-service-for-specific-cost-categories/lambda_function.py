import boto3

def lambda_handler(event, context):
    ce = boto3.client('ce')
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': '2022-02-01',
            'End' :  '2022-03-01',
        },
        Granularity='MONTHLY',
        Metrics= [
            'NetUnblendedCost'
        ],
        Filter={
            'CostCategories': {
                'Key': 'Sample System Accounts',
                'Values': [
                    'Sample System Accounts',
                ]
            }
        },
        GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'LINKED_ACCOUNT'
            },
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE'
            }
       ]
    )
    print(response['ResultsByTime'][0]['Groups'])

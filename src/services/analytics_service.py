"""
Analytics Service
Core analytics logic and data operations
"""

import boto3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from decimal import Decimal
import time

class DecimalEncoder(json.JSONEncoder):
    """JSON encoder for Decimal types"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


class AnalyticsService:
    """Service for analytics operations"""
    
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.s3 = boto3.client('s3')
        self.athena = boto3.client('athena')
        
        self.events_table = self.dynamodb.Table('analytics-events')
        self.aggregations_table = self.dynamodb.Table('analytics-aggregations')
        self.bucket_name = 'serverless-analytics-data-lake'
    
    def store_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Store analytics event in DynamoDB"""
        try:
            response = self.events_table.put_item(Item=event_data)
            
            # Also store in S3 for long-term storage
            self._store_event_to_s3(event_data)
            
            return {"status": "success", "event_id": event_data["event_id"]}
        
        except Exception as e:
            raise Exception(f"Failed to store event: {str(e)}")
    
    def _store_event_to_s3(self, event_data: Dict[str, Any]):
        """Store event to S3 data lake"""
        try:
            timestamp = datetime.fromisoformat(event_data["timestamp"])
            key = f"events/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/{event_data['event_id']}.json"
            
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(event_data, cls=DecimalEncoder),
                ContentType='application/json'
            )
        except Exception as e:
            # Log but don't fail the main operation
            print(f"Warning: Failed to store event to S3: {str(e)}")
    
    def get_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get event by ID"""
        try:
            response = self.events_table.get_item(Key={"event_id": event_id})
            return response.get('Item')
        except Exception as e:
            raise Exception(f"Failed to get event: {str(e)}")
    
    def get_user_events(self, user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get events for a specific user"""
        try:
            response = self.events_table.query(
                IndexName='user_id-timestamp-index',
                KeyConditionExpression='user_id = :user_id',
                ExpressionAttributeValues={':user_id': user_id},
                Limit=limit,
                ScanIndexForward=False  # Most recent first
            )
            
            return response.get('Items', [])
        except Exception as e:
            raise Exception(f"Failed to get user events: {str(e)}")
    
    def get_analytics(self, metric: str, start_date: str, end_date: str, 
                     group_by: str = 'day') -> List[Dict[str, Any]]:
        """Get analytics data for a metric"""
        try:
            # Query aggregations table
            response = self.aggregations_table.query(
                KeyConditionExpression='metric = :metric AND #date BETWEEN :start AND :end',
                ExpressionAttributeNames={'#date': 'date'},
                ExpressionAttributeValues={
                    ':metric': metric,
                    ':start': start_date,
                    ':end': end_date
                }
            )
            
            items = response.get('Items', [])
            
            # Group by specified period
            if group_by == 'hour':
                return self._group_by_hour(items)
            elif group_by == 'day':
                return items
            elif group_by == 'week':
                return self._group_by_week(items)
            elif group_by == 'month':
                return self._group_by_month(items)
            
            return items
        
        except Exception as e:
            raise Exception(f"Failed to get analytics: {str(e)}")
    
    def _group_by_hour(self, items: List[Dict]) -> List[Dict]:
        """Group data by hour"""
        # Implementation for hourly grouping
        return items
    
    def _group_by_week(self, items: List[Dict]) -> List[Dict]:
        """Group data by week"""
        # Implementation for weekly grouping
        return items
    
    def _group_by_month(self, items: List[Dict]) -> List[Dict]:
        """Group data by month"""
        # Implementation for monthly grouping
        return items
    
    def execute_athena_query(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query using Athena"""
        try:
            # Start query execution
            response = self.athena.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={'Database': 'analytics'},
                ResultConfiguration={
                    'OutputLocation': f's3://{self.bucket_name}/athena-results/'
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            
            # Wait for query to complete
            max_attempts = 30
            for attempt in range(max_attempts):
                status_response = self.athena.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = status_response['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    # Get results
                    results_response = self.athena.get_query_results(
                        QueryExecutionId=query_execution_id,
                        MaxResults=1000
                    )
                    
                    # Parse results
                    results = self._parse_athena_results(results_response)
                    
                    return {
                        'query_execution_id': query_execution_id,
                        'status': 'success',
                        'results': results
                    }
                
                elif status in ['FAILED', 'CANCELLED']:
                    error_message = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    raise Exception(f"Query failed: {error_message}")
                
                time.sleep(1)
            
            raise Exception("Query timeout")
        
        except Exception as e:
            raise Exception(f"Failed to execute Athena query: {str(e)}")
    
    def _parse_athena_results(self, results_response: Dict) -> List[Dict]:
        """Parse Athena query results"""
        rows = results_response['ResultSet']['Rows']
        
        if not rows:
            return []
        
        # Extract column names from first row
        columns = [col['VarCharValue'] for col in rows[0]['Data']]
        
        # Parse data rows
        data = []
        for row in rows[1:]:
            row_data = {}
            for i, col in enumerate(row['Data']):
                row_data[columns[i]] = col.get('VarCharValue', '')
            data.append(row_data)
        
        return data
    
    def store_events_batch(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Store multiple events in batch"""
        successful = []
        failed = []
        
        # Process in batches of 25 (DynamoDB limit)
        batch_size = 25
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            
            try:
                with self.events_table.batch_writer() as writer:
                    for event in batch:
                        # Add metadata
                        event["timestamp"] = event.get("timestamp", datetime.now().isoformat())
                        event["event_id"] = event.get("event_id", f"{event['user_id']}_{int(datetime.now().timestamp())}")
                        
                        writer.put_item(Item=event)
                        successful.append(event["event_id"])
                        
                        # Also store to S3
                        self._store_event_to_s3(event)
            
            except Exception as e:
                for event in batch:
                    failed.append({
                        "event": event,
                        "error": str(e)
                    })
        
        return {
            "successful": successful,
            "failed": failed
        }

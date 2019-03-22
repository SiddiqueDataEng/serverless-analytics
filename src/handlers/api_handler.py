"""
API Gateway Lambda Handler
Handles REST API requests for analytics
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import boto3
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.analytics_service import AnalyticsService
from services.aggregation_service import AggregationService

logger = Logger()
tracer = Tracer()
metrics = Metrics()
app = APIGatewayRestResolver()

# Initialize services
analytics_service = AnalyticsService()
aggregation_service = AggregationService()


@app.get("/health")
@tracer.capture_method
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "serverless-analytics"
    }


@app.post("/events")
@tracer.capture_method
def create_event():
    """Create analytics event"""
    try:
        event_data = app.current_event.json_body
        
        # Validate required fields
        required_fields = ["user_id", "event_type"]
        for field in required_fields:
            if field not in event_data:
                return {"error": f"Missing required field: {field}"}, 400
        
        # Add metadata
        event_data["timestamp"] = datetime.now().isoformat()
        event_data["event_id"] = f"{event_data['user_id']}_{int(datetime.now().timestamp())}"
        
        # Store event
        result = analytics_service.store_event(event_data)
        
        # Track metric
        metrics.add_metric(name="EventsCreated", unit=MetricUnit.Count, value=1)
        
        logger.info("Event created", extra={"event_id": event_data["event_id"]})
        
        return {
            "status": "success",
            "event_id": event_data["event_id"],
            "message": "Event created successfully"
        }
    
    except Exception as e:
        logger.error(f"Error creating event: {str(e)}")
        metrics.add_metric(name="EventsCreatedErrors", unit=MetricUnit.Count, value=1)
        return {"error": str(e)}, 500


@app.get("/events/<event_id>")
@tracer.capture_method
def get_event(event_id: str):
    """Get event by ID"""
    try:
        event = analytics_service.get_event(event_id)
        
        if not event:
            return {"error": "Event not found"}, 404
        
        return event
    
    except Exception as e:
        logger.error(f"Error getting event: {str(e)}")
        return {"error": str(e)}, 500


@app.get("/analytics")
@tracer.capture_method
def get_analytics():
    """Get analytics metrics"""
    try:
        # Parse query parameters
        params = app.current_event.query_string_parameters or {}
        
        metric = params.get("metric", "page_views")
        start_date = params.get("start", (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"))
        end_date = params.get("end", datetime.now().strftime("%Y-%m-%d"))
        group_by = params.get("group_by", "day")
        
        # Get analytics data
        data = analytics_service.get_analytics(
            metric=metric,
            start_date=start_date,
            end_date=end_date,
            group_by=group_by
        )
        
        metrics.add_metric(name="AnalyticsQueries", unit=MetricUnit.Count, value=1)
        
        return {
            "metric": metric,
            "start_date": start_date,
            "end_date": end_date,
            "group_by": group_by,
            "data": data
        }
    
    except Exception as e:
        logger.error(f"Error getting analytics: {str(e)}")
        return {"error": str(e)}, 500


@app.get("/realtime/metrics")
@tracer.capture_method
def get_realtime_metrics():
    """Get real-time metrics"""
    try:
        metrics_data = aggregation_service.get_realtime_metrics()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics_data
        }
    
    except Exception as e:
        logger.error(f"Error getting realtime metrics: {str(e)}")
        return {"error": str(e)}, 500


@app.post("/query")
@tracer.capture_method
def execute_query():
    """Execute SQL query on data lake"""
    try:
        query_data = app.current_event.json_body
        
        if "sql" not in query_data:
            return {"error": "Missing required field: sql"}, 400
        
        # Execute query using Athena
        result = analytics_service.execute_athena_query(query_data["sql"])
        
        metrics.add_metric(name="AthenaQueries", unit=MetricUnit.Count, value=1)
        
        return {
            "status": "success",
            "query_execution_id": result["query_execution_id"],
            "results": result.get("results", [])
        }
    
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return {"error": str(e)}, 500


@app.get("/users/<user_id>/events")
@tracer.capture_method
def get_user_events(user_id: str):
    """Get events for specific user"""
    try:
        params = app.current_event.query_string_parameters or {}
        limit = int(params.get("limit", 100))
        
        events = analytics_service.get_user_events(user_id, limit=limit)
        
        return {
            "user_id": user_id,
            "count": len(events),
            "events": events
        }
    
    except Exception as e:
        logger.error(f"Error getting user events: {str(e)}")
        return {"error": str(e)}, 500


@app.get("/aggregations/<aggregation_type>")
@tracer.capture_method
def get_aggregation(aggregation_type: str):
    """Get pre-computed aggregations"""
    try:
        params = app.current_event.query_string_parameters or {}
        date = params.get("date", datetime.now().strftime("%Y-%m-%d"))
        
        data = aggregation_service.get_aggregation(aggregation_type, date)
        
        return {
            "aggregation_type": aggregation_type,
            "date": date,
            "data": data
        }
    
    except Exception as e:
        logger.error(f"Error getting aggregation: {str(e)}")
        return {"error": str(e)}, 500


@app.post("/batch/events")
@tracer.capture_method
def create_batch_events():
    """Create multiple events in batch"""
    try:
        batch_data = app.current_event.json_body
        
        if "events" not in batch_data:
            return {"error": "Missing required field: events"}, 400
        
        events = batch_data["events"]
        
        # Process batch
        results = analytics_service.store_events_batch(events)
        
        metrics.add_metric(name="BatchEventsCreated", unit=MetricUnit.Count, value=len(events))
        
        return {
            "status": "success",
            "processed": len(results["successful"]),
            "failed": len(results["failed"]),
            "results": results
        }
    
    except Exception as e:
        logger.error(f"Error creating batch events: {str(e)}")
        return {"error": str(e)}, 500


@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler"""
    return app.resolve(event, context)

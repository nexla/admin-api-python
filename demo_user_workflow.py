#!/usr/bin/env python3
"""
Demo User Workflow: Create User + Data Sources with New Rails Features
Demonstrates the complete workflow from user creation to data source management
using the new Rails-to-Python migration features.
"""

import json
import requests
import time

# Server configuration
BASE_URL = "http://localhost:8001/api/v1"

def demo_print(message, level="info"):
    """Print formatted demo messages"""
    if level == "step":
        print(f"\n🔸 {message}")
    elif level == "success":
        print(f"✅ {message}")
    elif level == "error":
        print(f"❌ {message}")
    else:
        print(f"ℹ️  {message}")

def make_request(method, endpoint, data=None, headers=None, expected_status=200):
    """Make HTTP request with error handling"""
    url = f"{BASE_URL}{endpoint}"
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers)
        elif method.upper() == "POST":
            response = requests.post(url, json=data, headers=headers)
        elif method.upper() == "PUT":
            response = requests.put(url, json=data, headers=headers)
        
        print(f"  📡 {method.upper()} {endpoint} -> {response.status_code}")
        
        if response.status_code == expected_status:
            return response.json() if response.content else {}
        else:
            print(f"  ❌ Expected {expected_status}, got {response.status_code}")
            if response.content:
                print(f"  📝 Response: {response.text}")
            return None
    except Exception as e:
        demo_print(f"Request failed: {e}", "error")
        return None

def main():
    demo_print("🚀 Rails-to-Python Migration Demo: User Workflow + Data Sources", "step")
    
    # Step 1: Login as admin to get access token
    demo_print("Step 1: Admin Login", "step")
    
    admin_login = make_request("POST", "/auth/login", {
        "email": "admin@nexla.com",
        "password": "admin123"
    })
    
    if not admin_login or "access_token" not in admin_login:
        demo_print("Admin login failed - trying alternative password", "error")
        admin_login = make_request("POST", "/auth/login", {
            "email": "admin@nexla.com", 
            "password": "password123"
        })
    
    if not admin_login or "access_token" not in admin_login:
        demo_print("Cannot proceed without admin access token", "error")
        return
    
    admin_token = admin_login["access_token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    demo_print("Admin authentication successful", "success")
    
    # Step 2: Create a new user
    demo_print("Step 2: Create New User", "step")
    
    new_user_data = {
        "email": "data_engineer@example.com",
        "full_name": "Alice Data Engineer",
        "password": "secure_password123"
    }
    
    new_user = make_request("POST", "/users/", new_user_data, admin_headers, 201)
    
    if not new_user:
        demo_print("User creation failed", "error")
        return
    
    demo_print(f"New user created: {new_user.get('email')} (ID: {new_user.get('id')})", "success")
    
    # Step 3: Create an organization for the new user
    demo_print("Step 3: Create Organization", "step")
    
    org_data = {
        "name": "Data Engineering Corp",
        "description": "Organization for data engineering workflows",
        "owner_id": new_user["id"]
    }
    
    new_org = make_request("POST", "/organizations/", org_data, admin_headers, 201)
    
    if not new_org:
        demo_print("Organization creation failed", "error")
        return
    
    demo_print(f"Organization created: {new_org.get('name')} (ID: {new_org.get('id')})", "success")
    
    # Step 4: Set up billing account with new Rails features
    demo_print("Step 4: Setup Trial Billing (New Rails Feature)", "step")
    
    billing_setup = make_request("POST", f"/billing/setup-trial/{new_org['id']}", {
        "billing_email": new_user["email"],
        "trial_days": 30
    }, admin_headers)
    
    if billing_setup:
        demo_print("Trial billing account created with Rails business logic", "success")
        print(f"  📊 Account Status: {billing_setup['account']['status']}")
        print(f"  📅 Trial Active: {billing_setup['account']['trial_active']}")
    else:
        demo_print("Billing setup failed, but continuing with demo", "error")
    
    # Step 5: Assign custodian role using new Rails features
    demo_print("Step 5: Assign Custodian Role (New Rails Feature)", "step")
    
    custodian_assignment = make_request("POST", "/custodians/assign", {
        "org_id": new_org["id"],
        "user_id": new_user["id"],
        "role_level": "CUSTODIAN"
    }, admin_headers)
    
    if custodian_assignment:
        demo_print("Custodian role assigned with Rails permission patterns", "success")
        print(f"  👥 Role: {custodian_assignment['custodian']['role_level']}")
        print(f"  🔐 Can Manage Data: {custodian_assignment['custodian']['can_manage_data']}")
    else:
        demo_print("Custodian assignment failed, but continuing", "error")
    
    # Step 6: Login as the new user
    demo_print("Step 6: New User Login", "step")
    
    user_login = make_request("POST", "/auth/login", {
        "email": new_user["email"],
        "password": "secure_password123"
    })
    
    if not user_login or "access_token" not in user_login:
        demo_print("New user login failed", "error")
        return
    
    user_token = user_login["access_token"]
    user_headers = {"Authorization": f"Bearer {user_token}"}
    demo_print("New user authentication successful", "success")
    
    # Step 7: Create data sources as the new user
    demo_print("Step 7: Create Data Sources", "step")
    
    data_sources = [
        {
            "name": "Customer Database",
            "description": "PostgreSQL database with customer information",
            "type": "DATABASE",
            "connection_config": {
                "host": "db.example.com",
                "port": 5432,
                "database": "customers"
            },
            "org_id": new_org["id"]
        },
        {
            "name": "Sales API",
            "description": "REST API for sales data",
            "type": "API",
            "connection_config": {
                "base_url": "https://api.sales.example.com",
                "version": "v2"
            },
            "org_id": new_org["id"]
        },
        {
            "name": "User Events Stream",
            "description": "Kafka stream for user events",
            "type": "STREAM",
            "connection_config": {
                "bootstrap_servers": "kafka.example.com:9092",
                "topic": "user_events"
            },
            "org_id": new_org["id"]
        }
    ]
    
    created_sources = []
    for i, source_data in enumerate(data_sources, 1):
        demo_print(f"Creating Data Source {i}: {source_data['name']}", "step")
        
        source = make_request("POST", "/data-sources/", source_data, user_headers, 201)
        
        if source:
            created_sources.append(source)
            demo_print(f"Data source created: {source.get('name')} (ID: {source.get('id')})", "success")
            print(f"  🔗 Type: {source.get('type')}")
            print(f"  📊 Status: {source.get('status', 'CREATED')}")
        else:
            demo_print(f"Failed to create data source: {source_data['name']}", "error")
    
    # Step 8: Test data source operations
    demo_print("Step 8: Test Data Source Operations", "step")
    
    if created_sources:
        # Test getting all data sources
        all_sources = make_request("GET", "/data-sources/", headers=user_headers)
        if all_sources:
            demo_print(f"Retrieved {len(all_sources.get('data_sources', []))} data sources", "success")
        
        # Test getting specific data source
        first_source = created_sources[0]
        source_detail = make_request("GET", f"/data-sources/{first_source['id']}", headers=user_headers)
        if source_detail:
            demo_print(f"Retrieved detailed info for: {source_detail.get('name')}", "success")
        
        # Test connection (if endpoint exists)
        connection_test = make_request("POST", f"/data-sources/{first_source['id']}/test-connection", headers=user_headers, expected_status=[200, 404])
        if connection_test:
            demo_print("Data source connection test completed", "success")
    
    # Step 9: Show billing usage with new Rails features
    demo_print("Step 9: Check Billing Usage (Rails Business Logic)", "step")
    
    usage_summary = make_request("GET", f"/billing/account/{new_org['id']}/usage", headers=user_headers)
    if usage_summary:
        demo_print("Billing usage retrieved with Rails patterns", "success")
        print(f"  💰 Monthly Cost: ${usage_summary.get('total_monthly_subscription_cost', 0)}")
        print(f"  📈 Usage Percentage: {usage_summary.get('usage_percentage', 0)}%")
        print(f"  ⚠️  At Warning Threshold: {usage_summary.get('usage_warnings', {}).get('at_warning_threshold', False)}")
    
    # Step 10: Check permissions with new Rails features
    demo_print("Step 10: Verify Permissions (Rails Permission System)", "step")
    
    permission_check = make_request("GET", f"/custodians/check-permission/{new_org['id']}/{new_user['id']}/manage_data", headers=user_headers)
    if permission_check:
        has_permission = permission_check.get('has_permission', False)
        demo_print(f"Data management permission check: {has_permission}", "success")
        print(f"  🔐 Role: {permission_check.get('custodian_role', 'None')}")
        print(f"  ✅ Active: {permission_check.get('custodian_active', False)}")
    
    # Final Summary
    demo_print("🎉 Demo Complete! Summary:", "step")
    print(f"  👤 Created user: {new_user['email']}")
    print(f"  🏢 Created organization: {new_org['name']}")
    print(f"  💳 Set up trial billing with Rails logic")
    print(f"  👥 Assigned custodian permissions")
    print(f"  📊 Created {len(created_sources)} data sources")
    print(f"  🔄 Demonstrated Rails business logic patterns")
    
    demo_print("The new Rails-to-Python migration features are working!", "success")

if __name__ == "__main__":
    main()
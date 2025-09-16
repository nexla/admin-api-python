# Admin API - Missing Features & Implementation TODO

## ❌ Major Gaps Identified

### 1. Missing Core Business Logic Controllers

Rails Has (Missing in Python):
- probe_controller.rb          → Data source probing/testing
- transforms_controller.rb     → Data transformations
- validators_controller.rb     → Data validation rules
- attribute_transforms_controller.rb → Field-level transforms
- catalog_configs_controller.rb → Data catalog integration
- approval_requests_controller.rb → Access approval workflow
- tags_controller.rb           → Resource tagging system
- audit_log_controller.rb      → Comprehensive audit logs

### 2. Missing Marketplace & Advanced Features

Rails Has:
- marketplace/domains_controller.rb
- marketplace/items_controller.rb
- marketplace/stats_controller.rb
- account_summary/stats_controller.rb

### 3. Missing Models & Relationships

Critical Missing Models:
- data_map.py (exists but need to check implementation)
- transforms/validators
- marketplace domain models
- approval workflow models
- tag management models
- comprehensive audit models

### 4. Missing Service Layer

Rails Services (Not in Python):
- Access control services (access/*)
- Marketplace services
- Catalog integration services
- Advanced flow orchestration
- Resource transfer services

### 5. Missing Background Jobs/Workers

Rails Has 19+ Workers:
- catalog_worker.rb
- indexing_worker.rb
- flow_delete_worker.rb
- transfer_user_resources_worker.rb
- resource_event_notification_worker.rb
- user_events_webhooks_worker.rb

### 6. Missing Advanced API Features

Missing Endpoints:
- /probe/* - Data source testing
- /transforms/* - Data transformation management
- /validators/* - Validation rule management  
- /catalog/* - Data catalog integration
- /marketplace/* - Data marketplace
- /audit_log - Detailed audit trails
- /tags - Resource tagging
- /approval_requests - Access approval workflow

## 🎯 Implementation Priority

### Phase 1 (Critical - Implement Now)
- [ ] Data Source Probing (`/probe/*` endpoints)
- [ ] Data Transformations (`/transforms/*` endpoints) 
- [ ] Background Job System (async task processing)
- [ ] Enhanced Audit Logs (`/audit_log` endpoints)

### Phase 2 (Important - Next Sprint)
- [ ] Data Validation Framework (`/validators/*` endpoints)
- [ ] Resource Tagging (`/tags/*` endpoints)
- [ ] Advanced Flow Features (triggers, dependencies)

### Phase 3 (Future - Business Expansion)
- [ ] Marketplace System (`/marketplace/*`)
- [ ] Approval Workflows (`/approval_requests/*`)
- [ ] Data Catalog Integration (`/catalog/*`)

## 📈 Current Status
- **Models**: ~85% complete (missing specialized models)
- **Basic CRUD APIs**: ~90% complete  
- **Advanced Business Logic**: ~60% complete
- **Background Processing**: ~10% complete
- **Enterprise Features**: ~40% complete
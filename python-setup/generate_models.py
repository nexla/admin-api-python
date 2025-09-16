#!/usr/bin/env python3
"""
Generate SQLAlchemy models from existing MySQL database.
This script connects to your existing database and auto-generates
Python models for the FastAPI application.
"""

import os
import sys
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.schema import MetaData
import re
from typing import Dict, List, Any

# Database connection
DATABASE_URL = "mysql+pymysql://root:nexla123@localhost:3306/nexla_admin_dev"

def to_python_class_name(table_name: str) -> str:
    """Convert table name to Python class name."""
    # Handle special cases
    if table_name.endswith('_data'):
        table_name = table_name[:-5]  # Remove _data suffix
    
    # Convert snake_case to CamelCase
    parts = table_name.split('_')
    return ''.join(word.capitalize() for word in parts)

def to_python_attribute_name(column_name: str) -> str:
    """Convert column name to Python attribute name."""
    # Keep snake_case for Python attributes
    return column_name

def get_python_type(column_type: str, column_info: dict) -> str:
    """Convert MySQL column type to SQLAlchemy Python type."""
    type_str = str(column_type).upper()
    
    # Handle common MySQL types
    if 'INT' in type_str:
        return 'Integer'
    elif 'VARCHAR' in type_str or 'CHAR' in type_str:
        if 'VARCHAR(255)' in type_str:
            return 'String(255)'
        else:
            # Extract length
            match = re.search(r'\((\d+)\)', type_str)
            if match:
                return f'String({match.group(1)})'
            return 'String(255)'
    elif 'TEXT' in type_str or 'LONGTEXT' in type_str:
        return 'Text'
    elif 'JSON' in type_str:
        return 'JSON'
    elif 'DATETIME' in type_str or 'TIMESTAMP' in type_str:
        return 'DateTime'
    elif 'DATE' in type_str:
        return 'Date'
    elif 'TIME' in type_str:
        return 'Time'
    elif 'BOOLEAN' in type_str or 'TINYINT(1)' in type_str:
        return 'Boolean'
    elif 'DECIMAL' in type_str or 'NUMERIC' in type_str:
        return 'Numeric'
    elif 'FLOAT' in type_str:
        return 'Float'
    elif 'DOUBLE' in type_str:
        return 'Float'
    elif 'ENUM' in type_str:
        return 'String(50)'  # Simplify enums for now
    else:
        print(f"Unknown type: {type_str} for column {column_info.get('name', 'unknown')}")
        return 'String(255)'

def get_foreign_keys(inspector, table_name: str) -> List[Dict]:
    """Get foreign key information for a table."""
    try:
        return inspector.get_foreign_keys(table_name)
    except Exception as e:
        print(f"Error getting foreign keys for {table_name}: {e}")
        return []

def get_indexes(inspector, table_name: str) -> List[Dict]:
    """Get index information for a table."""
    try:
        return inspector.get_indexes(table_name)
    except Exception as e:
        print(f"Error getting indexes for {table_name}: {e}")
        return []

def generate_model_file(table_name: str, columns: List[Dict], 
                       foreign_keys: List[Dict], indexes: List[Dict]) -> str:
    """Generate a complete SQLAlchemy model file."""
    
    class_name = to_python_class_name(table_name)
    
    # Start building the model
    lines = [
        f'"""',
        f'{class_name} model - Generated from {table_name} table',
        f'"""',
        f'from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, JSON, Numeric, Float, Date, Time',
        f'from sqlalchemy.orm import relationship',
        f'from sqlalchemy.sql import func',
        f'from typing import Optional',
        f'from datetime import datetime',
        f'from ..database import Base',
        f'',
        f'',
        f'class {class_name}(Base):',
        f'    __tablename__ = "{table_name}"',
        f'    ',
    ]
    
    # Add columns
    for column in columns:
        col_name = column['name']
        col_type = get_python_type(column['type'], column)
        nullable = column['nullable']
        default = column['default']
        
        # Build column definition
        col_def_parts = [f"Column({col_type}"]
        
        # Handle primary key
        if column.get('primary_key', False):
            col_def_parts.append("primary_key=True")
            if 'id' in col_name.lower():
                col_def_parts.append("index=True")
        
        # Handle foreign keys
        fk_constraint = None
        for fk in foreign_keys:
            if col_name in fk['constrained_columns']:
                ref_table = fk['referred_table']
                ref_col = fk['referred_columns'][0]
                fk_constraint = f'ForeignKey("{ref_table}.{ref_col}")'
                break
        
        if fk_constraint:
            col_def_parts.insert(1, fk_constraint)  # Insert after type
        
        # Handle nullable
        if not nullable and not column.get('primary_key', False):
            col_def_parts.append("nullable=False")
        
        # Handle defaults
        if default is not None:
            if isinstance(default, str):
                if default.upper() in ['CURRENT_TIMESTAMP', 'NOW()']:
                    col_def_parts.append("server_default=func.now()")
                else:
                    col_def_parts.append(f'default="{default}"')
            else:
                col_def_parts.append(f"default={default}")
        
        # Handle special timestamp columns
        if col_name in ['created_at', 'updated_at']:
            if col_name == 'created_at':
                col_def_parts.append("server_default=func.now()")
            elif col_name == 'updated_at':
                col_def_parts.append("server_default=func.now()")
                col_def_parts.append("onupdate=func.now()")
        
        # Handle indexes
        if any(col_name in idx['column_names'] for idx in indexes if idx.get('unique', False)):
            col_def_parts.append("unique=True")
        elif any(col_name in idx['column_names'] for idx in indexes):
            col_def_parts.append("index=True")
        
        col_definition = f"    {col_name} = {', '.join(col_def_parts)})"
        lines.append(col_definition)
    
    lines.append('')
    
    # Add relationships (basic inference)
    relationships = []
    for fk in foreign_keys:
        if fk['constrained_columns']:
            col_name = fk['constrained_columns'][0]
            ref_table = fk['referred_table']
            ref_class = to_python_class_name(ref_table)
            
            # Infer relationship name (remove _id suffix if present)
            rel_name = col_name
            if rel_name.endswith('_id'):
                rel_name = rel_name[:-3]
            
            relationships.append(f'    {rel_name} = relationship("{ref_class}")')
    
    if relationships:
        lines.append('    # Relationships')
        lines.extend(relationships)
        lines.append('')
    
    # Add helper methods
    lines.extend([
        '    def __repr__(self):',
        f'        return f"<{class_name}({{self.id if hasattr(self, \'id\') else \'no-id\'}})"',
        '',
    ])
    
    # Add common methods for timestamp models
    if any(col['name'] in ['created_at', 'updated_at'] for col in columns):
        lines.extend([
            '    @property',
            '    def created_recently(self) -> bool:',
            '        """Check if created within last 24 hours."""',
            '        if not hasattr(self, "created_at") or not self.created_at:',
            '            return False',
            '        from datetime import datetime, timedelta',
            '        return datetime.utcnow() - self.created_at < timedelta(days=1)',
        ])
        lines.append('')
    
    # Add status methods for models with status column
    if any(col['name'] == 'status' for col in columns):
        lines.extend([
            '    def is_active(self) -> bool:',
            '        """Check if status is active."""',
            '        return getattr(self, "status", None) == "ACTIVE"',
            '',
            '    def is_deactivated(self) -> bool:',
            '        """Check if status is deactivated."""', 
            '        return getattr(self, "status", None) == "DEACTIVATED"',
        ])
        lines.append('')
    
    return '\n'.join(lines)

def main():
    """Main function to generate all models."""
    print("🚀 Generating SQLAlchemy models from existing database...")
    
    # Create database connection
    try:
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)
        print(f"✅ Connected to database")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        print("Make sure MySQL is running and accessible")
        return 1
    
    # Get all tables
    tables = inspector.get_table_names()
    print(f"📋 Found {len(tables)} tables")
    
    # Create models directory
    models_dir = "app/models"
    os.makedirs(models_dir, exist_ok=True)
    
    # Generate __init__.py
    init_imports = []
    
    # Process each table
    for table_name in sorted(tables):
        print(f"🔄 Processing table: {table_name}")
        
        try:
            # Get table information
            columns = inspector.get_columns(table_name)
            foreign_keys = get_foreign_keys(inspector, table_name)
            indexes = get_indexes(inspector, table_name)
            
            # Generate model
            model_content = generate_model_file(table_name, columns, foreign_keys, indexes)
            
            # Write model file
            class_name = to_python_class_name(table_name)
            file_name = f"{table_name}.py"
            file_path = os.path.join(models_dir, file_name)
            
            with open(file_path, 'w') as f:
                f.write(model_content)
            
            init_imports.append(f"from .{table_name} import {class_name}")
            print(f"✅ Generated {file_path}")
            
        except Exception as e:
            print(f"❌ Error processing {table_name}: {e}")
            continue
    
    # Generate __init__.py
    init_content = '"""Auto-generated SQLAlchemy models."""\n\n'
    init_content += '\n'.join(init_imports) + '\n\n'
    init_content += '__all__ = [\n'
    for table_name in sorted(tables):
        class_name = to_python_class_name(table_name)
        init_content += f'    "{class_name}",\n'
    init_content += ']\n'
    
    with open(os.path.join(models_dir, '__init__.py'), 'w') as f:
        f.write(init_content)
    
    print(f"✅ Generated {len(tables)} model files")
    print(f"📁 Models saved to: {models_dir}/")
    print(f"🎉 Model generation complete!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
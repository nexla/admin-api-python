json.(@code_container, :id, :name, :resource_type)
json.type @code_container.repo_type
json.branches @code_container.branches
json.(@code_container, :updated_at, :created_at)